import csv
from datetime import datetime
from playwright.sync_api import sync_playwright
from celery_config.celery_app import app
from kafka_config.create_kafka_producer import create_kafka_producer
from utils.split_list import split_list

def run_morele():
    print("Zbieranie danych startowych: Morele...")
    dane_produktow = scrape_main_morele()
    print(f"Znaleziono {len(dane_produktow)} produktów w Morele.")
    
    batches = split_list(dane_produktow, 8)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_morele.delay(batch, idx)



# --- 1. FUNKCJA ZBIERAJĄCA (Kierownik, odpalany z main.py) ---
def scrape_main_morele():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=0)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        page.goto("https://lp.morele.net/wyprzedaz-ostatnich-sztuk/")
        page.wait_for_timeout(2000)
        
        items = page.locator(".owl-item")
        c = items.count() # Używamy count() zamiast all() dla bezpieczeństwa
        
        products_data = []
        for i in range(c):
            item = items.nth(i)
            
            # Pobieranie danych bazowych ze strony głównej promocji
            item_link = item.locator(".product-link").get_attribute("href")
            
            # Zabezpieczenie przed względnymi URL-ami
            if item_link and not item_link.startswith("http"):
                item_link = "https://www.morele.net" + item_link
                
            name = item.get_attribute("data-product-name")
            promo_price = item.get_attribute("data-product-price")
            category = item.get_attribute("data-product-category")
            
            try:
                price_old = item.locator(".price-old").get_attribute("data-lowest-price-local-value")
            except Exception:
                price_old = "-"
                
            # Zamiast gołego linku, zapisujemy cały słownik danych
            products_data.append({
                "url_produktu": item_link,
                "nazwa": name,
                "cena": promo_price,
                "kategoria": category,
                "cena_old": price_old
            })
            
        browser.close()
        return products_data # Zwracamy listę słowników do main.py


# --- 2. ZADANIE CELERY (Worker, odpala się w tle) ---
@app.task
def scrape_morele(batch, id):
    with sync_playwright() as p:
        prod = create_kafka_producer()
        browser = p.chromium.launch(headless=True, slow_mo=0)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        # Blokujemy wczytywanie zdjęć i stylów, żeby strony produktów ładowały się błyskawicznie (jak w X-kom)
        page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "stylesheet", "font", "media"] else route.continue_())
        
        data_do_pliku = datetime.now().strftime("%Y-%m-%d")
        nazwa_pliku = f"morele_{data_do_pliku}-{id}.csv"
        
        with open(nazwa_pliku, mode='w', newline='', encoding='utf-8') as plik_csv:
            writer = csv.writer(plik_csv, delimiter=';')
            
            # Batch zawiera teraz paczkę słowników
            for p_data in batch:
                try:
                    page.goto(p_data["url_produktu"])
                    
                    try:
                        brand = page.get_by_text("Marka", exact=True).locator("..").locator("a").inner_text(timeout=2000)
                        kod = page.locator(".kod").inner_text(timeout=2000)
                        price_omnibus = page.locator(".price-box__lowest-price span").inner_text(timeout=2000)
                        price_omnibus = price_omnibus.replace("zł", "").replace(" ", "").replace(",", ".")
                    except Exception:
                        kod = "-"
                        price_omnibus = None
                        brand = "-"
                        
                    aktualna_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Sklejamy dane przysłane w słowniku z danymi pobranymi w workerze
                    wiersz_danych = [
                        aktualna_data,
                        "morele.net",
                        p_data["nazwa"],
                        brand,
                        p_data["kategoria"],
                        "-",
                        p_data["url_produktu"],
                        p_data["cena"],
                        p_data["cena_old"],
                        price_omnibus,
                        kod
                    ]
                    
                    writer.writerow(wiersz_danych)

                    
                    prod.send('test-topic-3', {'message': wiersz_danych})
                    prod.flush()
                    
                except Exception as e:
                    print(f"\n[BŁĄD] Morele: Pominięto {p_data.get('url_produktu', 'Brak URL')} z powodu: {e}")
                    
        browser.close()
        return nazwa_pliku