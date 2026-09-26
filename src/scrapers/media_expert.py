import json
from playwright.sync_api import sync_playwright
import csv
from datetime import datetime
from celery_config.celery_app import app
from utils.split_list import split_list
from kafka_config.create_kafka_producer import create_kafka_producer

# def scrape_media_expert():
# def run(playwright):


def run_media_expert():
    codes = scrape_main_media_expert()
    batches = split_list(codes, 8)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_media_expert.delay(batch, idx)


def scrape_main_media_expert():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=500)
            
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
            
            # page.goto("https://www.mediaexpert.pl/lp,wielka-wyprzedaz")
        page.goto("https://www.mediaexpert.pl/lp,okazje")

            
        page.wait_for_timeout(2000)
            
            # promocje = page.locator(".top-hity-top-hity")
        promocje = page.locator(".produkty")
        products = promocje.locator("[data-kod]")
        count = products.count()

        data = []
        for i in range(count):
            product = products.nth(i)
            kod = product.get_attribute("data-kod")
            data.append(kod)
            # inner = promocje.locator("div")
        print(len(data))
            # for div in inner.all():
            #     kod = div.get_attribute("data-kod") 
            #     if kod is not None:
            #         data.append(kod)
        
            # data_do_pobrania = data[:5]
        browser.close()
        return data


@app.task
def scrape_media_expert(batch, id):
    with sync_playwright() as p:
        prod = create_kafka_producer()
        browser = p.chromium.launch(headless=True, slow_mo=500)
            
        page = browser.new_page(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        #     run(p)
            # with sync_playwright() as p:
            # print(f"Znaleziono {len(data)} kodów. Pobieram dane dla {len(data_do_pobrania)} z nich...")
            # print("-" * 50)

            #5. Pętla uderzająca do API dla każdego pobranego kodu BEZ użycia page.goto()
        for field in batch:
            url = f"https://sgimg.mediaexpert.pl/json/me/{field}.json"
                
            try:
                    # Używamy page.evaluate() aby wykonać pobieranie "w tle" z poziomu otwartej strony.
                    # Zwraca on od razu gotowy słownik Pythona!
                dane_json = page.evaluate(f"""async () => {{
                        const response = await fetch("{url}");
                        if (!response.ok) throw new Error("Odrzucono zapytanie");
                        return await response.json();
                }}""")
                    
                    # Wchodzimy do głównego "folderu" z danymi
                dane_produktu = dane_json.get(field)
                    
                    # Wyciągamy dane z wnętrza
                if dane_produktu:
                    nazwa = dane_produktu.get("name", "Brak nazwy")
                    cena = dane_produktu.get("price", "Brak ceny")
                    cena_web = dane_produktu.get("price_web", "Brak ceny")
                    cena_app = dane_produktu.get("price_app", "Brak ceny")
                    cena_old = dane_produktu.get("price_old", "Brak ceny")
                    price_omnibus_web = dane_produktu.get("price_omnibus_web", "Brak ceny")
                    price_omnibus_app = dane_produktu.get("price_omnibus_app", "Brak ceny")
                    price_omnibus = dane_produktu.get("price_omnibus", "Brak ceny")
                    kod_rabat = dane_produktu.get("kod_rabat", "Brak kodu")
                    kod_rabat_app = dane_produktu.get("kod_rabat_app", "Brak kodu")
                    hot_end_buffor = dane_produktu.get("hot_end_buffor", "Brak daty")
                    hot_end_buffor_app = dane_produktu.get("hot_end_buffor_app", "Brak daty")
                    kategoria = dane_produktu.get("kategoria", "Brak kategorii")
                    producer = dane_produktu.get("producer", "Brak producenta")
                    url_produktu = dane_produktu.get("url", "Brak URL")

                        # Krótka data do nazwy pliku
                    data_do_pliku = datetime.now().strftime("%Y-%m-%d")
                    nazwa_pliku = f"media_expert_{data_do_pliku}-{id}.csv"

                        # Dokładny czas do środka tabeli i nazwa sklepu
                    aktualna_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    sklep = "Media Expert"

                    # wiersz_danych = [
                    #         aktualna_data, sklep,
                    #         field, nazwa, producer, kategoria, url_produktu,
                    #         cena, cena_old, cena_web, cena_app,
                    #         price_omnibus, price_omnibus_web, price_omnibus_app,
                    #         kod_rabat, kod_rabat_app,
                    #         hot_end_buffor, hot_end_buffor_app
                    #     ]
                    
                    wiersz_danych = [
                            aktualna_data, sklep,
                            nazwa, producer, kategoria, "-", url_produktu,
                            cena, cena_old, price_omnibus,
                            kod_rabat
                        ]
                    
                    prod.send('test-topic-3', {'message': wiersz_danych})
                    prod.flush()
                    
                        # Zapis do pliku
                    with open(nazwa_pliku, mode='a', newline='', encoding='utf-8') as plik_csv:
                        writer = csv.writer(plik_csv, delimiter=';')
                        writer.writerow(wiersz_danych)
                            
                    print(f"Zapisano: {field} | {cena} zł | {nazwa}")
                        
                else:
                    print(f"Błąd: {field} | Plik JSON nie zawiera danych.")
                    
            except Exception as e:
                print(f"Błąd przy pobieraniu {field}: {e}")

                # UWAGA: Zmniejszyliśmy czas oczekiwania do 0.2 sekundy!
                # Brak opóźnienia przy 4000 zapytań to pewny "ban" od serwera za spam.
                # 200 ms to bezpieczny bufor dla API.
            page.wait_for_timeout(200)

        browser.close()

    # with sync_playwright() as p:
    #     run(p)