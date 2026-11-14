import json
import re
import time
import random
import requests
from playwright.sync_api import sync_playwright
import csv
from datetime import datetime
from celery_config.celery_app import app
from kafka_config.create_kafka_producer import create_kafka_producer
from utils.split_list import split_list
from app_config.config import settings
from selectolax.lexbor import LexborHTMLParser


def parse_price(text):
    match = re.search(r'([\d\s]+,\d{2})', text)
    if match:
        clean_num = match.group(1).replace(" ", "").replace(",", ".")
        return float(clean_num)
    return 0.0


def get_text_by_keyword(tree_node, keyword):
    for el in tree_node.css('div, span, p, strong'):
        txt = el.text(strip=True)
        if keyword in txt:
            return txt
    return ""


@app.task
def run_x():
    codes = scrape_main_x()
    batches = split_list(codes, settings.number_of_workers)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_x.delay(batch, idx)


@app.task
def scrape_x(batch, id):
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }
    prod = create_kafka_producer()

    # Użycie Session() utrzymuje jedno połączenie Keep-Alive (znacznie szybciej niż pojedyncze requests.get)
    with requests.Session() as session:
        session.headers.update(HEADERS)

        for link in batch:
            if not link.startswith("http"):
                link = f"https://{link}"

            response = None
            for attempt in range(3):
                try:
                    response = session.get(link, timeout=15)
                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        # Tylko gdy serwer przytnie ruch (429), czekamy chwilę dłużej i ponawiamy
                        wait_time = (attempt + 1) * 1.5 + random.uniform(0.2, 0.8)
                        print(f"[Worker {id}] Rate limit (429) dla {link}, ponawiam za {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"[Worker {id}] HTTP {response.status_code} dla {link} (próba {attempt + 1}/3)")
                        time.sleep(0.5)
                except requests.RequestException as e:
                    print(f"[Worker {id}] Błąd połączenia dla {link}: {e}")
                    time.sleep(0.5)

            if not response or response.status_code != 200:
                print(f"[Worker {id}] Pominięto {link} (brak odpowiedzi 200 OK)")
                continue

            tree = LexborHTMLParser(response.text)

            # --- 1. PODSTAWOWE DANE O PRODUKCIE ---
            h1 = tree.css_first("h1")
            product_name = h1.text().strip() if h1 else "-"

            # Jeśli strona nie zwróciła poprawnego produktu, nie wysyłamy śmieci ("-", 0.0) do Kafki i bazy
            if product_name == "-" or not product_name:
                print(f"[Worker {id}] Pominięto {link} (brak nazwy produktu w HTML)")
                continue

            # --- PRODUCENT ---
            producer_el = tree.css_first('[class*="parts__LinkProducer"]')
            if producer_el:
                producer = producer_el.text(strip=True)
            else:
                producer = "-"
                separators = tree.css('span[class*="parts__Separator"]')
                for sep in separators:
                    next_el = sep.next
                    if next_el and next_el.tag == 'span' and not next_el.attributes.get('class'):
                        txt = next_el.text(strip=True)
                        if txt and len(txt) < 30:
                            producer = txt
                            break

            # --- 2. KATEGORIE Z JSON-LD (POPRAWIONE) ---
            category2 = "-"
            category = "-"
            category_deep = "-"

            json_scripts = tree.css('script[type="application/ld+json"]')
            for script in json_scripts:
                try:
                    content = script.text(strip=True)
                    data = json.loads(content)

                    items_list = []
                    if isinstance(data, list):
                        for item in data:
                            if item.get("@type") == "BreadcrumbList":
                                items_list = item.get("itemListElement", [])
                                break
                    elif isinstance(data, dict):
                        if data.get("@type") == "BreadcrumbList":
                            items_list = data.get("itemListElement", [])

                    if items_list:
                        items_list = sorted(items_list, key=lambda x: x.get("position", 0))
                        cat_names = [item.get("name") for item in items_list if item.get("name")]

                        if len(cat_names) > 0:
                            category2 = cat_names[0]  # np. Urządzenia peryferyjne
                        if len(cat_names) > 1:
                            category = cat_names[1]   # np. Monitory
                        if len(cat_names) > 2:
                            category_deep = cat_names[2]  # np. Monitory LED 27"
                        break
                except Exception:
                    continue

            aktualna_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sklep = "x-kom"

            # --- 3. POBIERANIE CEN ---
            price_wrapper = tree.css_first('div[data-name="productPrice"]')
            screen_prices = price_wrapper.css('span[class*="parts__ScreenReaderPrice"]') if price_wrapper else []

            price_texts = [sp.attributes.get("aria-label", "") for sp in screen_prices if sp.attributes.get("aria-label")]

            # --- 4. LOGIKA CENOWA ---
            coupon = "-"

            if len(price_texts) == 0:
                price_ready = 0.0
                price_old = 0.0
                price_omnibus = 0.0
            elif len(price_texts) == 1:
                price_ready = parse_price(price_texts[0])
                price_old = price_ready
                price_omnibus = price_ready
            else:
                price_main = parse_price(price_texts[0])
                price_second = parse_price(price_texts[1])

                price_omnibus = price_second
                price_ready = price_main
                price_old = price_second

                if "Najniższa cena z ostatnich 30 dni" in price_texts[1]:
                    if price_main > price_second:
                        price_ready = price_main
                        price_old = price_second
                        price_omnibus = price_old
                    elif price_main == price_second:
                        price_ready = price_main
                        element_text = get_text_by_keyword(tree, "Cena bez kodu:")
                        price_old = parse_price(element_text) if element_text else price_main

                        coupon_el = tree.css_first('#coupon-code span')
                        coupon = coupon_el.text(strip=True) if coupon_el else "-"

                        omnibus_text = get_text_by_keyword(tree, "Najniższa cena z 30 dni przed obniżką:")
                        price_omnibus = parse_price(omnibus_text) if omnibus_text else price_second

            if price_ready == 0.0:
                print(f"[Worker {id}] Pominięto {link} (cena 0.0)")
                continue

            print("=" * 50)
            print(" ZWERYFIKOWANE DANE ZE SCRAPERA (SELECTOLAX):")
            print("=" * 50)
            print(f"Data pobrania:     {aktualna_data}")
            print(f"Sklep:             {sklep}")
            print(f"Nazwa produktu:    {product_name}")
            print(f"Producent:         {producer}")
            print(f"Kategoria główna:  {category2}")
            print(f"Podkategoria:      {category}")
            print(f"Szczegółowa kat.:  {category_deep}")
            print(f"Link do produktu:  {link}")
            print("-" * 50)
            print(f"Cena promocyjna:   {price_ready}")
            print(f"Cena stara (przed):{price_old}")
            print(f"Cena Omnibus:      {price_omnibus}")
            print(f"Kod rabatowy:      {coupon}")
            print("=" * 50)

            wiersz_danych = [aktualna_data, sklep, product_name, producer, category, category2, link, price_ready, price_old, price_omnibus, coupon]
            prod.send('test-topic-3', {'message': wiersz_danych})

        # Flush wykonujemy raz na koniec całej paczki, dzięki czemu wysyłka do Kafki nie blokuje pętli po każdym produkcie
        prod.flush()

def scrape_main_x():

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=0)
        
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        page.goto(settings.x_url)
        element = page.locator('[class*="parts__PagesTotal"]').first.inner_text()

        links = []

        page_wrapers = page.locator('[class*="parts__InfoSection"] a')
        for i in page_wrapers.all():
            if "#Opinie" not in i.get_attribute("href"):
                item_link = f"{settings.x_shop}{i.get_attribute("href")}"
                links.append(item_link)

        pages = int(element[element.index(" ")+1:])
        for i in range(2, pages+1):
        # for i in range(2, 10):
            link  = f"{settings.x_url_2}={i}"
            print(f"Wchodzę na: {link}")
            
            # BRAKUJĄCA LINIJKA - przechodzimy na nową stronę!
            page.goto(link)
            
            # Czekamy chwilę, aż nowa lista produktów się wyrenderuje
            page.wait_for_timeout(100)

            page_wrapers = page.locator('[class*="parts__InfoSection"] a')
            for i in page_wrapers.all():
                if "#Opinie" not in i.get_attribute("href"):
                    item_link = f"{settings.x_shop}{i.get_attribute("href")}"
                    links.append(item_link)

        print(links)
        with open("xkom-links.json", "w", encoding="utf-8") as file:
            json.dump(links, file, indent=4)

    #h1 parts title - tytul
    # span class part price 
    # producer parts__LinkProducer 


    # browser.close()

    with open("xkom-links.json", "r", encoding="utf-8") as file:
        links = json.load(file)
        
    links = list(set(links))
    return links




