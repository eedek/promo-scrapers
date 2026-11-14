from app_config.config import settings
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from celery_config.celery_app import app
from utils.split_list import split_list
import random
import time
from kafka_config.create_kafka_producer import create_kafka_producer
import urllib.parse

settings.bd_shop
settings.bd_url
settings.bd_api

klasa = "product-miniature__reference-value"
naglowki = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

@app.task
def run_bd():
    codes = scrape_main_bd()
    print(f"[Botland] Zebrano {len(codes)} kodów produktów z paginacji.")
    batches = split_list(codes, settings.number_of_workers)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_bd.delay(batch, idx)

def scrape_main_bd():
    wyniki = []
    with requests.Session() as session:

        session.headers.update(naglowki)
        # przed ostatni item klasy page-list pagination    
        for strona in range(1, 29):  
            url = f"{settings.bd_url}{strona}"
            odpowiedz = session.get(url)
            
            if odpowiedz.status_code != 200:
                print(f"Błąd lub koniec stron na: {url}")
                break
                
            soup = BeautifulSoup(odpowiedz.text, "lxml")
            elementy = soup.find_all(class_=klasa)
            
            for el in elementy:
                tekst = el.get_text(strip=True)
                if tekst:
                    wyniki.append(tekst)

    return wyniki


import re


def parsuj_cene(wartosc) -> float | None:
    """Czyści string z ceny (np.

    '129,90 zł', ' 1 299.00 zl ') i zwraca float.
    """
    if wartosc is None:
        return None

    # Zamiana na string, obcięcie białych znaków
    tekst = str(wartosc).strip()

    # Zastąpienie przecinka kropką i usunięcie wszystkich spacji (w tym twardych spacjach \xa0)
    tekst = tekst.replace(",", ".").replace(" ", "").replace("\xa0", "")

    # Wyciągnięcie liczby (całkowitej lub zmiennoprzecinkowej)
    dopasowanie = re.search(r"(\d+(?:\.\d+)?)", tekst)
    if dopasowanie:
        return float(dopasowanie.group(1))

    return None


@app.task
def scrape_bd(batch, idx):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    wyniki = batch


    with requests.Session() as session:
        prod = create_kafka_producer()
        session.headers.update(headers)

        for index in wyniki:
            query_encoded = urllib.parse.quote(str(index))
            url = settings.bd_api.format(query=urllib.parse.quote(str(index)))
            max_retries = 3
            data = None

            for proba in range(max_retries):
                response = session.get(url)

                if response.status_code == 200:
                    data = response.json()
                    break
                elif response.status_code == 429:
                    # Sprawdzamy czy API odesłało nagłówek Retry-After, domyślnie czekamy 3-5 sekund
                    retry_after = int(response.headers.get("Retry-After", random.uniform(3.0, 5.0)))
                    print(f"[429] Zbyt wiele zapytań dla '{index}'. Czekam {retry_after}s...")
                    time.sleep(retry_after)
                else:
                    print(f"Błąd dla '{index}': Status {response.status_code}")
                    break

            if not data:
                continue

            hits = data.get("hits", [])

            for hit in hits:
                if hit.get("type") != "item":
                    continue

                # url znajduje się bezpośrednio w hit
                product_url = hit.get("url", "-")
                if product_url.startswith("/"):
                    product_url = f"https://{settings.bd_shop}{product_url}"

                # Pozostałe cechy znajdują się w attributes
                attr = hit.get("attributes", {})
                aktualna_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                kategorie = attr.get("all_categories") or []
                kat_pierwsza = kategorie[0] if len(kategorie) > 0 else "Brak"
                kat_ostatnia = kategorie[-1] if len(kategorie) > 0 else "Brak"

                title = attr.get("title", "Brak tytułu")
                raw_brand = attr.get("brand")
                if isinstance(raw_brand, list) and len(raw_brand) > 0:
                    brand_name = raw_brand[0]
                elif isinstance(raw_brand, str) and raw_brand:
                    brand_name = raw_brand
                else:
                    brand_name = "Brak marki"

                # Pobranie surowych wartości (API zwraca teraz cenę w polu 'price' lub 'price_amount')
                raw_price_old = attr.get("price_pln_old") or attr.get("price_old")
                raw_last_price = attr.get("cache_last_price") or attr.get("price") or attr.get("price_amount")
                raw_omnibus = attr.get("omnibus_price") or raw_price_old or raw_last_price

                # Konwersja na float
                price_pln_old = parsuj_cene(raw_price_old)
                cache_last_price = parsuj_cene(raw_last_price)
                omnibus_price = parsuj_cene(raw_omnibus)

                if not cache_last_price:
                    continue

                linijka = (
                    f"[{brand_name}] {title} | "
                    f"Cena: {cache_last_price} zł (Stara: {price_pln_old} zł, Omnibus: {omnibus_price} zł) | "
                    f"Kategorie: [{kat_pierwsza} -> {kat_ostatnia}] | "
                    f"URL: {product_url}"
                )
                print(linijka)

                wiersz_danych = [
                    aktualna_data,
                    settings.bd_shop,
                    title,
                    brand_name,
                    kat_pierwsza,
                    kat_ostatnia,
                    product_url,
                    cache_last_price,
                    price_pln_old,
                    omnibus_price,
                    "-"
                ]

                prod.send('test-topic-3', {'message': wiersz_danych})

            # Krótka pauza między indeksami, aby nie wywoływać 429
            time.sleep(random.uniform(0.2, 0.5))

        prod.flush()






