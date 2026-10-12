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

klasa = "d-inline-block reference"
naglowki = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

@app.task
def run_bd():
    codes = scrape_main_bd()
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

                # Pozostałe cechy znajdują się w attributes
                attr = hit.get("attributes", {})
                aktualna_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                kategorie = attr.get("all_categories") or []
                kat_pierwsza = kategorie[0] if len(kategorie) > 0 else "Brak"
                kat_ostatnia = kategorie[-1] if len(kategorie) > 0 else "Brak"

                title = attr.get("title", "Brak tytułu")
                brand = attr.get("brand", "Brak marki")
# Pobranie surowych wartości
                raw_price_old = attr.get("price_pln_old")
                raw_last_price = attr.get("cache_last_price")
                raw_omnibus = attr.get("omnibus_price")

                # Konwersja na float
                price_pln_old = parsuj_cene(raw_price_old)
                cache_last_price = parsuj_cene(raw_last_price)
                omnibus_price = parsuj_cene(raw_omnibus)

                # Formatowanie do printa/pliku (np. z formatowaniem do 2 miejsc po przecinku):

                linijka = (
                    f"[{brand}] {title} | "
                    f"Cena: {cache_last_price} zł (Stara: {price_pln_old} zł, Omnibus: {omnibus_price} zł) | "
                    f"Kategorie: [{kat_pierwsza} -> {kat_ostatnia}] | "
                    f"URL: {product_url}"
                )

                wiersz_danych = [aktualna_data, settings.bd_shop, title, brand[0], kat_pierwsza, kat_ostatnia, url, cache_last_price, price_pln_old,  omnibus_price, "-"]          
                
                # Wypisanie na ekran i natychmiastowy zapis do pliku
                prod.send('test-topic-3', {'message': wiersz_danych})
                prod.flush()

            # Stała pauza między indeksami (0.6s - 1.2s), aby nie wywoływać 429
            time.sleep(random.uniform(0.6, 1.2))






