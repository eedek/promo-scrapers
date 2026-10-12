from app_config.config import settings
import requests
from bs4 import BeautifulSoup
from celery_config.celery_app import app
from utils.split_list import split_list
import random
import time
import urllib.parse

settings.bd_shop
settings.bd_url
settings.bd_api

klasa = "d-inline-block reference"
naglowki = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def run_bd():
    codes = scrape_main_bd()
    batches = split_list(codes, settings.number_of_workers)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_bd.delay(batch, idx)

@app.task
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


@app.task
def scrape_bd(batch, idx):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    wyniki = batch


    with requests.Session() as session:
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

                kategorie = attr.get("all_categories") or []
                kat_pierwsza = kategorie[0] if len(kategorie) > 0 else "Brak"
                kat_ostatnia = kategorie[-1] if len(kategorie) > 0 else "Brak"

                title = attr.get("title", "Brak tytułu")
                brand = attr.get("brand", "Brak marki")
                price_pln_old = attr.get("price_pln_old", "-")
                cache_last_price = attr.get("cache_last_price", "-")
                omnibus_price = attr.get("omnibus_price", "-")

                linijka = (
                    f"[{brand}] {title} | "
                    f"Cena: {cache_last_price} zł (Stara: {price_pln_old} zł, Omnibus: {omnibus_price} zł) | "
                    f"Kategorie: [{kat_pierwsza} -> {kat_ostatnia}] | "
                    f"URL: {product_url}"
                )

                # Wypisanie na ekran i natychmiastowy zapis do pliku
                print(linijka)

            # Stała pauza między indeksami (0.6s - 1.2s), aby nie wywoływać 429
            time.sleep(random.uniform(0.6, 1.2))






