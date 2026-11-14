import csv
import json
import random
import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from celery_config.celery_app import app
from kafka_config.create_kafka_producer import create_kafka_producer
from utils.split_list import split_list
from app_config.config import settings

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}


def parsuj_cene(wartosc) -> float | None:
    if wartosc is None:
        return None
    tekst = str(wartosc).strip().replace(",", ".").replace(" ", "").replace("\xa0", "")
    dopasowanie = re.search(r"(\d+(?:\.\d+)?)", tekst)
    if dopasowanie:
        return float(dopasowanie.group(1))
    return None


@app.task
def run_mo():
    print("Zbieranie danych startowych: Morele (HTTP + BeautifulSoup)...")
    dane_produktow = scrape_main_mo()
    print(f"Znaleziono {len(dane_produktow)} produktów w Morele.")

    batches = split_list(dane_produktow, settings.number_of_workers)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_mo.delay(batch, idx)


# --- 1. FUNKCJA ZBIERAJĄCA (Bez Playwrighta - 1 szybkie zapytanie HTTP) ---
def scrape_main_mo():
    response = requests.get(settings.mo_url, headers=HEADERS, timeout=30)
    if response.status_code != 200:
        print(f"[BŁĄD] Morele LP zwróciło status {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "lxml")
    items = soup.select(".owl-item")

    products_data = []
    seen_urls = set()

    for item in items:
        name = item.get("data-product-name")
        if not name:
            continue

        link_el = item.select_one(".product-link")
        item_link = link_el.get("href") if link_el else None
        if not item_link:
            continue

        # Zabezpieczenie przed względnymi URL-ami
        if not item_link.startswith("http"):
            item_link = settings.mo_url_2 + item_link

        # Odfiltrowanie duplikatów z karuzeli na stronie głównej
        if item_link in seen_urls:
            continue
        seen_urls.add(item_link)

        promo_price = parsuj_cene(item.get("data-product-price"))
        category = item.get("data-product-category") or "-"
        category_second = item.get("data-product-catgroup") or item.get("data-product-department") or "-"

        price_old_el = item.select_one(".price-old")
        raw_price_old = price_old_el.get("data-lowest-price-local-value") if price_old_el else None
        price_old = parsuj_cene(raw_price_old) or promo_price

        products_data.append({
            "url_produktu": item_link,
            "nazwa": name.strip(),
            "cena": promo_price,
            "kategoria": category,
            "kategoria_druga": category_second,
            "cena_old": price_old,
        })

    return products_data


def wyciagnij_marke(soup: BeautifulSoup) -> str:
    # 1. Najpewniejsze źródło: JSON-LD (@type == Product)
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or script.get_text())
            if isinstance(data, dict) and data.get("@type") == "Product":
                brand = data.get("brand")
                if isinstance(brand, dict) and brand.get("name"):
                    return brand["name"].strip()
                elif isinstance(brand, str) and brand:
                    return brand.strip()
        except Exception:
            continue

    # 2. Fallback: tabela specyfikacji w HTML
    for el in soup.find_all(string=re.compile(r"^\s*Marka\s*$")):
        parent = el.find_parent()
        if parent and parent.parent:
            link = parent.parent.find("a")
            if link:
                return link.get_text(strip=True)

    return "-"


# --- 2. ZADANIE CELERY (Worker: requests.Session + BeautifulSoup zamiast przeglądarki) ---
@app.task
def scrape_mo(batch, id):
    prod = create_kafka_producer()

    with requests.Session() as session:
        session.headers.update(HEADERS)

        for p_data in batch:
            url = p_data["url_produktu"]
            response = None

            for attempt in range(3):
                try:
                    response = session.get(url, timeout=15)
                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        wait_time = (attempt + 1) * 1.5 + random.uniform(0.3, 0.8)
                        print(f"[Morele Worker {id}] Rate limit (429) dla {url}, czekam {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        time.sleep(0.5)
                except requests.RequestException as e:
                    print(f"[Morele Worker {id}] Błąd sieci dla {url}: {e}")
                    time.sleep(0.5)

            if not response or response.status_code != 200:
                print(f"[Morele Worker {id}] Pominięto {url} (brak odpowiedzi 200 OK)")
                continue

            try:
                soup = BeautifulSoup(response.text, "lxml")

                brand = wyciagnij_marke(soup)

                kod_el = soup.select_one(".kod")
                kod = kod_el.get_text(strip=True) if kod_el else "-"

                omnibus_el = soup.select_one(".price-box__lowest-price span")
                price_omnibus = (
                    parsuj_cene(omnibus_el.get_text(strip=True))
                    if omnibus_el
                    else (p_data["cena_old"] or p_data["cena"])
                )

                cena_promo = p_data["cena"]
                cena_old = p_data["cena_old"] or cena_promo
                if not price_omnibus:
                    price_omnibus = cena_old

                if not cena_promo:
                    continue

                aktualna_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                wiersz_danych = [
                    aktualna_data,
                    settings.mo_shop,
                    p_data["nazwa"],
                    brand,
                    p_data["kategoria"],
                    p_data.get("kategoria_druga", "-"),
                    url,
                    cena_promo,
                    cena_old,
                    price_omnibus,
                    kod,
                ]

                prod.send("test-topic-3", {"message": wiersz_danych})

            except Exception as e:
                print(f"[BŁĄD] Morele: Pominięto {url} z powodu: {e}")

        prod.flush()
