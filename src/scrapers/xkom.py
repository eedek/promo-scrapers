import json
from playwright.sync_api import sync_playwright
import csv
from datetime import datetime
from celery_config.celery_app import app
from kafka_config.create_kafka_producer import create_kafka_producer
from utils.split_list import split_list


def run_xkom():
    codes = scrape_main_xkom()
    batches = split_list(codes, 8)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_xkom.delay(batch, idx)



@app.task
def scrape_xkom(batch, id):

    with sync_playwright() as p:
        prod = create_kafka_producer()
        browser = p.chromium.launch(headless=True, slow_mo=0)
        
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "stylesheet", "font", "media"] else route.continue_())
        
        for link in batch:
            # GŁÓWNY BLOK TRY-EXCEPT ZABEZPIECZAJĄCY CAŁY LINK
            try:
                page.goto(f"https://{link}", wait_until="domcontentloaded")
                title = page.locator('h1[class*="parts__Title"]').inner_text()
                
                prices = page.locator('div[data-name="productPrice"]') 
                price = prices.locator('[class*="parts__ScreenReaderPrice"]').nth(0).inner_text()
                price1 = prices.locator('[class*="parts__ScreenReaderPrice"]').nth(1).inner_text()
                producer = page.locator('[class*="parts__LinkProducer"]').inner_text()
                item_wrapper = page.locator('[class*="parts__InnerWrapper"]')
                category = item_wrapper.locator('a[class*="parts__Link"]').nth(-2).inner_text()
                
                try:
                    category2 = item_wrapper.locator('a[class*="parts__Link"]').nth(-3).inner_text()
                except:
                    category2 = "-"
                    
                aktualna_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                sklep = "xkom"

                if "Najniższa cena z ostatnich 30 dni" in price1:
                    price_main = float(price[price.index(" ")+1:-3].replace(",", ".").replace(" ", ""))
                    price_second = float(price1[price1.index(":")+2:-3].replace(",", ".").replace(" ", ""))

                    if price_main > price_second:
                        price_ready = price_main
                        price_old = price_second
                        price_omnibus = price_old
                        coupon = "-"
                    elif price_main == price_second:
                        element = page.get_by_text("Cena bez kodu:").nth(0).inner_text()
                        price_old = float(element[element.index(":")+2:-3].replace(",", ".").replace(" ", ""))
                        price_ready = price_main
                        coupon = page.locator("#coupon-code span").inner_text()
                        price_omnibus_text = page.get_by_text("Najniższa cena z 30 dni przed obniżką:").nth(0).inner_text()
                        price_omnibus = float(price_omnibus_text[price_omnibus_text.index(":")+2:-3].replace(",", ".").replace(" ", ""))
                    else: 
                        price_ready = price_main
                        price_old = price_second
                        coupon = "-"
                        price_omnibus = price_old

                    wiersz_danych = [aktualna_data, sklep, title, producer, category, category2, link, price_ready, price_old,  price_omnibus, coupon]          
                    data_do_pliku = datetime.now().strftime("%Y-%m-%d")
                    nazwa_pliku = f"xkom{data_do_pliku}-{id}.csv"

                    prod.send('test-topic-3', {'message': wiersz_danych})
                    prod.flush()

                    with open(nazwa_pliku, mode='a', newline='', encoding='utf-8') as plik_csv:
                        writer = csv.writer(plik_csv, delimiter=';')
                        writer.writerow(wiersz_danych)
                            
            except Exception as e:
                # Wypisujemy błąd z nową linią, aby nie mazać paska postępu
                print(f"\n[BŁĄD] Pominięto link {link} z powodu: {e}")
                


        browser.close()

def scrape_main_xkom():

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=0)
        
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        page.goto("https://www.x-kom.pl/trendy/promocje")
        element = page.locator('[class*="parts__PagesTotal"]').first.inner_text()

        links = []

        page_wrapers = page.locator('[class*="parts__InfoSection"] a')
        for i in page_wrapers.all():
            if "#Opinie" not in i.get_attribute("href"):
                item_link = f"x-kom.pl{i.get_attribute("href")}"
                links.append(item_link)

        pages = int(element[element.index(" ")+1:])
        for i in range(2, pages+1):
        # for i in range(2, 10):
        
            link = f"https://www.x-kom.pl/trendy/promocje?page={i}"
            print(f"Wchodzę na: {link}")
            
            # BRAKUJĄCA LINIJKA - przechodzimy na nową stronę!
            page.goto(link)
            
            # Czekamy chwilę, aż nowa lista produktów się wyrenderuje
            page.wait_for_timeout(500)

            page_wrapers = page.locator('[class*="parts__InfoSection"] a')
            for i in page_wrapers.all():
                if "#Opinie" not in i.get_attribute("href"):
                    item_link = f"x-kom.pl{i.get_attribute("href")}"
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




