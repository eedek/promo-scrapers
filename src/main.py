from utils.split_list import split_list
from scrapers.media_expert import scrape_main_media_expert, scrape_media_expert
from scrapers.xkom import scrape_main_xkom, scrape_xkom
from scrapers.morele import scrape_main_morele, scrape_morele

def run_media_expert():
    codes = scrape_main_media_expert()
    batches = split_list(codes, 8)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_media_expert.delay(batch, idx)

def run_xkom():
    codes = scrape_main_xkom()
    batches = split_list(codes, 8)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_xkom.delay(batch, idx)


def run_morele():
    print("Zbieranie danych startowych: Morele...")
    dane_produktow = scrape_main_morele()
    print(f"Znaleziono {len(dane_produktow)} produktów w Morele.")
    
    batches = split_list(dane_produktow, 8)

    for idx, batch in enumerate(batches):
        if batch:
            scrape_morele.delay(batch, idx)



if __name__ == "__main__":

    # run_media_expert()
    # run_xkom()
    run_morele()