import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]
    

## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, page_number, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.target.com/s?searchTerm={formatted_keyword}&Nao={page_number*24}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")
            div_cards = soup.select("div[data-test='@web/site-top-of-funnel/ProductCardWrapper']", recursive=False)

            for div_card in div_cards:
                a_tags = div_card.find_all("a")
                href = a_tags[0].get("href")
                name = href.split("/")[2]                
                link = f"https://www.target.com{href}"                
                
                search_data = {
                    "name": name,
                    "url": link,
                }

                print(search_data)
            logger.info(f"Successfully parsed data from: {url}")
            success = True
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_scrape(keyword, pages, location, data_pipeline=None, max_threads=5, retries=3):
    for page in range(pages):
        scrape_search_results(keyword, location, page, data_pipeline=data_pipeline, retries=retries)




if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["laptop"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        start_scrape(keyword, PAGES, LOCATION, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")