#Los datos de NFT deben buscarse en sitios particulares. El scraping es individual para cada proyecto.

from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()

#NFT11
#Precios mínimos todo (BNB)
def price_tofu(url):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Firefox(options=options)
    driver.get(url)
    time.sleep(3)  # time to wait to start scraping the html
    page = driver.page_source  # raw html
    driver.quit()
    soup = BeautifulSoup(page, 'html.parser')  # parsing html to text
    lowestPrice = soup.find('p', {'class': 'chakra-text css-0'}).text.replace(' ', '').replace('BNB', '')
    return float(lowestPrice)
#print(price_tofu('https://tofunft.com/collection/nft11players/items?sort=price_asc'))
'https://tofunft.com/collection/nft11-regular-player/items?sort=price_asc'
'https://tofunft.com/collection/nft-11-legend-player/items?sort=price_asc'
'https://tofunft.com/collection/nft11-stadium/items?sort=price_asc'

#Volumen (BNB), debe usarse la segunda función
'https://tofunft.com/collection/nft11-regular-player/activities?page='
'https://tofunft.com/collection/nft-11-legend-player/activities?page='
'https://tofunft.com/collection/nft11players/activities?page='
url='https://tofunft.com/collection/nft11-stadium/activities?page='
def page_volume_tofu(url,page=1):
    url=url+str(page)
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Firefox(options=options)
    driver.get(url)
    time.sleep(3)  # time to wait to start scraping the html
    source = driver.page_source  # raw html
    driver.quit()
    soup = BeautifulSoup(source, 'html.parser')  # parsing html to text
    lastSales = soup.findAll('span', {'class': ['chakra-text css-1dp94ug','chakra-text css-1y3pdrv']})
    salePrices = soup.findAll('p', {'class': 'chakra-text css-1uhznsn'})
    total_sales=len(lastSales)
    amountOfSales = 0
    volumeOfSales = 0
    for i in range(total_sales):
        if ('hour' in lastSales[i].text or 'minute' in lastSales[i].text):
            amountOfSales += 1
            volumeOfSales += float(salePrices[i].text.replace(' ', '').replace('BNB', ''))
    return [volumeOfSales,amountOfSales,total_sales,page]
def volume_tofu(url):
    volume,trades,page_trades,page=page_volume_tofu(url)
    add_trades=trades
    while add_trades==page_trades:
        add_volume,add_trades,page_trades,page=page_volume_tofu(url,page+1)
        volume+=add_volume
        trades+=add_trades
    return volume
#print(volume_tofu('https://tofunft.com/collection/nft-11-legend-player/activities?page='))