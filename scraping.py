import requests
from bs4 import BeautifulSoup
import re
from credentials import bsc_apikey
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from pycoingecko import CoinGeckoAPI
import pandas as pd
cg = CoinGeckoAPI()

def scrape_telegram(url):
    # Hacer la petición GET al sitio web
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    # Crear objeto BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    # Encontrar el div con la clase 'tgme_page_extra' y obtener su contenido
    div = soup.find('div', {'class': 'tgme_page_extra'}).text.replace(' ', '')
    tgGroupMembers = int(re.findall('[0-9]+', div)[0])
    return tgGroupMembers

def scrape_discord(url):
    # Hago petición GET
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(response.content, 'html.parser')
    # Busco en Head, porque si buscaba por Span(era lo más intuitivo) me encontraba que devolvía None por estar dentro de un iframe.
    # Si quiere hacerse con Span, es necesario usar Selenium.
    scrapedGroupMembers = soup.find('meta', {'property': 'og:description'}).get('content').replace(',', '')
    # Get the numbers from a string and use the [1] index to get the number of members
    discordGroupMembers = int(re.findall(' [0-9]+ ', scrapedGroupMembers)[0])
    return discordGroupMembers

def scrape_twitter(url):
    patron=r'[^/]+$'
    name=re.findall(patron,url)[0]
    #El scraping se hace usando Selenium, es decir que se abre una ventana para acceder a la web como si fuera un usuario y tomar los datos
    #Para que la ventana solo se abra dentro de python (y no la veamos), debemos agregar estas opciones
    options=Options() #Crea objeto options para configurar navegador
    options.add_argument('--headless') #Se ejecuta sin interfaz gráfica (la ventana se abre en segundo plano)
    options.add_argument('--disable-gpu') #desactivan gpu del sistema, mejora rendimiento
    driver = webdriver.Firefox(options=options) #Creas la instancia, dejas este objeto listo para acceder a una web.
    # Entras a la web, en este caso twitter
    driver.get(url)
    time.sleep(5)
    #Si tiene menos de 10000 seguidores el HTML tiene una forma, y si tiene más tiene otro. Por eso contemplo ambos casos
    try:
        page=driver.page_source
        soup = BeautifulSoup(page, 'html.parser')
        lowestPrices = soup.find('a', {'href': f'/{name}/followers'})
        followers=lowestPrices.find('span', {'class': 'css-901oao css-16my406 r-poiln3 r-bcqeeo r-qvutc0'}).text.replace('.','')
        followers=int(followers)
        driver.quit()
    except:
        #Obtenemos el elemento HTML de la página a la cual debemos pasar el cursor del mouse
        #El elemento lo obtenemos con la funcion find_element, y lo encontramos mediante su selector CSS
        #En este caso, buscamos un elemento tipo "a" con un atributo href=/NFT11_Official/followers, y dentro de el un span con class css-901oao
        #El espacio que hay entre a[href="/NFT11_Official/followers"] y span.css-901oao, implica que el segundo esta dentro del primero
        follower_count_element = driver.find_element(By.CSS_SELECTOR, f'a[href="/{name}/followers"] span.css-901oao')
        # Mover el puntero del mouse sobre el elemento
        #ActionChains lo que hace es simular una acción (se le pasa la instancia driver en la que queremos simular la acción)
        #move_to_element es la acción que realizamos, en este caso mover el mouse hacia el elemento indicado
        #perform() ejecuta la acción indicada anteriormente.
        hover = ActionChains(driver).move_to_element(follower_count_element)
        hover.perform()
        # Tooltip es la ventana emergente que aparece al pasar el cursor.
        #Estas ventanas emergente son objetos, y en general no salen en el HTML, ya que son objetos generados dinámicamente por JavaScript
        #Por lo general, estos objetos son tipo div y tiene el atributo role=Tooltip. Por eso lo buscamos con el siguiente path:
        tooltip_xpath = "//div[@role='tooltip']"
        #Webdriverwait sirve para establecer un tiempo máximo de espera en la instancia "driver". Se espera una cantidad de segundos o hasta que("until") otra condición se cumpla
        #En esta caso, en la condición until hacemos esperar hasta que encuentre el elemento tooltip.
        #visibility_of_element_located, lo que hace es checkear si un elemento esta visible. En este caso el elemento se indica por su xpath
        #Si el elemento se encuentra visible, lo almacenará en la variable tooltip
        tooltip = WebDriverWait(driver,10).until(EC.visibility_of_element_located((By.XPATH, tooltip_xpath)))
        # Obtener el texto del elemento
        followers = int(tooltip.text.replace('.','').replace(',',''))
        driver.quit()
    return followers



def price_vol(blockchain,days,address=None):
    vs_currency = "usd"
    # Hacer la petición a la API
    if address==None:
        url = f"https://api.coingecko.com/api/v3/coins/{blockchain}/market_chart"
    else:
        url = f"https://api.coingecko.com/api/v3/coins/{blockchain}/contract/{address}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval":"daily"}
    response = requests.get(url, params=params)
    # Obtener los datos de precios y volúmenes
    data = response.json()
    prices = data["prices"]
    volumes = data["total_volumes"]
    data=[]
    for price,volume in zip(prices,volumes):
        data.append([price[0],price[1],volume[1]])
    # Convertir los datos a dataframes de pandas y renombrar las columnas
    df_data = pd.DataFrame(data, columns=["Timestamp", "Price","Volume"])
    # Convertir la columna de timestamps a formato de fecha y hora
    df_data["upload_date"] = pd.to_datetime(df_data["Timestamp"], unit="ms")- pd.Timedelta(days=1)
    df_data["upload_date"] = df_data["upload_date"].dt.date
    # Eliminar la columna de timestamps ,axis=1 implica que se elimina una columna e inplace=True indica que se elimina del dataframe original.
    df_data.drop("Timestamp", axis=1, inplace=True)
    df_data=df_data.iloc[:-1]
    df_data.drop_duplicates(subset=["upload_date"], keep="first", inplace=True)
    # Imprimir los dataframes resultantes
    return df_data
#print(price_vol('bsc',2000,'0x73f67ae7f934ff15beabf55a28c2da1eeb9b56ec'))
#print(price_vol('binancecoin',2000))

def btc_dominance():
    url = "https://api.coingecko.com/api/v3/global"
    response = requests.get(url)
    data = response.json()
    bitcoin_dominance = data["data"]["market_cap_percentage"]["btc"]
    return bitcoin_dominance

#No es Hist
#La categoría debe ser una de esta lista en categoría https://api.coingecko.com/api/v3/coins/categories/list
def market_cap_category(category_id):
    url = "https://api.coingecko.com/api/v3/coins/categories"
    params = {"vs_currency": "usd"}
    response = requests.get(url, params=params)
    data = response.json()
    for element in data:
        if element['id']==category_id:
            market_cap=element['market_cap']
    return market_cap
#print(market_cap_category('gaming'))
def token_bscscan_data(contract_address,logs,sort='desc',startblock=0):
    url = url = f'https://api.bscscan.com/api?module=account&action=tokentx&contractaddress={contract_address}&startblock={startblock}&page=1&offset={logs}&sort={sort}&apikey={bsc_apikey}&tag=latest'
    response=requests.get(url)
    transactions = []
    for i in response.json()['result']:
        timestamp = int(i['timeStamp'])
        temp = {
            'dateTime': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
            'from': i['from'],
            'to': i['to'],
            'value': float(i['value'])/1000000000000000000,
            'tokenSymbol': i['tokenSymbol'],
            'blocknumber':i['blockNumber'],
            'hash':i['hash'],
            'method': i['input'][:10]}
        transactions.append(temp)
    df_bscscan=pd.DataFrame(transactions)    
    return df_bscscan 
#print(token_bscscan_data('0x73F67AE7f934FF15beaBf55A28C2Da1eEb9B56Ec',10000,'asc',2863350700)) 

#Para NFTs
def nfttoken_bscscan_data(contract_address,logs,sort='desc',startblock=0):
    url = url = f'https://api.bscscan.com/api?module=account&action=tokennfttx&contractaddress={contract_address}&startblock={startblock}&page=1&offset={logs}&sort={sort}&apikey={bsc_apikey}&tag=latest'
    response=requests.get(url)
    transactions = []
    for i in response.json()['result']:
        ts = int(i['timeStamp'])
        temp = {
            'dateTime': datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),
            'from': i['from'],
            'to': i['to'],
            'tokenID': int(i['tokenID']),
            'tokenSymbol': i['tokenSymbol'],
            'blocknumber':i['blockNumber'],
            'hash':i['hash'],
            'method': i['input'][:10]}
        transactions.append(temp)
    df_bscscan=pd.DataFrame(transactions)    
    return df_bscscan
#print(nfttoken_bscscan_data('0xc2dea142de50b58f2dc82f2cafda9e08c3323d53',10000,'asc',0)) 
