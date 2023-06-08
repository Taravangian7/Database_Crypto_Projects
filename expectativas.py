from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
import requests
import re
from bs4 import BeautifulSoup
import time
url='https://twitter.com/NFT11_Official'
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
print(followers)


