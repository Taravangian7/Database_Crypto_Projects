from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from credentials import server,usuario,contraseña,driver
from schema import Holders,Transfers_Historical,Holders_Historical,Assets,Socials
from projects import projects
from scraping import nfttoken_bscscan_data,token_bscscan_data
import pandas as pd
import numpy as np
from nft_scraping import price_tofu,volume_tofu
from datetime import datetime,timezone,timedelta
date1=datetime.now(timezone.utc)
date2=datetime.now(timezone.utc)-timedelta(minutes=200)
#print((date1-date2).days)

database='NFT11_test'
engine = create_engine(f'mssql+pyodbc://{usuario}:{contraseña}@{server}/{database}?driver={driver}')
Session=sessionmaker(bind=engine)
session=Session()
date="2023-06-08"
query=session.query(Assets)
df_assets = pd.read_sql(query.statement, session.connection())
for project in projects:
    for token in project.tokens_NFT:
            #Hacer por categoría?
            if token.price:
                print('elton')
                price=token.price(token.url_price)
                print(price)
                df_assets.loc[df_assets['upload_date'] == date, f'{token.name}_price'] = price
            if token.vol:
                vol=token.vol(token.url_vol)
                df_assets.loc[df_assets['upload_date'] == date, f'{token.name}_volume'] = vol
                print(vol)