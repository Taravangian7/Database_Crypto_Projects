from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from credentials import server,usuario,contraseña,driver
from schema import Holders,Transfers_Historical,Holders_Historical,Assets,Socials,Market_Context,Main_assets
from projects import projects
from scraping import nfttoken_bscscan_data,token_bscscan_data,scrape_twitter
import pandas as pd
import numpy as np
from nft_scraping import price_tofu,volume_tofu
from datetime import datetime,timezone,timedelta
from pbi_tables import table_pbi_4weeks_3months,table_pbi_assets_and_whales,table_pbi_socials,table_pbi_monthly
from credentials import twitter_mail,twitter_password,user_name
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
query=session.query(Socials)
df_socials = pd.read_sql(query.statement, session.connection())
query=session.query(Market_Context)
df_market = pd.read_sql(query.statement, session.connection())
query=session.query(Main_assets)
df_main_assets = pd.read_sql(query.statement, session.connection())
query=session.query(Holders_Historical)
df_holders = pd.read_sql(query.statement, session.connection())
query=session.query(Transfers_Historical)
df_transfer = pd.read_sql(query.statement, session.connection())
#pd.set_option('display.max_columns', None) 
dataframe=table_pbi_4weeks_3months(project=projects[0],social_dataframe=df_socials,market_dataframe=df_market,asset_dataframe=df_assets,date="2023-07-06",weekly=False)
dataframe.to_sql(name='PBI_3months_4weeks', con=engine, if_exists='replace', index=False)

dataframe=table_pbi_assets_and_whales(project=projects[0],asset_list=df_main_assets,holders_dataframe=df_holders,market_dataframe=df_market,asset_dataframe=df_assets,transfer_dataframe=df_transfer,date="2023-07-06",weekly=True)[0]
#print(dataframe)
#dataframe.to_sql(name='PBI_Assets', con=engine, if_exists='replace', index=False)

dataframe=table_pbi_assets_and_whales(project=projects[0],asset_list=df_main_assets,holders_dataframe=df_holders,market_dataframe=df_market,asset_dataframe=df_assets,transfer_dataframe=df_transfer,date="2023-07-06",weekly=True)[1]
dataframe.to_sql(name='PBI_Whales', con=engine, if_exists='replace', index=False)

dataframe=table_pbi_socials(project=projects[0],social_dataframe=df_socials,date="2023-07-06",weekly=True)
dataframe.to_sql(name='PBI_Socials', con=engine, if_exists='replace', index=False)

dataframe=table_pbi_monthly(project=projects[0],market_dataframe=df_market,transfer_dataframe=df_transfer,asset_dataframe=df_assets,date="2023-07-06")
dataframe.to_sql(name='PBI_Monthly', con=engine, if_exists='replace', index=False)