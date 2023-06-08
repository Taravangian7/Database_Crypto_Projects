from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from credentials import server,usuario,contraseña,driver
from schema import Holders,Transfers_Historical,Holders_Historical,Assets,Socials
from projects import projects
from scraping import nfttoken_bscscan_data,token_bscscan_data
import pandas as pd
import numpy as np
from datetime import datetime,timezone,timedelta
date1=datetime.now(timezone.utc)
date2=datetime.now(timezone.utc)-timedelta(minutes=200)
#print((date1-date2).days)

database='NFT11_test'
engine = create_engine(f'mssql+pyodbc://{usuario}:{contraseña}@{server}/{database}?driver={driver}')
Session=sessionmaker(bind=engine)
session=Session()
query=session.query(Socials)
df_socials = pd.read_sql(query.statement, session.connection())
print(df_socials)