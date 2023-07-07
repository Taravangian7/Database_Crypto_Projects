from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from credentials import server,usuario,contraseña,driver
from schema import Holders,Transfers_Historical,Holders_Historical,Assets,Socials,Market_Context,Main_assets
from projects import projects
import pandas as pd
from pbi_tables import table_pbi_4weeks_3months,table_pbi_assets_and_whales,table_pbi_socials,table_pbi_monthly

date='2023-07-06'
weekly=True

for project in projects:
    database=project.name
    engine = create_engine(f'mssql+pyodbc://{usuario}:{contraseña}@{server}/{database}?driver={driver}')
    Session=sessionmaker(bind=engine)
    session=Session()
    query=session.query(Holders)
    df_holders = pd.read_sql(query.statement, session.connection())
    query=session.query(Transfers_Historical)
    df_transfers = pd.read_sql(query.statement, session.connection())
    query=session.query(Holders_Historical)
    df_holders_hist = pd.read_sql(query.statement, session.connection())
    query=session.query(Assets)
    df_assets = pd.read_sql(query.statement, session.connection())
    query=session.query(Market_Context)
    df_market_context = pd.read_sql(query.statement, session.connection())
    query=session.query(Socials)
    df_socials = pd.read_sql(query.statement, session.connection())
    query=session.query(Main_assets)
    df_main_assets = pd.read_sql(query.statement, session.connection())
    
    dataframe=table_pbi_4weeks_3months(project=project,social_dataframe=df_socials,market_dataframe=df_market_context,asset_dataframe=df_assets,date=date,weekly=weekly)
    dataframe.to_sql(name='PBI_3months_4weeks', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_assets_and_whales(project=project,asset_list=df_main_assets,holders_dataframe=df_holders_hist,market_dataframe=df_market_context,asset_dataframe=df_assets,transfer_dataframe=df_transfers,date=date,weekly=weekly)[0]
    dataframe.to_sql(name='PBI_Assets', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_assets_and_whales(project=project,asset_list=df_main_assets,holders_dataframe=df_holders_hist,market_dataframe=df_market_context,asset_dataframe=df_assets,transfer_dataframe=df_transfers,date=date,weekly=weekly)[1]
    dataframe.to_sql(name='PBI_Whales', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_socials(project=project,social_dataframe=df_socials,date=date,weekly=weekly)
    dataframe.to_sql(name='PBI_Socials', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_monthly(project=project,market_dataframe=df_market_context,transfer_dataframe=df_transfers,asset_dataframe=df_assets,date=date)
    dataframe.to_sql(name='PBI_Monthly', con=engine, if_exists='replace', index=False)