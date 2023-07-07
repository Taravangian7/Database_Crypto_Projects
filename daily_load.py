from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from projects import projects
from credentials import server,usuario,contraseña,driver, user_name,twitter_mail,twitter_password
from schema import Holders,Transfers_Historical,Holders_Historical,Assets,Market_Context,Socials,Main_assets
import pandas as pd
from datetime import datetime, timezone,timedelta
from scraping import price_vol,token_bscscan_data,nfttoken_bscscan_data,market_cap_category,scrape_telegram,scrape_discord,scrape_twitter,coin_price
import numpy as np
import time
from pbi_tables import table_pbi_4weeks_3months,table_pbi_assets_and_whales,table_pbi_socials,table_pbi_monthly

def get_all_transfers(token,today,last_date,last_blocknumber,NFT=False):
    block_number=last_blocknumber
    last_date = pd.to_datetime(last_date, format='%Y-%m-%d %H:%M:%S')
    logs_obtained=1000
    while logs_obtained ==1000:
        if NFT:
            new_dataframe = nfttoken_bscscan_data(token.contract_address,1000,'asc',block_number)
        else:
            new_dataframe = token_bscscan_data(token.contract_address,1000,'asc',block_number)
        if 'dataframe' in locals():
            dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)
        else:
            dataframe=new_dataframe
        if not dataframe.empty:
            block_number = dataframe.iloc[-1]['blocknumber']
        logs_obtained=len(new_dataframe)
        if logs_obtained ==1000:
            mask=dataframe['blocknumber'] == block_number
            dataframe.drop(dataframe.index[mask], inplace=True)
    if not dataframe.empty:        
        dataframe = dataframe.sort_values('dateTime', ascending=False)
        dataframe['dateTime']=pd.to_datetime(dataframe['dateTime'])
        dataframe['date'] = dataframe['dateTime'].dt.date
        mask = dataframe['date'] != today
        dataframe = dataframe[mask]
        mask= dataframe['dateTime']>last_date
        dataframe = dataframe[mask]
        dataframe['from'] = dataframe['from'].apply(lambda x: x.lower())
        dataframe['to'] = dataframe['to'].apply(lambda x: x.lower())
    elif NFT:
        columns = ['dateTime', 'from', 'to', 'tokenID', 'tokenSymbol', 'blocknumber', 'hash', 'method']
        dtypes = {'dateTime': 'datetime64', 'from': 'object', 'to': 'object', 'tokenID': 'int64',
          'tokenSymbol': 'object', 'blocknumber': 'int64', 'hash': 'object', 'method': 'object'}
        dataframe = pd.DataFrame(columns=columns).astype(dtypes)
    else:
        columns = ['dateTime', 'from', 'to', 'value', 'tokenSymbol', 'blocknumber', 'hash', 'method']
        dtypes = {'dateTime': 'datetime64', 'from': 'object', 'to': 'object', 'value': 'float64',
          'tokenSymbol': 'object', 'blocknumber': 'int64', 'hash': 'object', 'method': 'object'}
        dataframe = pd.DataFrame(columns=columns).astype(dtypes)
    return dataframe

def holding_staking_role_update(token,df_holders,holding_staking,NFT=False):
    if NFT:
        my_dataframe=nfttoken_dataframe
        my_column='tokenID'
        aggregation='count'
    else:
        my_dataframe=token_dataframe
        my_column='value'
        aggregation='sum'
    if holding_staking=='Holding':
        to_staking_filter=logs_asset_filter
        from_staking_filter=logs_asset_filter
    elif holding_staking=='Staking':
        to_staking_filter=logs_to_staking
        from_staking_filter=logs_from_staking
    to_address_balance=my_dataframe[logs_date_filter & logs_asset_filter & from_staking_filter].groupby('to').agg({my_column: aggregation})
    to_address_balance.reset_index(inplace=True)
    to_address_balance.rename(columns={'to': 'address'}, inplace=True)
    from_address_balance=my_dataframe[logs_date_filter & logs_asset_filter & to_staking_filter].groupby('from').agg({my_column: aggregation})
    from_address_balance.reset_index(inplace=True)
    from_address_balance.rename(columns={'from': 'address'}, inplace=True)
    if holding_staking=='Holding':
        from_address_balance[my_column] = from_address_balance[my_column] * -1
    elif holding_staking=='Staking':
        to_address_balance[my_column] = to_address_balance[my_column] * -1
    balance=pd.concat([to_address_balance,from_address_balance],ignore_index=True)
    balance=balance.groupby('address').agg({my_column: 'sum'})
    balance.reset_index(inplace=True)
    new_addresses= pd.DataFrame({'address':balance[~balance['address'].isin(df_holders['address'])]['address']})
    df_holders=pd.concat([df_holders,new_addresses],ignore_index=True) #Acá los tipo int me los volvio float
    #Divido en 10 para no consumir tanta memoria.Sino salta error.
    holder_chunks = np.array_split(balance, 3)
    if holder_chunks: #Si tengo lista vacía no hago nada.
        for holder_chunk in holder_chunks:
            merged_holders = pd.merge(df_holders,holder_chunk, on='address', how='left')
            merged_holders[f'{token.name}_{holding_staking}'] = merged_holders[f'{token.name}_{holding_staking}'].fillna(0)
            merged_holders[my_column] = merged_holders[my_column].fillna(0)
            merged_holders[f'{token.name}_{holding_staking}'] += merged_holders[my_column]
            merged_holders = merged_holders.drop(my_column, axis=1)
            df_holders=merged_holders
    if not NFT:
        df_holders[f'{token.name}_{holding_staking}'] = df_holders[f'{token.name}_{holding_staking}'].apply(lambda x: 0 if -0.01<x<0.0001 else x)
    return df_holders

def get_dates_between(date1, date2):
    dates = []
    current_date = date1
    while current_date <= date2:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates
btc_price=0
eth_price=0
bnb_price=0
while btc_price==0 or eth_price==0 or bnb_price==0:
    if btc_price==0:
        try:
            btc_price=coin_price('bitcoin')
        except:
            time.sleep(1)
    if eth_price==0:
        try:
            eth_price=coin_price('ethereum')
        except:
            time.sleep(1)
    if bnb_price==0:
        try:
            bnb_price=coin_price('bnb')
        except:
            time.sleep(1)
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
    last_marketcap=df_market_context.loc[df_market_context.index[-1], 'marketcap_niche']
    today = datetime.now(timezone.utc).date()
    last_datetime= df_holders_hist.iloc[-1]['last_transfer']
    last_block_number= df_holders_hist.iloc[-1]['last_block_number']
    if 'token_dataframe' in globals():
        del token_dataframe
    if 'nfttoken_dataframe' in globals():
        del nfttoken_dataframe
    for token in project.tokens:
        dataframe = get_all_transfers(token,today=today,last_date=last_datetime,last_blocknumber=last_block_number)
        if 'token_dataframe' in globals():
            token_dataframe=pd.concat([token_dataframe,dataframe],ignore_index=True)
        else:
            token_dataframe=dataframe
        del dataframe
    for token in project.tokens_NFT:
        dataframe = get_all_transfers(token,today=today,last_date=last_datetime,last_blocknumber=last_block_number,NFT=True)
        if 'nfttoken_dataframe' in globals():
            nfttoken_dataframe=pd.concat([nfttoken_dataframe,dataframe],ignore_index=True)
        else:
            nfttoken_dataframe=dataframe
        del dataframe
    #query=session.query(Holders_Historical)
    #df_holders_hist = pd.read_sql(query.statement, session.connection())
    #query=session.query(Assets)
    #df_assets = pd.read_sql(query.statement, session.connection())
    index=df_holders_hist.iloc[-1]['id']+1

    if not(token_dataframe.empty and nfttoken_dataframe.empty):
        #Cálculo de la última fecha y último bloque
        dates= pd.concat([token_dataframe[['dateTime','blocknumber']], nfttoken_dataframe[['dateTime','blocknumber']]], ignore_index=True)
        dates = pd.DataFrame(dates, columns=['dateTime', 'blocknumber'])
        dates = dates.sort_values('dateTime', ascending=False)
        last_datetime= dates.loc[0]['dateTime'] #Para holders_hist
        last_block_number=dates.loc[0]['blocknumber']
    date=df_holders_hist.iloc[-1]['upload_date']+timedelta(days=1)
    min_date=date
    delta=timedelta(days=1)
    last_date = datetime.now(timezone.utc).date()-timedelta(days=1)    
    difference_dates=(last_date-date).days
    if date>last_date:
        print('Ya Actualizado')
        pass
    #Carga de datos por día
    while date<=last_date:
        df_transfers.loc[len(df_transfers)] = [date] + [0] * (len(df_transfers.columns) - 1)
        df_holders_hist.loc[len(df_holders_hist)] = [index] + [date] + [last_datetime]+[last_block_number]+ [0] * (len(df_holders_hist.columns) - 4)
        df_assets.loc[len(df_assets)] = [index] + [date] +  [0] * (len(df_assets.columns) - 2)
        df_market_context.loc[len(df_market_context)] = [index] + [date] +  [0] * (len(df_market_context.columns) - 2)
        #print('fase 1 completa')
        for token in project.tokens:
            if not token_dataframe.empty:
                whales= df_holders.loc[df_holders[f'{token.name}_Role']=='Whale']['address']
                logs_to_whales = token_dataframe['to'].isin(list(whales))
                logs_from_whales= token_dataframe['from'].isin(list(whales))
                logs_from_dex = token_dataframe['from'].isin(list(df_holders[df_holders['address_owner']=='DEX']['address']))
                logs_to_dex = token_dataframe['to'].isin(list(df_holders[df_holders['address_owner']=='DEX']['address']))
                logs_from_staking = token_dataframe['from'].isin([i.lower() for i in token.staking_addresses])
                logs_to_staking = token_dataframe['to'].isin([i.lower() for i in token.staking_addresses]) #El resto de address ya pasó por lower()
                logs_asset_filter = token_dataframe['tokenSymbol']==token.symbol
                logs_date_filter = token_dataframe['date'] == date
                logs_mint_address=token_dataframe['from'].isin(project.minting_addresses)
                logs_burn_address=token_dataframe['to'].isin(project.burning_addresses)

                bought_whale_filter = logs_to_whales & logs_from_dex & logs_asset_filter & logs_date_filter
                sold_whale_filter= logs_from_whales & logs_to_dex & logs_asset_filter & logs_date_filter
                staking_in_whale_filter= logs_from_whales & logs_to_staking & logs_asset_filter & logs_date_filter
                staking_out_whale_filter= logs_to_whales & logs_from_staking & logs_asset_filter & logs_date_filter

                whales_bought=token_dataframe[bought_whale_filter]['value'].fillna(0).sum()
                whales_sold=token_dataframe[sold_whale_filter]['value'].fillna(0).sum()
                whales_staking= token_dataframe[staking_in_whale_filter]['value'].fillna(0).sum()-token_dataframe[staking_out_whale_filter]['value'].fillna(0).sum()
                #print('fase 2 completa')
                #Actualizo holding del token
                df_holders=holding_staking_role_update(token,df_holders=df_holders,holding_staking='Holding')
                #Actualizo staking del token
                df_holders=holding_staking_role_update(token,df_holders=df_holders,holding_staking='Staking')
                #Actualizo categoría de ballenas
                whale_amount=float(token.whale_amount)
                whale_mask=(df_holders[f'{token.name}_Staking']+df_holders[f'{token.name}_Holding'])>= whale_amount
                not_declared_address=~df_holders['address_owner'].isin(['DEX','Burning','Minting','Project'])
                df_holders.loc[whale_mask & not_declared_address, f'{token.name}_Role'] = 'Whale'
                df_holders.loc[~(whale_mask & not_declared_address), f'{token.name}_Role'] = '-'
                #Transfer historical
                not_project_address=~df_holders['address_owner'].isin(['Project'])
                new_whales= df_holders.loc[(df_holders[f'{token.name}_Role']=='Whale') & (~df_holders['address'].isin(list(whales)))]['address']
                logs_to_whales = token_dataframe['to'].isin(list(new_whales))
                logs_from_whales= token_dataframe['from'].isin(list(new_whales))
                logs_not_project_addresses=token_dataframe['from'].isin(list(df_holders[not_project_address]['address']))&token_dataframe['to'].isin(list(df_holders[not_project_address]['address']))

                bought_whale_filter = logs_to_whales & logs_from_dex & logs_asset_filter & logs_date_filter
                sold_whale_filter= logs_from_whales & logs_to_dex & logs_asset_filter & logs_date_filter
                staking_in_whale_filter= logs_from_whales & logs_to_staking & logs_asset_filter & logs_date_filter
                staking_out_whale_filter= logs_to_whales & logs_from_staking & logs_asset_filter & logs_date_filter

                whales_bought+=token_dataframe[bought_whale_filter]['value'].fillna(0).sum()
                whales_sold+=token_dataframe[sold_whale_filter]['value'].fillna(0).sum()
                whales_staking+= token_dataframe[staking_in_whale_filter]['value'].fillna(0).sum()-token_dataframe[staking_out_whale_filter]['value'].fillna(0).sum()
                bought=token_dataframe[logs_from_dex & logs_asset_filter & logs_date_filter]['value'].fillna(0).sum()
                sold=token_dataframe[logs_to_dex & logs_asset_filter & logs_date_filter]['value'].fillna(0).sum()
                staked=token_dataframe[logs_to_staking & logs_asset_filter & logs_date_filter]['value'].fillna(0).sum()-token_dataframe[logs_from_staking & logs_asset_filter & logs_date_filter]['value'].fillna(0).sum()
                minted_total=token_dataframe[logs_mint_address & logs_asset_filter & logs_date_filter]['value'].fillna(0).sum()
                minted_not_project=token_dataframe[logs_mint_address & logs_asset_filter & logs_date_filter & logs_not_project_addresses]['value'].fillna(0).sum()
                burned=token_dataframe[logs_burn_address & logs_asset_filter & logs_date_filter]['value'].fillna(0).sum()
                
                #Actualizo dataframe_transfer_historical
                columns=[f'{token.name}_bought_total', f'{token.name}_bought_whales',f'{token.name}_sold_total',f'{token.name}_sold_whales',f'{token.name}_staking',f'{token.name}_staking_whales',f'{token.name}_minted_total',f'{token.name}_burned',f'{token.name}_minted_private']
                values=[bought,whales_bought,sold,whales_sold,staked,whales_staking,minted_total,burned,minted_not_project]
                for column, value in zip(columns, values):
                    df_transfers.loc[df_transfers['transfer_date'] == date, column] = value
            #Actualizo dataframe holders_historical
            whale_mask=df_holders[f'{token.name}_Role']=='Whale'
            whales=df_holders[whale_mask].shape[0]
            whales_balance_total=df_holders[whale_mask][f'{token.name}_Holding'].sum()+df_holders[whale_mask][f'{token.name}_Staking'].sum()
            whales_staking=df_holders[whale_mask][f'{token.name}_Staking'].sum()
            columns=[f'{token.name}_whales',f'{token.name}_whales_balance',f'{token.name}_whales_staking']
            values=[whales,whales_balance_total,whales_staking]
            for column, value in zip(columns, values):
                df_holders_hist.loc[df_holders_hist['upload_date'] == date, column] = value
            #Actualizo dataframe assets
            holders=df_holders[(df_holders[f'{token.name}_Staking']+df_holders[f'{token.name}_Holding'])>0].shape[0] 
            burned=df_holders[df_holders['address_owner']=='Burning'][f'{token.name}_Holding'].fillna(0).sum()
            minted=df_holders[df_holders['address_owner']=='Minting'][f'{token.name}_Holding'].fillna(0).sum()*(-1)
            circulating=minted-burned
            columns=[f'{token.name}_holders',f'{token.name}_circulating',f'{token.name}_burned']
            values=[holders,circulating,burned]
            for column, value in zip(columns, values):
                df_assets.loc[df_assets['upload_date'] == date, column] = value
        for token in project.tokens_NFT:
            if not nfttoken_dataframe.empty:
                whales= df_holders.loc[df_holders[f'{token.name}_Role']=='Whale']['address']
                logs_to_whales = nfttoken_dataframe['to'].isin(list(whales))
                logs_from_whales= nfttoken_dataframe['from'].isin(list(whales)) 
                logs_from_dex = nfttoken_dataframe['from'].isin(list(df_holders.loc[df_holders['address_owner']=='DEX']['address']))
                logs_to_dex = nfttoken_dataframe['to'].isin(list(df_holders.loc[df_holders['address_owner']=='DEX']['address']))
                logs_from_staking = nfttoken_dataframe['from'].isin([i.lower() for i in token.staking_addresses])
                logs_to_staking = nfttoken_dataframe['to'].isin([i.lower() for i in token.staking_addresses])
                logs_asset_filter = nfttoken_dataframe['tokenSymbol']==token.symbol
                logs_date_filter = nfttoken_dataframe['date'] == date
                logs_mint_address=nfttoken_dataframe['from'].isin(project.minting_addresses)
                logs_burn_address=nfttoken_dataframe['to'].isin(project.burning_addresses)

                bought_whale_filter = logs_to_whales & logs_from_dex & logs_asset_filter & logs_date_filter
                sold_whale_filter= logs_from_whales & logs_to_dex & logs_asset_filter & logs_date_filter
                staking_in_whale_filter= logs_from_whales & logs_to_staking & logs_asset_filter & logs_date_filter
                staking_out_whale_filter= logs_to_whales & logs_from_staking & logs_asset_filter & logs_date_filter

                whales_bought=nfttoken_dataframe[bought_whale_filter].shape[0]
                whales_sold=nfttoken_dataframe[sold_whale_filter].shape[0]
                whales_staking= nfttoken_dataframe[staking_in_whale_filter].shape[0]-nfttoken_dataframe[staking_out_whale_filter].shape[0]
                #Actualizo holding del token
                df_holders=holding_staking_role_update(token,df_holders=df_holders,holding_staking='Holding',NFT=True)
                #Actualizo staking del token
                df_holders=holding_staking_role_update(token,df_holders=df_holders,holding_staking='Staking',NFT=True)
                #Rango de ballenas
                whale_amount=float(token.whale_amount)
                whale_mask=(df_holders[f'{token.name}_Staking']+df_holders[f'{token.name}_Holding'])>= whale_amount
                not_declared_address=~df_holders['address_owner'].isin(['DEX','Burning','Minting','Project'])
                df_holders.loc[whale_mask & not_declared_address, f'{token.name}_Role'] = 'Whale'
                df_holders.loc[~(whale_mask & not_declared_address), f'{token.name}_Role'] = '-'
                #Transfer historical
                not_project_address=~df_holders['address_owner'].isin(['Project'])
                new_whales= df_holders.loc[(df_holders[f'{token.name}_Role']=='Whale') & (~df_holders['address'].isin(list(whales)))]['address']
                logs_to_whales = nfttoken_dataframe['to'].isin(list(new_whales))
                logs_from_whales= nfttoken_dataframe['from'].isin(list(new_whales))
                logs_not_project_addresses= nfttoken_dataframe['from'].isin(list(df_holders[not_project_address]['address'])) & nfttoken_dataframe['to'].isin(list(df_holders[not_project_address]['address']))

                bought_whale_filter = logs_to_whales & logs_from_dex & logs_asset_filter & logs_date_filter
                sold_whale_filter= logs_from_whales & logs_to_dex & logs_asset_filter & logs_date_filter
                staking_in_whale_filter= logs_from_whales & logs_to_staking & logs_asset_filter & logs_date_filter
                staking_out_whale_filter= logs_to_whales & logs_from_staking & logs_asset_filter & logs_date_filter

                whales_bought+=nfttoken_dataframe[bought_whale_filter].shape[0]
                whales_sold+=nfttoken_dataframe[sold_whale_filter].shape[0]
                whales_staking+= nfttoken_dataframe[staking_in_whale_filter].shape[0]-nfttoken_dataframe[staking_out_whale_filter].shape[0]
                bought=nfttoken_dataframe[logs_from_dex & logs_asset_filter & logs_date_filter].shape[0]
                sold=nfttoken_dataframe[logs_to_dex & logs_asset_filter & logs_date_filter].shape[0]
                staked=nfttoken_dataframe[logs_to_staking & logs_asset_filter & logs_date_filter].shape[0]-nfttoken_dataframe[logs_from_staking & logs_asset_filter & logs_date_filter].shape[0]
                minted_total=nfttoken_dataframe[logs_mint_address & logs_asset_filter & logs_date_filter].shape[0]
                minted_not_project=nfttoken_dataframe[logs_mint_address & logs_asset_filter & logs_date_filter & logs_not_project_addresses].shape[0]
                burned=nfttoken_dataframe[logs_burn_address & logs_asset_filter & logs_date_filter].shape[0]
                #Actualizo dataframe_transfer_historical
                columns=[f'{token.name}_bought_total', f'{token.name}_bought_whales',f'{token.name}_sold_total',f'{token.name}_sold_whales',f'{token.name}_staking',f'{token.name}_staking_whales',f'{token.name}_minted_total',f'{token.name}_burned',f'{token.name}_minted_private']
                values=[bought,whales_bought,sold,whales_sold,staked,whales_staking,minted_total,burned,minted_not_project]
                for column, value in zip(columns, values):
                    df_transfers.loc[df_transfers['transfer_date'] == date, column] = value
            #Actualizo holders_historical
            whale_mask=df_holders[f'{token.name}_Role']=='Whale'
            whales=df_holders[whale_mask].shape[0]
            whales_balance_total=df_holders[whale_mask][f'{token.name}_Holding'].sum()+df_holders[whale_mask][f'{token.name}_Staking'].sum()
            whales_staking=df_holders[whale_mask][f'{token.name}_Staking'].sum()
            columns=[f'{token.name}_whales',f'{token.name}_whales_balance',f'{token.name}_whales_staking']
            values=[whales,whales_balance_total,whales_staking]
            for column, value in zip(columns, values):
                df_holders_hist.loc[df_holders_hist['upload_date'] == date, column] = value
            #Actualizo dataframe assets
            holders=df_holders[(df_holders[f'{token.name}_Staking']+df_holders[f'{token.name}_Holding'])>0].shape[0] 
            burned=df_holders[df_holders['address_owner']=='Burning'][f'{token.name}_Holding'].fillna(0).sum()
            minted=df_holders[df_holders['address_owner']=='Minting'][f'{token.name}_Holding'].fillna(0).sum()*(-1)
            circulating=minted-burned
            columns=[f'{token.name}_holders',f'{token.name}_circulating',f'{token.name}_burned']
            values=[holders,circulating,burned]
            for column, value in zip(columns, values):
                df_assets.loc[df_assets['upload_date'] == date, column] = value
        #código #2,#3,#4 y #5
        print(date)
        date+=delta
        index+=1
    date-=delta
    index-=1    
    #TABLA MARKET CONTEXT
    df_btc=price_vol('bitcoin',difference_dates+3)
    df_btc.drop('Volume', axis=1, inplace=True)
    df_eth=price_vol('ethereum',difference_dates+3)
    df_eth.drop('Volume', axis=1, inplace=True)
    df_bnb=price_vol('binancecoin',difference_dates+3)
    df_bnb.drop('Volume', axis=1, inplace=True)
    df_market_context=pd.merge(df_market_context,df_btc, on='upload_date', how='left')
    df_market_context['btc_price'].fillna(df_market_context['Price'], inplace=True)
    mask = df_market_context['btc_price'] == 0
    df_market_context.loc[mask, 'btc_price'] = df_market_context.loc[mask, 'Price']
    df_market_context.loc[df_market_context.index[-1], 'btc_price'] = btc_price
    df_market_context.drop('Price', axis=1, inplace=True)
    df_market_context=pd.merge(df_market_context,df_eth, on='upload_date', how='left')
    df_market_context['eth_price'].fillna(df_market_context['Price'], inplace=True)
    mask = df_market_context['eth_price'] == 0
    df_market_context.loc[mask, 'eth_price'] = df_market_context.loc[mask, 'Price']
    df_market_context.loc[df_market_context.index[-1], 'eth_price'] = eth_price
    df_market_context.drop('Price', axis=1, inplace=True)
    df_market_context=pd.merge(df_market_context,df_bnb, on='upload_date', how='left')
    df_market_context['bnb_price'].fillna(df_market_context['Price'], inplace=True)
    mask = df_market_context['bnb_price'] == 0
    df_market_context.loc[mask, 'bnb_price'] = df_market_context.loc[mask, 'Price']
    df_market_context.loc[df_market_context.index[-1], 'bnb_price'] = bnb_price
    df_market_context.drop('Price', axis=1, inplace=True)
    market_difference_dates=difference_dates
    while market_difference_dates>=1:
        new_date=date-timedelta(days=market_difference_dates)
        df_market_context.loc[df_market_context['upload_date'] == new_date, 'marketcap_niche'] = last_marketcap
        market_difference_dates-=1
    market_cap_niche= market_cap_category(project.category)
    df_market_context.loc[df_market_context.index[-1], 'marketcap_niche'] = market_cap_niche        
    #COMPLETO TABLA ASSETS. HAY REGISTROS QUE APARECEN NAN, ESTO ES PORQUE EL PRECIO NO VARIO NI HUBO VOLUMEN, ASÍ QUE LOS COMPLETO
    for token in project.tokens:
        df_price_vol=price_vol(token.blockchain,difference_dates+3,token.contract_address)
        df_assets = pd.merge(df_assets,df_price_vol, on='upload_date', how='left')
        mask = df_assets[f'{token.name}_price'] == 0
        df_assets.loc[mask, f'{token.name}_price'] = df_assets.loc[mask, 'Price']
        mask = df_assets[f'{token.name}_volume'] == 0
        df_assets.loc[mask, f'{token.name}_volume'] = df_assets.loc[mask, 'Volume']
        idx = df_assets[f'{token.name}_price'].first_valid_index()
        df_assets.loc[idx:, f'{token.name}_price'].fillna(method='ffill', inplace=True)
        idx = df_assets[f'{token.name}_volume'].first_valid_index()
        df_assets.loc[idx:, f'{token.name}_volume'].fillna(0, inplace=True)
        df_assets.drop('Price', axis=1, inplace=True)
        df_assets.drop('Volume', axis=1, inplace=True)
        token_price=0
        while token_price==0:
            try:
                token_price=coin_price(token.name)
            except:
                time.sleep(1)
        df_assets.loc[df_assets.index[-1], f'{token.name}_price'] = token_price
    for token in project.tokens_NFT:
        #Hacer por categoría?
        if token.blockchain=='BSC':
            coin=bnb_price
        elif token.blockchain=='Ethereum':
            coin=eth_price
        if token.price:
            price=token.price(token.url_price)
            df_assets.loc[df_assets['upload_date'] == date, f'{token.name}_price'] = (price*coin)
        if token.vol:
            vol=token.vol(token.url_vol)
            df_assets.loc[df_assets['upload_date'] == date, f'{token.name}_volume'] = (vol*coin)
    #COPIO DATAFRAME PARA TABLA PBI
    df_transfers_pbi=df_transfers
    df_holders_hist_pbi=df_holders_hist
    df_assets_pbi=df_assets
    #AGREGO SOLO LOS NUEVOS REGISTROS. 
    dates=get_dates_between(min_date, last_date)
    dates_mask_transfer = df_transfers['transfer_date'].isin(dates)
    dates_mask_holders_hist = df_holders_hist['upload_date'].isin(dates)
    dates_mask_assets = df_assets['upload_date'].isin(dates)
    df_transfers=df_transfers[dates_mask_transfer]
    df_holders_hist=df_holders_hist[dates_mask_holders_hist]
    df_assets=df_assets[dates_mask_assets]
    #CARGO LA DATA A SQL SERVER
    df_holders.to_sql(name='Holders', con=engine, if_exists='replace', index=False)
    df_transfers.to_sql(name='Transfers_Historical', con=engine, if_exists='append', index=False)
    session.commit()
    df_holders_hist.to_sql(name='Holders_Historical', con=engine, if_exists='append', index=False)
    df_assets.to_sql(name='Assets', con=engine, if_exists='append', index=False)
    df_market_context.to_sql(name='Market_Context', con=engine, if_exists='replace', index=False)
    #AGREGO REGISTO DEL DÍA A SOCIALS
    twitter=scrape_twitter(project.twitter,username=twitter_mail,password=twitter_password,user_name=user_name)
    discord=scrape_discord(project.discord)
    columns=[]
    values=[]
    for group in project.telegram:
        telegram_group= f'telegram_{list(group.keys())[0]}_members'
        value=scrape_telegram(list(group.values())[0])
        columns.append(telegram_group)
        values.append(value)
    if df_socials.empty:
        first_row=Socials(id=int(index),upload_date=date,twitter_members=twitter,discord_members=discord)
        for column, value in zip(columns, values):
            setattr(first_row, column, value)
        session.add(first_row)
        session.commit()
    else:
        last_twitter=df_socials.loc[df_socials.index[-1], 'twitter_members']
        last_discord=df_socials.loc[df_socials.index[-1], 'discord_members']
        last_columns=[]
        last_values=[]
        for group in project.telegram:
            last_telegram_group= f'telegram_{list(group.keys())[0]}_members'
            last_value=df_socials.loc[df_socials.index[-1], last_telegram_group]
            last_columns.append(last_telegram_group)
            last_values.append(last_value)
        while difference_dates>=1:
            new_date=date-timedelta(days=difference_dates)
            df_socials.loc[len(df_socials)] = [index-difference_dates] + [new_date] + [last_twitter] + [last_discord] + [0] * (len(df_socials.columns) - 4)
            for last_column, last_value in zip(last_columns, last_values):
                df_socials.loc[df_socials['upload_date'] == new_date, last_column] = last_value
            difference_dates-=1
        df_socials.loc[len(df_socials)] = [index] + [date] + [twitter] + [discord] + [0] * (len(df_socials.columns) - 4)
        for column, value in zip(columns, values):
            df_socials.loc[df_socials['upload_date'] == date, column] = value
        df_socials_pbi=df_socials
        dates_mask_socials = df_socials['upload_date'].isin(dates)
        df_socials=df_socials[dates_mask_socials]
        df_socials.to_sql(name='Socials', con=engine, if_exists='append', index=False)
    #CARGO TABLAS PBI
    dataframe=table_pbi_4weeks_3months(project=project,social_dataframe=df_socials_pbi,market_dataframe=df_market_context,asset_dataframe=df_assets_pbi,date=date,weekly=False)
    dataframe.to_sql(name='PBI_3months_4weeks', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_assets_and_whales(project=project,asset_list=df_main_assets,holders_dataframe=df_holders_hist_pbi,market_dataframe=df_market_context,asset_dataframe=df_assets_pbi,transfer_dataframe=df_transfers_pbi,date=date,weekly=True)[0]
    dataframe.to_sql(name='PBI_Assets', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_assets_and_whales(project=project,asset_list=df_main_assets,holders_dataframe=df_holders_hist_pbi,market_dataframe=df_market_context,asset_dataframe=df_assets_pbi,transfer_dataframe=df_transfers_pbi,date=date,weekly=True)[1]
    dataframe.to_sql(name='PBI_Whales', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_socials(project=project,social_dataframe=df_socials,date=date,weekly=True)
    dataframe.to_sql(name='PBI_Socials', con=engine, if_exists='replace', index=False)

    dataframe=table_pbi_monthly(project=project,market_dataframe=df_market_context,transfer_dataframe=df_transfers_pbi,asset_dataframe=df_assets_pbi,date=date)
    dataframe.to_sql(name='PBI_Monthly', con=engine, if_exists='replace', index=False)
