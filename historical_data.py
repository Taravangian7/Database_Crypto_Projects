from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from credentials import server,usuario,contraseña,driver
from schema import Holders,Transfers_Historical,Holders_Historical,Assets,Market_Context
from projects import projects
from scraping import price_vol,token_bscscan_data,nfttoken_bscscan_data
import pandas as pd
from datetime import datetime, timezone,timedelta
import numpy as np

def get_all_transfers(token,today,NFT=False):
    block_number=0
    logs_obtained=10000
    while logs_obtained==10000:
        if NFT:
            new_dataframe = nfttoken_bscscan_data(token.contract_address,10000,'asc',block_number)
        else:
            new_dataframe = token_bscscan_data(token.contract_address,10000,'asc',block_number)
        if 'dataframe' in locals():
            dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)
        else:
            dataframe=new_dataframe
        block_number = dataframe.iloc[-1]['blocknumber']
        logs_obtained=len(new_dataframe)
        if logs_obtained ==10000:
            mask=dataframe['blocknumber'] == block_number
            dataframe.drop(dataframe.index[mask], inplace=True)
        #logs_obtained=100 #ESTO SE DEBE BORRAR!!!!!!!!
    dataframe = dataframe.sort_values('dateTime', ascending=False)
    dataframe['dateTime']=pd.to_datetime(dataframe['dateTime'])
    dataframe['date'] = dataframe['dateTime'].dt.date
    mask = dataframe['date'] != today
    dataframe = dataframe[mask]
    dataframe['from'] = dataframe['from'].apply(lambda x: x.lower())
    dataframe['to'] = dataframe['to'].apply(lambda x: x.lower())
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

for project in projects:
    database=project.name
    engine = create_engine(f'mssql+pyodbc://{usuario}:{contraseña}@{server}/{database}?driver={driver}')
    Session=sessionmaker(bind=engine)
    session=Session()
    query=session.query(Holders)
    df_holders = pd.read_sql(query.statement, session.connection())
    query=session.query(Transfers_Historical)
    df_transfers = pd.read_sql(query.statement, session.connection())
    test_date=df_transfers['transfer_date']
    today = datetime.now(timezone.utc).date()
    if len(test_date)==1:
        for token in project.tokens:
            dataframe = get_all_transfers(token,today=today)
            if 'token_dataframe' in globals():
                token_dataframe=pd.concat([token_dataframe,dataframe],ignore_index=True)
            else:
                token_dataframe=dataframe
            del dataframe

        for token in project.tokens_NFT:
            dataframe = get_all_transfers(token,NFT=True,today=today)
            if 'nfttoken_dataframe' in globals():
                nfttoken_dataframe=pd.concat([nfttoken_dataframe,dataframe],ignore_index=True)
            else:
                nfttoken_dataframe=dataframe
            del dataframe

        #Cálculo de la fecha mínima
        dates= pd.concat([token_dataframe[['dateTime','blocknumber']], nfttoken_dataframe[['dateTime','blocknumber']]], ignore_index=True)
        dates = pd.DataFrame(dates, columns=['dateTime', 'blocknumber'])
        dates = dates.sort_values('dateTime', ascending=False)
        last_datetime= dates.loc[0]['dateTime'] #Para holders_hist
        last_block_number=dates.loc[0]['blocknumber']
        first_datetime=dates['dateTime'].min()
        difference_dates=(last_datetime-first_datetime).days
        last_date = datetime.now(timezone.utc).date()-timedelta(days=1)
        dates['dateTime']=dates['dateTime'].dt.date
        dates.drop_duplicates(subset=['dateTime'],keep='first',inplace=True)    
        min_date=dates['dateTime'].min()
        delta=timedelta(days=1)
        date=min_date
        query=session.query(Holders_Historical)
        df_holders_hist = pd.read_sql(query.statement, session.connection())
        query=session.query(Assets)
        df_assets = pd.read_sql(query.statement, session.connection())
        query=session.query(Market_Context)
        df_market_context = pd.read_sql(query.statement, session.connection())
        index=0
        #Carga de datos por día
        while date<=last_date:
            if date==min_date:
                df_transfers.loc[0] = [date] + [0] * (len(df_transfers.columns) - 1)
                df_holders_hist.loc[0] = [index] + [date] + [last_datetime]+ [last_block_number] +[0] * (len(df_holders_hist.columns) - 4)
                df_assets.loc[0] = [index] + [date] +[0] * (len(df_assets.columns) - 2)
                df_market_context.loc[0] = [index] + [date] +[0] * (len(df_market_context.columns) - 2)
            else:
                df_transfers.loc[len(df_transfers)] = [date] + [0] * (len(df_transfers.columns) - 1)
                df_holders_hist.loc[len(df_holders_hist)] = [index] + [date] + [last_datetime]+  [last_block_number] +[0] * (len(df_holders_hist.columns) - 4)
                df_assets.loc[len(df_assets)] = [index] + [date] +  [0] * (len(df_assets.columns) - 2)
                df_market_context.loc[len(df_market_context)] = [index] + [date] +  [0] * (len(df_market_context.columns) - 2)
            #print('fase 1 completa')
            for token in project.tokens:
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
                #Rango de ballenas
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
        #COMPLETO TABLA ASSETS. HAY REGISTROS QUE CMC MUCHAS VECES NO ACTUALIZA. ENTONCES QUEDAN EN BLANCO. POR ESO EN EL CÓDIGO DE CARGA DIARIA DEBO CORROBORAR SI HAY ESPACIOS EN BLANCO Y EN CASO QUE LOS HAYA COMPLETAR.
        for token in project.tokens:
            df_assets.drop(f'{token.name}_price', axis=1, inplace=True)
            df_assets.drop(f'{token.name}_volume', axis=1, inplace=True)
            df_price_vol=price_vol(token.blockchain,difference_dates,token.contract_address)
            df_assets = pd.merge(df_assets,df_price_vol, on='upload_date', how='left')
            df_assets.rename(columns={'Price': f'{token.name}_price'}, inplace=True)
            df_assets.rename(columns={'Volume': f'{token.name}_volume'}, inplace=True)
            #mask = df_assets[f'{token.name}_price'].isna()
            idx = df_assets[f'{token.name}_price'].first_valid_index()
            df_assets.loc[idx:, f'{token.name}_price'].fillna(method='ffill', inplace=True)
            #mask = df_assets[f'{token.name}_volume'].isna()
            idx = df_assets[f'{token.name}_volume'].first_valid_index()
            df_assets.loc[idx:, f'{token.name}_volume'].fillna(0, inplace=True)
        for token in project.tokens_NFT:
            if token.price_hist:
                df_assets.drop(f'{token.name}_Price', axis=1, inplace=True)
                df_price=token.price_hist(token.blockchain,difference_dates,token.contract_address)
                df_assets = pd.merge(df_assets,df_price, on='upload_date', how='left')
                df_assets.rename(columns={'Price': f'{token.name}_Price'}, inplace=True)
                #mask = df_assets[f'{token.name}_price'].isna()
                idx = df_assets[f'{token.name}_price'].first_valid_index()
                df_assets.loc[idx:, f'{token.name}_price'].fillna(method='ffill', inplace=True)
            if token.vol_hist:
                df_assets.drop(f'{token.name}_Volume', axis=1, inplace=True)
                df_volume=token.vol_hist(token.blockchain,difference_dates,token.contract_address)
                df_assets = pd.merge(df_assets,df_volume, on='upload_date', how='left')
                df_assets.rename(columns={'Volume': f'{token.name}_Volume'}, inplace=True)
                #mask = df_assets[f'{token.name}_volume'].isna()
                idx = df_assets[f'{token.name}_volume'].first_valid_index()
                df_assets.loc[idx:, f'{token.name}_volume'].fillna(0, inplace=True)
        #TABLA MARKET CONTEXT
        df_btc=price_vol('bitcoin',difference_dates+5)
        df_btc.drop('Volume', axis=1, inplace=True)
        df_eth=price_vol('ethereum',difference_dates+5)
        df_eth.drop('Volume', axis=1, inplace=True)
        df_bnb=price_vol('binancecoin',difference_dates+5)
        df_bnb.drop('Volume', axis=1, inplace=True)
        df_market_context.drop('btc_price', axis=1, inplace=True)
        df_market_context.drop('eth_price', axis=1, inplace=True)
        df_market_context.drop('bnb_price', axis=1, inplace=True)
        df_market_context=pd.merge(df_market_context,df_btc, on='upload_date', how='left')
        df_market_context.rename(columns={'Price': 'btc_price'}, inplace=True)
        df_market_context=pd.merge(df_market_context,df_eth, on='upload_date', how='left')
        df_market_context.rename(columns={'Price': 'eth_price'}, inplace=True)
        df_market_context=pd.merge(df_market_context,df_bnb, on='upload_date', how='left')
        df_market_context.rename(columns={'Price': 'bnb_price'}, inplace=True)
        #CARGO LA DATA A SQL SERVER
        df_holders.to_sql(name='Holders', con=engine, if_exists='replace', index=False)
        df_transfers.to_sql(name='Transfers_Historical', con=engine, if_exists='append', index=False)
        query = text('delete from Transfers_Historical where transfer_date=\'2000-01-01\'')
        session.execute(query)
        session.commit()
        df_holders_hist.to_sql(name='Holders_Historical', con=engine, if_exists='append', index=False)
        df_assets.to_sql(name='Assets', con=engine, if_exists='append', index=False)
        df_market_context.to_sql(name='Market_Context', con=engine, if_exists='append', index=False)
        #ACÁ ME QUEDA COMPLETAR LAS TABLAS PBI. PARA ESO CREO DATAFRAMES CON LOS TIPOS CORRECTOS DE DATOS Y LUEGO LOS ENVIO A BASE DE DATOS
        del token_dataframe
        del nfttoken_dataframe