import pandas as pd
from datetime import timedelta,datetime
import numpy as np
import calendar
def convert_to_int(dataframe,column):
    dataframe[column].fillna(0, inplace=True)        
    dataframe[column] = dataframe[column].astype(int)

def get_dates_between(date1, date2):
    dates = []
    current_date = date1
    while current_date <= date2:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates

def table_pbi_4weeks_3months(project,social_dataframe,market_dataframe,asset_dataframe,date,weekly=True):
    market_dataframe=market_dataframe.drop('id', axis=1)
    asset_dataframe=asset_dataframe.drop('id', axis=1)
    dataframe=social_dataframe
    dataframe['telegram_total_members']=0
    for group in project.telegram:
            telegram_group= f'telegram_{list(group.keys())[0]}_members'
            dataframe['telegram_total_members']+=dataframe[telegram_group]
    dataframe=pd.merge(dataframe,market_dataframe, on='upload_date', how='outer')
    dataframe=pd.merge(dataframe,asset_dataframe, on='upload_date', how='outer')
    dataframe = dataframe.sort_values('upload_date', ascending=True)
    for group in project.telegram:
            convert_to_int(dataframe=dataframe,column=f'telegram_{list(group.keys())[0]}_members')
    convert_to_int(dataframe=dataframe,column='telegram_total_members')
    convert_to_int(dataframe=dataframe,column='twitter_members')
    convert_to_int(dataframe=dataframe,column='discord_members')
    first_date=dataframe.loc[dataframe.index[0], 'upload_date']
    date = datetime.strptime(date, "%Y-%m-%d").date()
    dates=get_dates_between(first_date,date)
    mask=dataframe['upload_date'].isin(dates)
    dataframe=dataframe[mask]
    for token in project.tokens+project.tokens_NFT:
        dataframe=dataframe.drop(f'{token.name}_circulating', axis=1)
        dataframe=dataframe.drop(f'{token.name}_burned', axis=1)
    if weekly:
        #last 4 weeks
        my_dataframe=dataframe.tail(28).copy()
        max_logs=len(my_dataframe)
        new_id= range(1, max_logs+1)
        my_dataframe.loc[:,'id']=new_id
    else:
        #last 3 months
        my_dataframe=dataframe.tail(100).copy()
        max_logs=len(my_dataframe)
        new_id= range(1, max_logs+1)
        my_dataframe.loc[:,'id']=new_id
        my_dataframe['month']=my_dataframe['upload_date'].apply(lambda x: x.month)
        months=my_dataframe.groupby('month', as_index=False).last()
        last_months=months['month'].tail(3).tolist()
        dates_mask_my_dataframe = my_dataframe['month'].isin(last_months)
        my_dataframe=my_dataframe[dates_mask_my_dataframe]
        my_dataframe.drop('month',inplace=True,axis=1)
    return my_dataframe
#dataframe=table_pbi_4weeks_3months(project='NFT11_test',social_dataframe='df_socials',market_dataframe="df_market_context",asset_dataframe="df_assets",date='2023-06-17',day=True)
#dataframe.to_sql(name='PBI_3months_4weeks', con=engine, if_exists='replace', index=False)

def table_pbi_assets_and_whales(project,asset_list,holders_dataframe,market_dataframe,asset_dataframe,transfer_dataframe,date,weekly=True):
    assets=list(asset_list['asset'])
    assets.append('BTC')
    assets.append('ETH')
    assets.append('BNB')
    #Merge all dataframes
    transfer_dataframe['upload_date']=transfer_dataframe['transfer_date']
    asset_dataframe=pd.merge(asset_dataframe,holders_dataframe, on='upload_date', how='outer')
    asset_dataframe=pd.merge(asset_dataframe,transfer_dataframe, on='upload_date', how='outer')
    asset_dataframe=pd.merge(asset_dataframe,market_dataframe, on='upload_date', how='outer')
    #primero filtro por fecha
    first_date=asset_dataframe.loc[asset_dataframe.index[0], 'upload_date']
    date = datetime.strptime(date, "%Y-%m-%d").date()
    dates=get_dates_between(first_date,date)
    mask=asset_dataframe['upload_date'].isin(dates)
    asset_dataframe=asset_dataframe[mask]
    #Trabajo los dataframe
    my_assets_sum=[]
    for token in project.tokens+project.tokens_NFT:
            if len(token.category)>0:
                for category in token.category:
                    my_assets_sum.append(f'{token.name}_{category}_volume')
                my_assets_sum.append(f'{token.name}_bought_total')
                my_assets_sum.append(f'{token.name}_bought_whales')
                my_assets_sum.append(f'{token.name}_sold_total')
                my_assets_sum.append(f'{token.name}_sold_whales')
            else:
                my_assets_sum.append(f'{token.name}_volume')
                my_assets_sum.append(f'{token.name}_bought_total')
                my_assets_sum.append(f'{token.name}_bought_whales')
                my_assets_sum.append(f'{token.name}_sold_total')
                my_assets_sum.append(f'{token.name}_sold_whales')
    
    #Sea semanal o mensual, termino con un dataframe de dos registros (semana/mes previo y actual)
    if weekly:
        asset_dataframe=asset_dataframe.tail(14).copy()
        asset_dataframe = asset_dataframe.reset_index(drop=True)
        asset_dataframe['week'] = np.nan
        asset_dataframe.loc[:6, 'week'] = int(1)
        asset_dataframe.loc[7:, 'week'] = int(2)
        asset_dataframe2=asset_dataframe.groupby('week')[my_assets_sum].sum()
        asset_dataframe=asset_dataframe.groupby('week').last()
        for token in project.tokens+project.tokens_NFT:
            if len(token.category)>0:
                for category in token.category:
                    asset_dataframe[f'{token.name}_{category}_volume']=asset_dataframe2[f'{token.name}_{category}_volume']
            else:
                asset_dataframe[f'{token.name}_volume']=asset_dataframe2[f'{token.name}_volume']
            asset_dataframe[f'{token.name}_bought_total']=asset_dataframe2[f'{token.name}_bought_total']
            asset_dataframe[f'{token.name}_bought_whales']=asset_dataframe2[f'{token.name}_bought_whales']
            asset_dataframe[f'{token.name}_sold_total']=asset_dataframe2[f'{token.name}_sold_total']
            asset_dataframe[f'{token.name}_sold_whales']=asset_dataframe2[f'{token.name}_sold_whales']
    else:
        asset_dataframe=asset_dataframe.tail(70).copy()
        asset_dataframe['month']=asset_dataframe['upload_date'].apply(lambda x: x.month)
        asset_dataframe2=asset_dataframe.groupby('month')[my_assets_sum].sum()
        asset_dataframe=asset_dataframe.groupby('month').last()
        for token in project.tokens+project.tokens_NFT:
            if token.category:
                for category in token.category:
                    asset_dataframe[f'{token.name}_{category}_volume']=asset_dataframe2[f'{token.name}_{category}_volume']
            else:
                asset_dataframe[f'{token.name}_volume']=asset_dataframe2[f'{token.name}_volume']
        asset_dataframe[f'{token.name}_bought_total']=asset_dataframe2[f'{token.name}_bought_total']
        asset_dataframe[f'{token.name}_bought_whales']=asset_dataframe2[f'{token.name}_bought_whales']
        asset_dataframe[f'{token.name}_sold_total']=asset_dataframe2[f'{token.name}_sold_total']
        asset_dataframe[f'{token.name}_sold_whales']=asset_dataframe2[f'{token.name}_sold_whales']
        asset_dataframe=asset_dataframe.tail(2).copy() 
    #SOLO PRECIO Y VOLUMEN HACE DIFERENCIA POR CATEGORIA, LO DEMÁS CUENTO POR TOKEN.    
    #CON ASSET_DATAFRAME YA TENGO TODAS LAS COLUMNAS PARA CALCUlar
    #CÁLCULO DE PRECIO, VOLUMEN Y VARIACION DE AMBAS PARA ASSETS Y SUS CATEGORIAS.
    price_list=[]
    last_price_list=[]
    volume_list=[]
    last_volume_list=[]
    holder_list=[]
    last_holder_list=[]
    whales_list=[]
    last_whales_list=[]
    whales_balance_list=[]
    last_whales_balance_list=[]
    circulating_list=[]
    bought_list=[]
    bought_whales_list=[]
    sold_list=[]
    sold_whales_list=[]
    #Para dataframe whales
    operation=['bought','sold']
    columnas_whales={'operation':operation}
    for token in project.tokens+project.tokens_NFT:
        if token.category:
            price_max=0
            last_price_max=0
            vol_acum=0
            last_vol_acum=0
            for category in token.category:
                price=asset_dataframe.iloc[-1][f'{token.name}_{category}_price']
                last_price=asset_dataframe.iloc[0][f'{token.name}_{category}_price']
                volume=asset_dataframe.iloc[-1][f'{token.name}_{category}_volume']
                last_volume=asset_dataframe.iloc[0][f'{token.name}_{category}_volume']
                price_list.append(price)
                last_price_list.append(last_price)
                volume_list.append(volume)
                last_volume_list.append(last_volume)
                holder_list.append(None)
                last_holder_list.append(None)
                whales_list.append(None)
                last_whales_list.append(None)
                whales_balance_list.append(None)
                last_whales_balance_list.append(None)
                circulating_list.append(None)
                bought_list.append(None)
                bought_whales_list.append(None)
                sold_list.append(None)
                sold_whales_list.append(None)
                if price>price_max:
                    price_max=price
                if last_price>last_price_max:
                    last_price_max=last_price
                vol_acum+=volume
                last_vol_acum+=last_volume
            price_list.append(price_max)
            last_price_list.append(last_price_max)
            volume_list.append(vol_acum)
            last_volume_list.append(last_vol_acum)
            holders=asset_dataframe.iloc[-1][f'{token.name}_holders']
            last_holders=asset_dataframe.iloc[0][f'{token.name}_holders']
            holder_list.append(holders)
            last_holder_list.append(last_holders)
            whales=asset_dataframe.iloc[-1][f'{token.name}_whales']
            last_whales=asset_dataframe.iloc[0][f'{token.name}_whales']
            whales_list.append(whales)
            last_whales_list.append(last_whales)
            whales_balance=asset_dataframe.iloc[-1][f'{token.name}_whales_balance']
            last_whales_balance=asset_dataframe.iloc[0][f'{token.name}_whales_balance']
            whales_balance_list.append(whales_balance)
            last_whales_balance_list.append(last_whales_balance)
            circulating=asset_dataframe.iloc[-1][f'{token.name}_circulating']
            circulating_list.append(circulating)
            bought=asset_dataframe.iloc[-1][f'{token.name}_bought_total']
            bought_list.append(bought)
            bought_whales=asset_dataframe.iloc[-1][f'{token.name}_bought_whales']
            bought_whales_list.append(bought_whales)
            sold=asset_dataframe.iloc[-1][f'{token.name}_sold_total']
            sold_list.append(sold)
            sold_whales=asset_dataframe.iloc[-1][f'{token.name}_sold_whales']
            sold_whales_list.append(sold_whales)
            pbi_whales=[bought_whales,sold_whales]
            pbi_total=[bought_whales,sold_whales]
            columnas_whales[f'{token.name}_whale']=pbi_whales
            columnas_whales[f'{token.name}_total']=pbi_total
        else:
            price=asset_dataframe.iloc[-1][f'{token.name}_price']
            last_price=asset_dataframe.iloc[0][f'{token.name}_price']
            volume=asset_dataframe.iloc[-1][f'{token.name}_volume']
            last_volume=asset_dataframe.iloc[0][f'{token.name}_volume']
            price_list.append(price)
            last_price_list.append(last_price)
            volume_list.append(volume)
            last_volume_list.append(last_volume)
            holders=asset_dataframe.iloc[-1][f'{token.name}_holders']
            last_holders=asset_dataframe.iloc[0][f'{token.name}_holders']
            holder_list.append(holders)
            last_holder_list.append(last_holders)
            whales=asset_dataframe.iloc[-1][f'{token.name}_whales']
            last_whales=asset_dataframe.iloc[0][f'{token.name}_whales']
            whales_list.append(whales)
            last_whales_list.append(last_whales)
            whales_balance=asset_dataframe.iloc[-1][f'{token.name}_whales_balance']
            last_whales_balance=asset_dataframe.iloc[0][f'{token.name}_whales_balance']
            whales_balance_list.append(whales_balance)
            last_whales_balance_list.append(last_whales_balance)
            circulating=asset_dataframe.iloc[-1][f'{token.name}_circulating']
            circulating_list.append(circulating)
            bought=asset_dataframe.iloc[-1][f'{token.name}_bought_total']
            bought_list.append(bought)
            bought_whales=asset_dataframe.iloc[-1][f'{token.name}_bought_whales']
            bought_whales_list.append(bought_whales)
            sold=asset_dataframe.iloc[-1][f'{token.name}_sold_total']
            sold_list.append(sold)
            sold_whales=asset_dataframe.iloc[-1][f'{token.name}_sold_whales']
            sold_whales_list.append(sold_whales)
            pbi_whales=[bought_whales,sold_whales]
            pbi_total=[bought,sold]
            columnas_whales[f'{token.name}_whale']=pbi_whales
            columnas_whales[f'{token.name}_total']=pbi_total
    price_list.append(asset_dataframe.iloc[-1]['btc_price']) 
    last_price_list.append(asset_dataframe.iloc[0]['btc_price'])
    price_list.append(asset_dataframe.iloc[-1]['eth_price']) 
    last_price_list.append(asset_dataframe.iloc[0]['eth_price'])
    price_list.append(asset_dataframe.iloc[-1]['bnb_price']) 
    last_price_list.append(asset_dataframe.iloc[0]['bnb_price'])
    for element in range (0,3):
        holder_list.append(None)
        volume_list.append(None)
        last_holder_list.append(None)
        last_volume_list.append(None)
        whales_list.append(None)
        last_whales_list.append(None)
        whales_balance_list.append(None)
        last_whales_balance_list.append(None)
        circulating_list.append(None)
        bought_list.append(None)
        bought_whales_list.append(None)
        sold_list.append(None)
        sold_whales_list.append(None)
    price_change_list=[]
    volume_change_list=[]
    holders_change_list=[]
    whales_change_list=[]
    whales_balance_change_list=[]
    whales_relative_holding=[]
    for price,price_last in zip (price_list,last_price_list):
        if price_last!=0 and isinstance(price_last, (int, float)):
            variation=(price-price_last)/price_last
        else:
            variation=None
        price_change_list.append(variation)
    for volume,volume_last in zip (volume_list,last_volume_list):
        if volume_last!=0 and isinstance(volume_last, (int, float)):
            variation=(volume-volume_last)/volume_last
        else:
            variation=None
        volume_change_list.append(variation)
    for holders,holders_last in zip (holder_list,last_holder_list):
        if holders_last!=0 and isinstance(holders_last, (int, float)):
            variation=(holders-holders_last)/holders_last
        else:
            variation=None
        holders_change_list.append(variation)
    for whales,whales_last in zip (whales_list,last_whales_list):
        if whales_last!=0 and isinstance(whales_last, (int, float)):
            variation=(whales-whales_last)/whales_last
        else:
            variation=None
        whales_change_list.append(variation)
    for whales_balance,whales_balance_last in zip (whales_balance_list,last_whales_balance_list):
        if whales_balance_last!=0 and isinstance(whales_balance_last, (int, float)):
            variation=(whales_balance-whales_balance_last)/whales_balance_last
        else:
            variation=None
        whales_balance_change_list.append(variation)
    for whales_balance,circulating in zip (whales_balance_list,circulating_list):
        if circulating!=0 and isinstance(circulating, (int, float)):
            variation=whales_balance/circulating
        else:
            variation=None
        whales_relative_holding.append(variation)
    columnas={'assets':assets,'price':price_list,'price_change':price_change_list,'volume':volume_list,'volume_change':volume_change_list,
              'holders':holder_list,'holders_change':holders_change_list,'whales':whales_list,'whales_change':whales_change_list,
              'whales_holding_variation_por':whales_balance_change_list,'whales_holding_por_of_total':whales_relative_holding,
              'bought_total':bought_list,'bought_whales':bought_whales_list,'sold_total':sold_list,'sold_whales':sold_whales_list}
    dataframe=pd.DataFrame(columnas)
    dataframe2=pd.DataFrame(columnas_whales)
    return [dataframe,dataframe2]

def table_pbi_socials(project,social_dataframe,date,weekly=True):
    social_network=['Community Index']
    for group in project.telegram:
        social_network.append(f'Telegram {list(group.keys())[0]}')
    social_network.append('Telegram Total')
    social_network.append('Discord')
    social_network.append('Twitter')
    id=[i for i in range(0,len(social_network))]
    #primero filtro por fecha
    first_date=social_dataframe.loc[social_dataframe.index[0], 'upload_date']
    date = datetime.strptime(date, "%Y-%m-%d").date()
    dates=get_dates_between(first_date,date)
    mask=social_dataframe['upload_date'].isin(dates)
    social_dataframe=social_dataframe[mask]
    #Sea semanal o mensual, termino con un dataframe de dos registros (semana/mes previo y actual)
    if weekly:
        social_dataframe=social_dataframe.tail(14).copy()
        social_dataframe = social_dataframe.reset_index(drop=True)
        social_dataframe['week'] = np.nan
        social_dataframe.loc[:6, 'week'] = int(1)
        social_dataframe.loc[7:, 'week'] = int(2)
        social_dataframe=social_dataframe.groupby('week').last()
    else:
        social_dataframe=social_dataframe.tail(70).copy()
        social_dataframe['month']=social_dataframe['upload_date'].apply(lambda x: x.month)
        social_dataframe=social_dataframe.groupby('month').last()
        social_dataframe=social_dataframe.tail(2).copy()
    this_cycle=[]
    last_cycle=[]
    telegram_total_this_cycle=0
    telegram_total_last_cycle=0
    for group in project.telegram:
        telegram_tc=social_dataframe.iloc[-1][f'telegram_{list(group.keys())[0]}_members']
        this_cycle.append(telegram_tc)
        telegram_total_this_cycle+=telegram_tc
        telegram_lc=social_dataframe.iloc[0][f'telegram_{list(group.keys())[0]}_members']
        last_cycle.append(telegram_lc)
        telegram_total_last_cycle+=telegram_lc
    this_cycle.append(telegram_total_this_cycle)
    last_cycle.append(telegram_total_last_cycle)
    discord_tc=social_dataframe.iloc[-1]['discord_members']
    this_cycle.append(discord_tc)
    discord_lc=social_dataframe.iloc[0]['discord_members']
    last_cycle.append(discord_lc)
    twitter_tc=social_dataframe.iloc[-1]['twitter_members']
    this_cycle.append(twitter_tc)
    twitter_lc=social_dataframe.iloc[0]['twitter_members']
    last_cycle.append(twitter_lc)
    this_cycle.insert(0,telegram_total_this_cycle+discord_tc+twitter_tc)
    last_cycle.insert(0,telegram_total_last_cycle+discord_lc+twitter_lc)
    change=[]
    for this,last in zip(this_cycle,last_cycle):
        try:
            variation=(this-last)/last
        except:
            variation=None
        change.append(variation)
    columns={'id':id,'social_network':social_network,'this_cycle':this_cycle,'last_cycle':last_cycle,'change':change}
    dataframe=pd.DataFrame(columns)
    return dataframe

def table_pbi_monthly(project,market_dataframe,asset_dataframe,transfer_dataframe,date):
    #Merge all dataframes
    transfer_dataframe['upload_date']=transfer_dataframe['transfer_date']
    asset_dataframe=pd.merge(asset_dataframe,transfer_dataframe, on='upload_date', how='outer')
    asset_dataframe=pd.merge(asset_dataframe,market_dataframe, on='upload_date', how='outer')
    #primero filtro por fecha
    first_date=asset_dataframe.loc[asset_dataframe.index[0], 'upload_date']
    date = datetime.strptime(date, "%Y-%m-%d").date()
    dates=get_dates_between(first_date,date)
    mask=asset_dataframe['upload_date'].isin(dates)
    asset_dataframe=asset_dataframe[mask]
    #Trabajo los dataframe
    my_assets_sum=[]
    for token in project.tokens:
            my_assets_sum.append(f'{token.name}_volume')
            my_assets_sum.append(f'{token.name}_staking')
    for token in project.tokens_NFT:
            if len(token.category)>0:
                for category in token.category:
                    my_assets_sum.append(f'{token.name}_{category}_volume')
            else:
                my_assets_sum.append(f'{token.name}_volume')
    #Sea semanal o mensual, termino con un dataframe de dos registros (semana/mes previo y actual)
    asset_dataframe=asset_dataframe.tail(370).copy()
    asset_dataframe['year']=asset_dataframe['upload_date'].apply(lambda x: x.year)
    asset_dataframe['month']=asset_dataframe['upload_date'].apply(lambda x: x.month)
    current_year=asset_dataframe.iloc[-1]['year']
    current_month=asset_dataframe.iloc[-1]['month']
    mask_year=asset_dataframe['year']==current_year
    mask_month=asset_dataframe['month']>current_month
    asset_dataframe=asset_dataframe[mask_year | mask_month]
    asset_dataframe['month'] = asset_dataframe['month'].apply(lambda x: calendar.month_name[x])
    asset_dataframe2=asset_dataframe.groupby('month')[my_assets_sum].sum()
    asset_dataframe=asset_dataframe.groupby('month').last()
    for token in project.tokens:
        asset_dataframe[f'{token.name}_volume']=asset_dataframe2[f'{token.name}_volume']
        asset_dataframe[f'{token.name}_staking']=asset_dataframe2[f'{token.name}_staking']
        asset_dataframe.drop(f'{token.name}_circulating',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_burned_x',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_burned_y',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_bought_total',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_bought_whales',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_sold_total',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_sold_whales',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_staking_whales',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_minted_total',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_minted_private',axis=1, inplace=True)
    for token in project.tokens_NFT:
        if token.category:
            for category in token.category:
                asset_dataframe[f'{token.name}_{category}_volume']=asset_dataframe2[f'{token.name}_{category}_volume']
        else:
            asset_dataframe[f'{token.name}_volume']=asset_dataframe2[f'{token.name}_volume']
        asset_dataframe.drop(f'{token.name}_circulating',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_burned_x',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_burned_y',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_bought_total',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_bought_whales',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_sold_total',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_sold_whales',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_staking_whales',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_minted_total',axis=1, inplace=True)
        asset_dataframe.drop(f'{token.name}_minted_private',axis=1, inplace=True)
    asset_dataframe.drop('id_x',axis=1, inplace=True)
    asset_dataframe.drop('id_y',axis=1, inplace=True)
    asset_dataframe=asset_dataframe.sort_values('upload_date')
    return asset_dataframe


#PARA AGRUPAR POR MESES
'''
else:
        my_dataframe=dataframe.tail(100).copy()
        max_logs=len(my_dataframe)
        new_id= range(1, max_logs+1)
        my_dataframe.loc[:,'id']=new_id
        my_dataframe['month']=my_dataframe['upload_date'].apply(lambda x: x.month)
        my_list=[]
        for token in project.tokens:
             my_list.append(f'{token.name}_volume')
        for token in project.tokens_NFT:
            if len(token.category)>0:
                for category in token.category:
                    my_list.append(f'{token.name}_{category}_volume')
            else:
                 my_list.append(f'{token.name}_volume')
        sum_dataframe=my_dataframe.groupby('month')[my_list].sum()
        my_dataframe=my_dataframe.groupby('month', as_index=False).last()
        for token in project.tokens:
             my_dataframe[f'{token.name}_volume']=sum_dataframe[f'{token.name}_volume']
        for token in project.tokens_NFT:
            if len(token.category)>0:
                for category in token.category:
                    my_dataframe[f'{token.name}_{category}_volume']=sum_dataframe[f'{token.name}_{category}_volume']
            else:
                 my_dataframe[f'{token.name}_volume']=sum_dataframe[f'{token.name}_volume']'''