from projects import projects
from sqlalchemy import create_engine,Column,String,Integer,Numeric,Date,ForeignKey,DateTime
from sqlalchemy.orm import sessionmaker,declarative_base,relationship
from credentials import server,usuario,contraseña,driver
import pyodbc

for project in projects:
    database=project.name
    cnxn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE=Master;UID={usuario};PWD={contraseña}') #Conexión a Servidor de SQLServer
    cnxn.autocommit = True #Cualquier cambio que se hace en la base de datos se confirma automaticamente (commit)
    #consulto si existe la base de datos
    result = cnxn.execute(f"select name from master.dbo.sysdatabases where name = '{database}'")
    if result.fetchone():
        print(f'La base de datos {database} existe en el servidor')
        create_tables=False
        cnxn.close()
    else:
        #si no existe la base de datos, la creo
        cnxn.execute(f'create database {database}')
        create_tables=True
        cnxn.close()
    #Creación de tablas
    engine = create_engine(f'mssql+pyodbc://{usuario}:{contraseña}@{server}/{database}?driver={driver}')
    Session=sessionmaker(bind=engine)
    session=Session()
    Base = declarative_base()
    # Obtener la lista de tokens del proyecto   
    tokens = project.tokens
    tokens_nft=project.tokens_NFT    
    #TABLA HOLDERS
    class Holders(Base):
        __tablename__ = 'Holders'
        address = Column(String(length=255), primary_key=True)
        address_owner = Column(String(length=255))
        for token in tokens:
            holders_holding = f'{token.name}_Holding'
            holders_staking = f'{token.name}_Staking'
            holders_role = f'{token.name}_Role'
            locals()[holders_holding] = Column(Numeric(precision=18, scale=6))
            locals()[holders_staking] = Column(Numeric(precision=18, scale=6))
            locals()[holders_role] = Column(String(length=255))
        for token in tokens_nft:
            holders_holding = f'{token.name}_Holding'
            holders_staking = f'{token.name}_Staking'
            holders_role = f'{token.name}_Role'
            locals()[holders_holding] = Column(Integer)
            locals()[holders_staking] = Column(Integer)
            locals()[holders_role] = Column(String(length=255))
        def __init__(self, address, address_owner=None, **kwargs):
            self.address = address
            self.address_owner = address_owner
            #Explicación: la clase tiene un número variable de atributos (en cantidad y nombre), entonces al definir __init__ no puedo pasarle
            #atributo= None (u otro valor) para que se defina por defecto. por eso uso setattr y kwargs. Esta función de python me permite definir el atributo
            #al pasar **kwargs, lo que estoy diciendo es que va a tomar como parámetro lo que pase dentor de la función cuando haga Holders(....)
            #si no paso uno de los parámetros, no va a estar en kwargs. Si no está, en setattr estoy definiendo que por defecto sea 0. Si está, me pasa el valor.
            #al poner kwargs dentro de la funcion __init__, significa que a Holders() le voy a poder pasar cualquier cosa, ej Holders(cualquiera='un valor').
            #Sin embargo, como "cualquiera" no es un atributo que se esté definiendo, la función no va a hacer nada con ese valor.
            for token in tokens+tokens_nft:
                holders_holding = f'{token.name}_Holding'
                holders_staking = f'{token.name}_Staking'
                holders_role = f'{token.name}_Role'
                setattr(self, holders_holding, kwargs.get(holders_holding, 0))
                setattr(self, holders_staking, kwargs.get(holders_staking, 0))
                setattr(self, holders_role, kwargs.get(holders_role, '-'))
    #TABLA TRANSFER HISTORICAL
    class Transfers_Historical(Base):
        __tablename__ = 'Transfers_Historical'
        transfer_date=Column(Date,primary_key=True)
        for token in tokens:
            transfersh_bought_total = f'{token.name}_bought_total'
            transfersh_bought_whales = f'{token.name}_bought_whales'
            transfersh_sold_total = f'{token.name}_sold_total'
            transfersh_sold_whales = f'{token.name}_sold_whales'
            transfersh_staking = f'{token.name}_staking'
            transfersh_staking_whales = f'{token.name}_staking_whales'
            transfersh_minted_total = f'{token.name}_minted_total'
            transfersh_burned = f'{token.name}_burned'
            transfersh_minted_private = f'{token.name}_minted_private'
            locals()[transfersh_bought_total] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_bought_whales] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_sold_total] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_sold_whales] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_staking] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_staking_whales] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_minted_total] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_burned] = Column(Numeric(precision=18, scale=6))
            locals()[transfersh_minted_private] = Column(Numeric(precision=18, scale=6))
        for token in tokens_nft:
            transfersh_bought_total = f'{token.name}_bought_total'
            transfersh_bought_whales = f'{token.name}_bought_whales'
            transfersh_sold_total = f'{token.name}_sold_total'
            transfersh_sold_whales = f'{token.name}_sold_whales'
            transfersh_staking = f'{token.name}_staking'
            transfersh_staking_whales = f'{token.name}_staking_whales'
            transfersh_minted_total = f'{token.name}_minted_total'
            transfersh_burned = f'{token.name}_burned'
            transfersh_minted_private = f'{token.name}_minted_private'
            locals()[transfersh_bought_total] = Column(Integer)
            locals()[transfersh_bought_whales] = Column(Integer)
            locals()[transfersh_sold_total] = Column(Integer)
            locals()[transfersh_sold_whales] = Column(Integer)
            locals()[transfersh_staking] = Column(Integer)
            locals()[transfersh_staking_whales] = Column(Integer)
            locals()[transfersh_minted_total] = Column(Integer)
            locals()[transfersh_burned] = Column(Integer)
            locals()[transfersh_minted_private] = Column(Integer)
        def __init__(self, transfer_date, **kwargs):
            self.transfer_date = transfer_date
            for token in tokens+tokens_nft:
                transfersh_bought_total = f'{token.name}_bought_total'
                transfersh_bought_whales = f'{token.name}_bought_whales'
                transfersh_sold_total = f'{token.name}_sold_total'
                transfersh_sold_whales = f'{token.name}_sold_whales'
                transfersh_staking = f'{token.name}_staking'
                transfersh_staking_whales = f'{token.name}_staking_whales'
                transfersh_minted_total = f'{token.name}_minted_total'
                transfersh_burned = f'{token.name}_burned'
                transfersh_minted_private = f'{token.name}_minted_private'
                setattr(self, transfersh_bought_total, kwargs.get(transfersh_bought_total,0))
                setattr(self, transfersh_bought_whales, kwargs.get(transfersh_bought_whales,0))
                setattr(self, transfersh_sold_total, kwargs.get(transfersh_sold_total,0))
                setattr(self, transfersh_sold_whales, kwargs.get(transfersh_sold_whales,0))
                setattr(self, transfersh_staking, kwargs.get(transfersh_staking,0))
                setattr(self, transfersh_staking_whales, kwargs.get(transfersh_staking_whales,0))
                setattr(self, transfersh_minted_total, kwargs.get(transfersh_minted_total,0))
                setattr(self, transfersh_burned, kwargs.get(transfersh_burned,0))
                setattr(self, transfersh_minted_private, kwargs.get(transfersh_minted_private,0))
    #TABLA HOLDERS HISTORICAL
    class Holders_Historical(Base):
        __tablename__ = 'Holders_Historical'
        id = Column(Integer, primary_key=True, autoincrement=False)
        upload_date=Column(Date,ForeignKey('Transfers_Historical.transfer_date', ondelete='CASCADE'))
        last_transfer=Column(DateTime)
        last_block_number=Column(Integer)
        for token in tokens:
            holdersh_whales= f'{token.name}_whales'
            holdersh_whales_holding = f'{token.name}_whales_balance'
            holdersh_whales_staking = f'{token.name}_whales_staking'
            locals()[holdersh_whales] = Column(Integer)
            locals()[holdersh_whales_holding] = Column(Numeric(precision=18, scale=6))
            locals()[holdersh_whales_staking] = Column(Numeric(precision=18, scale=6))
        for token in tokens_nft:
            holdersh_whales= f'{token.name}_whales'
            holdersh_whales_holding = f'{token.name}_whales_balance'
            holdersh_whales_staking = f'{token.name}_whales_staking'
            locals()[holdersh_whales] = Column(Integer)
            locals()[holdersh_whales_holding] = Column(Integer)
            locals()[holdersh_whales_staking] = Column(Integer)
        def __init__(self,id, upload_date,last_transfer,last_block_number, **kwargs):
            self.id=id
            self.upload_date = upload_date
            self.last_transfer=last_transfer
            self.last_block_number=last_block_number
            for token in tokens+tokens_nft:
                holdersh_whales = f'{token.name}_whales'
                holdersh_whales_holding = f'{token.name}_whales_balance'
                holdersh_whales_staking = f'{token.name}_whales_staking'
                setattr(self, holdersh_whales, kwargs.get(holdersh_whales,0))
                setattr(self, holdersh_whales_holding, kwargs.get(holdersh_whales_holding,0))
                setattr(self, holdersh_whales_staking, kwargs.get(holdersh_whales_staking,0))
    #TABLE ASSETS
    class Assets(Base):
        __tablename__ = 'Assets'
        id = Column(Integer, primary_key=True, autoincrement=False)
        upload_date=Column(Date,ForeignKey('Transfers_Historical.transfer_date', ondelete='CASCADE'))
        for token in tokens:
            assets_price= f'{token.name}_price'
            assets_volume = f'{token.name}_volume'
            assets_holders = f'{token.name}_holders'
            assets_circulating = f'{token.name}_circulating'
            assets_burned = f'{token.name}_burned'
            locals()[assets_price] = Column(Numeric(precision=18, scale=6))
            locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            locals()[assets_holders] = Column(Integer)
            locals()[assets_circulating] = Column(Numeric(precision=18, scale=6))
            locals()[assets_burned] = Column(Numeric(precision=18, scale=6))
        for token in tokens_nft:
            if len(token.category)>0:
                for category in token.category:
                    assets_price= f'{token.name}_{category}_price'
                    assets_volume = f'{token.name}_{category}_volume'
                    locals()[assets_price] = Column(Numeric(precision=18, scale=6))
                    locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            else:
                assets_price= f'{token.name}_price'
                assets_volume = f'{token.name}_volume'
                locals()[assets_price] = Column(Numeric(precision=18, scale=6))
                locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            assets_holders = f'{token.name}_holders'
            assets_circulating = f'{token.name}_circulating'
            assets_burned = f'{token.name}_burned'
            locals()[assets_holders] = Column(Integer)
            locals()[assets_circulating] = Column(Integer)
            locals()[assets_burned] = Column(Integer)
        def __init__(self,id,upload_date, **kwargs):
            self.id=id
            self.upload_date = upload_date
            for token in tokens:
                assets_price= f'{token.name}_price'
                assets_volume = f'{token.name}_volume'
                assets_holders = f'{token.name}_holders'
                assets_circulating = f'{token.name}_circulating'
                assets_burned = f'{token.name}_burned'
                setattr(self, assets_price, kwargs.get(assets_price,0))
                setattr(self, assets_volume, kwargs.get(assets_volume,0))
                setattr(self, assets_holders, kwargs.get(assets_holders,0))
                setattr(self, assets_circulating, kwargs.get(assets_circulating,0))
                setattr(self, assets_burned, kwargs.get(assets_burned,0))
            for token in tokens_nft:
                if len(token.category)>0:
                    for category in token.category:
                        assets_price= f'{token.name}_{category}_price'
                        assets_volume = f'{token.name}_{category}_volume'
                        setattr(self, assets_price, kwargs.get(assets_price,0))
                        setattr(self, assets_volume, kwargs.get(assets_volume,0))
                else:
                    assets_price= f'{token.name}_price'
                    assets_volume = f'{token.name}_volume'
                    setattr(self, assets_price, kwargs.get(assets_price,0))
                    setattr(self, assets_volume, kwargs.get(assets_volume,0))
                assets_holders = f'{token.name}_holders'
                assets_circulating = f'{token.name}_circulating'
                assets_burned = f'{token.name}_burned'
                setattr(self, assets_holders, kwargs.get(assets_holders,0))
                setattr(self, assets_circulating, kwargs.get(assets_circulating,0))
                setattr(self, assets_burned, kwargs.get(assets_burned,0))
    #TABLE MARKET CONTEXT
    class Market_Context(Base):
        __tablename__ = 'Market_Context'
        id = Column(Integer, primary_key=True, autoincrement=False)
        upload_date=Column(Date,ForeignKey('Transfers_Historical.transfer_date', ondelete='CASCADE'))
        btc_price=Column(Numeric(precision=18, scale=6))
        eth_price=Column(Numeric(precision=18, scale=6))
        bnb_price=Column(Numeric(precision=18, scale=6))
        marketcap_niche=Column(Numeric(precision=18, scale=6))
        def __init__(self,id, upload_date,btc_price=0,eth_price=0,bnb_price=0,marketcap_niche=0):
            self.id=id
            self.upload_date = upload_date
            self.btc_price=btc_price
            self.eth_price=eth_price
            self.bnb_price=bnb_price
            self.marketcap_niche=marketcap_niche
    #TABLE SOCIALS
    class Socials(Base):
        __tablename__ = 'Socials'
        id = Column(Integer, primary_key=True, autoincrement=False)
        upload_date=Column(Date,ForeignKey('Transfers_Historical.transfer_date', ondelete='CASCADE'))
        twitter_members=Column(Integer)
        discord_members=Column(Integer)
        for group in project.telegram:
            telegram_group= f'telegram_{list(group.keys())[0]}_members'
            locals()[telegram_group] = Column(Integer)
        def __init__(self,id,upload_date,twitter_members,discord_members,**kwargs):
            self.id=id
            self.upload_date=upload_date
            self.twitter_members=twitter_members
            self.discord_members=discord_members
            for group in project.telegram:
                telegram_group= f'telegram_{list(group.keys())[0]}_members'
                if telegram_group in kwargs:
                    setattr(self, telegram_group, kwargs[telegram_group])
    #Activos
    class Main_assets(Base):
        __tablename__= 'Main_Assets'
        id=Column(Integer, primary_key=True)
        asset=Column(String)
    
    #TABLA PBI 4 semanas
    class PBI_3months_4weeks(Base):
        __tablename__ = 'PBI_3months_4weeks'
        id = Column(Integer, primary_key=True, autoincrement=False)
        upload_date=Column(Date)
        twitter_followers=Column(Integer)
        discord_members=Column(Integer)
        for group in project.telegram:
            telegram_group= f'telegram_{list(group.keys())[0]}_members'
            locals()[telegram_group] = Column(Integer)
        telegram_total_members=Column(Integer)
        btc_price=Column(Numeric(precision=18, scale=6))
        eth_price=Column(Numeric(precision=18, scale=6))
        bnb_price=Column(Numeric(precision=18, scale=6))
        marketcap_niche=Column(Numeric(precision=18, scale=6))
        for token in tokens:
            assets_price= f'{token.name}_price'
            assets_volume = f'{token.name}_volume'
            assets_holders = f'{token.name}_holders'
            locals()[assets_price] = Column(Numeric(precision=18, scale=6))
            locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            locals()[assets_holders] = Column(Integer)
        for token in tokens_nft:
            if len(token.category)>0:
                for category in token.category:
                    assets_price= f'{token.name}_{category}_price'
                    assets_volume = f'{token.name}_{category}_volume'
                    locals()[assets_price] = Column(Numeric(precision=18, scale=6))
                    locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            else:
                assets_price= f'{token.name}_price'
                assets_volume = f'{token.name}_volume'
                locals()[assets_price] = Column(Numeric(precision=18, scale=6))
                locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            assets_holders = f'{token.name}_holders'
            locals()[assets_holders] = Column(Integer)
        def __init__(self,id,upload_date,twitter_followers,discord_members,telegram_total_members,btc_price,eth_price,bnb_price,marketcap_niche, **kwargs):
            self.id=id
            self.upload_date = upload_date
            self.twitter_followers=twitter_followers
            self.discord_members=discord_members
            for group in project.telegram:
                telegram_group= f'telegram_{list(group.keys())[0]}_members'
                if telegram_group in kwargs:
                    setattr(self, telegram_group, kwargs[telegram_group])
            self.telegram_total_members=telegram_total_members        
            self.btc_price=btc_price
            self.eth_price=eth_price
            self.bnb_price=bnb_price
            self.marketcap_niche=marketcap_niche
            for token in tokens:
                assets_price= f'{token.name}_price'
                assets_volume = f'{token.name}_volume'
                assets_holders = f'{token.name}_holders'
                setattr(self, assets_price, kwargs.get(assets_price,0))
                setattr(self, assets_volume, kwargs.get(assets_volume,0))
                setattr(self, assets_holders, kwargs.get(assets_holders,0))
            for token in tokens_nft:
                if len(token.category)>0:
                    for category in token.category:
                        assets_price= f'{token.name}_{category}_price'
                        assets_volume = f'{token.name}_{category}_volume'
                        setattr(self, assets_price, kwargs.get(assets_price,0))
                        setattr(self, assets_volume, kwargs.get(assets_volume,0))
                else:
                    assets_price= f'{token.name}_price'
                    assets_volume = f'{token.name}_volume'
                    setattr(self, assets_price, kwargs.get(assets_price,0))
                    setattr(self, assets_volume, kwargs.get(assets_volume,0))
                assets_holders = f'{token.name}_holders'
                setattr(self, assets_holders, kwargs.get(assets_holders,0))
    #TABLA PBI Assets
    class PBI_Assets(Base):
        __tablename__ = 'PBI_Assets'
        assets = Column(String(length=255), primary_key=True)
        price=Column(Numeric(precision=18, scale=6))
        price_change=Column(Numeric(precision=18, scale=6))
        holders=Column(Integer)
        holders_change=Column(Numeric(precision=18, scale=6))
        volume=Column(Numeric(precision=18, scale=6))
        volume_change=Column(Numeric(precision=18, scale=6))
        whales=Column(Integer)
        whales_change=Column(Numeric(precision=18, scale=6))
        whales_holding_variation_por=Column(Numeric(precision=18, scale=6))
        whales_holding_por_of_total=Column(Numeric(precision=18, scale=6))
        bought_total=Column(Numeric(precision=18, scale=6))
        bought_whales=Column(Numeric(precision=18, scale=6))
        sold_total=Column(Numeric(precision=18, scale=6))
        sold_whales=Column(Numeric(precision=18, scale=6))
        def __init__(self,assets,price,price_change,holders,holders_change,volume,volume_change,whales,whales_change,whales_holding_variation_por,whales_holding_por_of_total,bought_total,bought_whales,sold_total,sold_whales):
            self.assets = assets
            self.price=price
            self.price_change=price_change
            self.holders=holders
            self.holders_change=holders_change
            self.volume=volume
            self.volume_change=volume_change
            self.whales=whales
            self.whales_change=whales_change
            self.whales_holding_variation_por=whales_holding_variation_por
            self.whales_holding_por_of_total=whales_holding_por_of_total
            self.bought_total=bought_total
            self.bought_whales=bought_whales
            self.sold_total=sold_total
            self.sold_whales=sold_whales
    #TABLA PBI Whales
    class PBI_Whales(Base):
        __tablename__ = 'PBI_Whales'
        operation = Column(String(length=255), primary_key=True)
        for token in tokens:
            assets_whale= f'{token.name}_whale'
            assets_total = f'{token.name}_total'
            locals()[assets_whale] = Column(Numeric(precision=18, scale=6))
            locals()[assets_total] = Column(Numeric(precision=18, scale=6))
        for token in tokens_nft:
            assets_whale= f'{token.name}_whale'
            assets_total = f'{token.name}_total'
            locals()[assets_whale] = Column(Integer)
            locals()[assets_total] = Column(Integer)
        def __init__(self,operation,**kwargs):
            self.operation=operation
            for token in tokens+tokens_nft:
                assets_whale= f'{token.name}_whale'
                assets_total = f'{token.name}_total'
                setattr(self, assets_whale, kwargs.get(assets_whale,0))
                setattr(self, assets_total, kwargs.get(assets_total,0))
    #TABLA PBI Socials
    class PBI_Socials(Base):
        __tablename__ = 'PBI_Socials'
        id=Column(Integer,primary_key=True,autoincrement=False)
        social_network = Column(String(length=255))
        this_cycle=Column(Integer)
        last_cycle=Column(Integer)
        change=Column(Numeric(precision=18, scale=6))
        def __init__(self,id,social_network,this_cycle,last_cycle,change):
            self.id = id
            self.social_network=social_network
            self.this_cycle=this_cycle
            self.last_cycle=last_cycle
            self.change=change
    #TABLA PBI MONTHLY
    class PBI_Monthly(Base):
        __tablename__ = 'PBI_Monthly'
        id=Column(Integer, primary_key=True, autoincrement=False)
        año=Column(Integer)
        mes=Column(String(length=255))
        upload_date=Column(Date)
        for token in tokens:
            assets_price= f'Last_{token.name}_price'
            assets_volume = f'{token.name}_volume'
            assets_holders = f'{token.name}_holders'
            assets_staking = f'{token.name}_staking'
            locals()[assets_price] = Column(Numeric(precision=18, scale=6))
            locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            locals()[assets_holders] = Column(Integer)
            locals()[assets_staking] = Column(Numeric(precision=18, scale=6))
        for token in tokens_nft:
            if len(token.category)>0:
                for category in token.category:
                    assets_price= f'Last_{token.name}_{category}_price'
                    assets_volume = f'{token.name}_{category}_volume'
                    locals()[assets_price] = Column(Numeric(precision=18, scale=6))
                    locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            else:
                assets_price= f'Last_{token.name}_price'
                assets_volume = f'{token.name}_volume'
                locals()[assets_price] = Column(Numeric(precision=18, scale=6))
                locals()[assets_volume] = Column(Numeric(precision=18, scale=6))
            assets_holders = f'{token.name}_holders'
            locals()[assets_holders] = Column(Integer)
        btc_price=Column(Numeric(precision=18, scale=6))
        eth_price=Column(Numeric(precision=18, scale=6))
        bnb_price=Column(Numeric(precision=18, scale=6))
        niche_marketcap=Column(Numeric(precision=18, scale=6))
        def __init__(self,id,año,mes,upload_date,btc_price,eth_price,bnb_price,niche_marketcap,**kwargs):
            self.id=id
            self.año=año
            self.mes=mes
            self.upload_date=upload_date
            self.btc_price=btc_price
            self.eth_price=eth_price
            self.bnb_price=bnb_price
            self.niche_marketcap=niche_marketcap
            for token in tokens:
                assets_price= f'Last_{token.name}_price'
                assets_volume = f'{token.name}_volume'
                assets_holders = f'{token.name}_holders'
                assets_staking = f'{token.name}_staking'
                setattr(self, assets_price, kwargs.get(assets_price,0))
                setattr(self, assets_volume, kwargs.get(assets_volume,0))
                setattr(self, assets_holders, kwargs.get(assets_holders,0))
                setattr(self, assets_staking, kwargs.get(assets_staking,0))
            for token in tokens_nft:
                if len(token.category)>0:
                    for category in token.category:
                        assets_price= f'Last_{token.name}_{category}_price'
                        assets_volume = f'{token.name}_{category}_volume'
                        setattr(self, assets_price, kwargs.get(assets_price,0))
                        setattr(self, assets_volume, kwargs.get(assets_volume,0))
                else:
                    assets_price= f'Last_{token.name}_price'
                    assets_volume = f'{token.name}_volume'
                    setattr(self, assets_price, kwargs.get(assets_price,0))
                    setattr(self, assets_volume, kwargs.get(assets_volume,0))
                assets_holders = f'{token.name}_holders'
                setattr(self, assets_holders, kwargs.get(assets_holders,0))

    #generate database schema
    if create_tables:
        Base.metadata.create_all(engine)
        set_dtype_transfers_hist=Transfers_Historical(transfer_date='2000-01-01')
        session.add(set_dtype_transfers_hist)
        session.commit()
        set_dtype_holders_hist=Holders_Historical(id=0,upload_date='2000-01-01',last_transfer='2000-01-01',last_block_number=0)
        session.add(set_dtype_holders_hist)
        set_dtype_assets=Assets(id=0,upload_date='2000-01-01')
        session.add(set_dtype_assets)
        set_dtype_market_context=Market_Context(id=0,upload_date='2000-01-01')
        session.add(set_dtype_market_context)
        for address in project.dex_addresses:
            address=address.lower()
            dex_address=Holders(address=address,address_owner='DEX')
            session.add(dex_address)
        for address in project.burning_addresses:
            address=address.lower()
            dex_address=Holders(address=address,address_owner='Burning')
            session.add(dex_address)
        for address in project.minting_addresses:
            address=address.lower()
            dex_address=Holders(address=address,address_owner='Minting')
            session.add(dex_address)
        for address in project.project_addresses:
            address=address.lower()
            dex_address=Holders(address=address,address_owner='Project')
            session.add(dex_address)
        for token in project.tokens+project.tokens_NFT:
            if token.category:
                for category in token.category:
                    asset=Main_assets(asset=f'{token.name}_{category}')
                    session.add(asset)
            asset=Main_assets(asset=token.name)
            session.add(asset)
        session.commit()
    session.close()

