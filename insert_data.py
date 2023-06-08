from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker,declarative_base
from credentials import server,usuario,contraseña,driver
from schema import Holders
database="NFT11_test"
engine = create_engine(f'mssql+pyodbc://{usuario}:{contraseña}@{server}/{database}?driver={driver}')
Session=sessionmaker(bind=engine)
session=Session()
Base = declarative_base()

#create canciones
the_real_slim_shady = Holders(address_id="88df15dfd8f8fdf",address_owner="elton")
session.add(the_real_slim_shady)
session.commit()
session.close()