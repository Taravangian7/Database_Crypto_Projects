from nft_scraping import price_tofu,volume_tofu

class CryptoProject:
    def __init__(self,name,category,twitter,telegram,discord,tokens,tokens_NFT,dex_addresses,burning_addresses,minting_addresses,project_addresses):
        self.name=name
        self.category=category
        self.twitter=twitter
        self.telegram=telegram
        self.discord=discord
        self.tokens=tokens
        self.tokens_NFT=tokens_NFT
        self.dex_addresses=dex_addresses
        self.burning_addresses=burning_addresses
        self.minting_addresses=minting_addresses
        self.project_addresses=project_addresses
class Tokens:
    def __init__(self,name,symbol,blockchain,contract_address,whale_amount,price=None,vol=None,url_price=None,url_vol=None,staking_addresses=[],category=[],price_hist=None,vol_hist=None):
        self.name=name
        self.symbol=symbol #Aparece al lado de total supply
        self.blockchain=blockchain
        self.contract_address=contract_address
        self.staking_addresses=staking_addresses
        self.category=category
        self.whale_amount=whale_amount
        self.price_hist=price_hist
        self.vol_hist=vol_hist
        self.price=price
        self.vol=vol
        self.url_price=url_price
        self.url_vol=url_vol

my_projects=[
    {'Name':'NFT11_test',
     'Category':'gaming',
     'Twitter':'https://twitter.com/NFT11_Official',
     'Telegram':[{'English':'https://t.me/nft11_en_official'},{'Spanish':'https://t.me/nft11_es_official'}],
     'Discord':'https://discord.com/invite/sCMwjDABu5',
     'Tokens':[Tokens(name='NFT11',symbol='NFT11',blockchain='BSC',contract_address='0x73f67ae7f934ff15beabf55a28c2da1eeb9b56ec',whale_amount=20000)],
     'Tokens_NFT':[Tokens(name='Players',symbol='NP',blockchain='BSC',contract_address='0xc2dea142de50b58f2dc82f2cafda9e08c3323d53',whale_amount=60,
                          staking_addresses=['0x7bd96834dd035c487da56173032fb6bbb1c45925','0x7fb39a38d1a3e4e57d6791b954e1effc4039b3df'],category=['Legends_V1','Regulars_V1']),
                   Tokens(name='Seats',symbol='NS',blockchain='BSC',contract_address='0x6bf87165ea4c3442964752c359c3306d74bf4f3c',whale_amount=20,
                          staking_addresses=['0x7bd96834dd035c487da56173032fb6bbb1c45925','0x7fb39a38d1a3e4e57d6791b954e1effc4039b3df',
                                         '0x410d60015512e794768982d4ee7f86b1d41544cc','0x59abfe4b88024a267f962326d5bb82e0c0bd09ff'],category=['Tier_1','Tier_2','Tier_3','Tier_4','Tier_5','Tier_6']),
                   Tokens(name='Legends V2',symbol='NLP',blockchain='BSC',contract_address='0x0a83be45f65962e50495e20d7163a5eee1fb467c',whale_amount=60,
                          staking_addresses=['0x410d60015512e794768982d4ee7f86b1d41544cc','0x59abfe4b88024a267f962326d5bb82e0c0bd09ff'],price=price_tofu,vol=volume_tofu,
                          url_price='https://tofunft.com/collection/nft-11-legend-player/items?sort=price_asc',url_vol='https://tofunft.com/collection/nft-11-legend-player/activities?page='),
                   Tokens(name='Regulars V2',symbol='NRP',blockchain='BSC',contract_address='0xDCD4721E489F8FaC0d5D8F964Db826d0EdaFA547',whale_amount=60,
                          staking_addresses=['0x410d60015512e794768982d4ee7f86b1d41544cc','0x59abfe4b88024a267f962326d5bb82e0c0bd09ff'])],
     'Dex_addresses':['0x767f34BF351F67a0b9781a249C964508aD61f8F3','0xEf0A90fb728195F63C911f52ab4bde331089319f'],
     'Burning_addresses':['0x000000000000000000000000000000000000dead'],
     'Minting_addresses':['0x0000000000000000000000000000000000000000'],
     'Project_addresses':['0x1b5c0523ada3f1e6cfa1c8b9c2ff00beeade06e8','0x1f6e5bfd178caeab69e62161fb13d4313c79ba67','0x2242404b9be914a6e5fd6ec8e9e2295cdfb72ac6',
                          '0x2ed1187b8de59af657f8b81b57d9c888076bf599','0x3ab6b500a683c8f42729f6fe0f42be632d2d8c71','0x3bfd26b0ff576568660cb345af26daf59642c62a',
                          '0x410d60015512e794768982d4ee7f86b1d41544cc','0x420b9bad20d43d561377615325e60d5b7cdb1b35','0x4a63f4113eb45d8f25132757005a5be5bf4951c0',
                          '0x59abfe4b88024a267f962326d5bb82e0c0bd09ff','0x5a596017eccf5e9f69708dbb815e1f018a5bc587','0x61a902123ca8e5bb8936648ed4f10d18a6316c6d',
                          '0x6cdb1e27a18970e18e98f9ae80f3f52592dc676c','0x7bd96834dd035c487da56173032fb6bbb1c45925','0x7fb39a38d1a3e4e57d6791b954e1effc4039b3df',
                          '0x8724063e1e57ca83c49bed20e6e4a57153a6d6e6','0x9a2b021b8fb6ddcf81893571ff5123ea5d2989c2','0xa3c8803e89539813e10a9cad9af97e46d83f4da8',
                          '0xadea6f6df33a137e1113665770d1af268ade4ce2','0xb24d5557f8a467e5f0a92104ff4a26c1da51a392','0xb3dc391186c6644267ce5c61901e7c7f9db60759',
                          '0xb54a242d9f7aef8310924bcabf058fbd1c8d58af','0xc071b8176a96cdaebea711813f052ea89a21fd79','0xc70aa2c504e7db353395304a6fcda5c8f2e1f68f',
                          '0xcf701a6809e30cf615eb0b446d62091a3bf0cf0f','0xd29bad01a05c3db18fa4b2dbf92126bf7c1a4ac4','0xd89ef8e2556fccb5df5b0ce48cbd2a2bfff09778',
                          '0xedbdb5d45307b3112f823e818da8a0e6b6ef22b0','0xfc8b194c274ca935611441bcfb3379c4cff33d9f']}
    ]

projects=[]
for project in my_projects:
    projects.append(CryptoProject(project['Name'],project['Category'],project['Twitter'],project['Telegram'],project['Discord'],
                                  project['Tokens'],project['Tokens_NFT'],project['Dex_addresses'],project['Burning_addresses'],
                                  project['Minting_addresses'],project['Project_addresses']))