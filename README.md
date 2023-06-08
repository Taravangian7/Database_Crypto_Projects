# Database_Crypto_Projects
This code creates a database for crypto projects and also takes care of keeping the database up to date.
The BSCScan and CoinGecko API is used to obtain project data. It is necessary to obtain credentials to use this.
The code works with all token transactions in the project. Therefore, it obtains information about all holders and their holdings. It also allows to know for each day the total amount of purchase/sale, purchase/sale made by whales, staking, price, volume, and also data related to social networks.
in projects.py you shoud charge the parameters of the projects. 
historical_data.py will upload the data until the current day.
daily_load.py is the file wich upload the daily data.