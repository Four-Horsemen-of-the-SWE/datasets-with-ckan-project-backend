import os
from ckanapi import RemoteCKAN
from dotenv import load_dotenv

# load .env file
load_dotenv()
CKAN_URL = os.getenv('CKAN_URL')
CKAN_ADMIN_API = os.getenv('CKAN_ADMIN_API')

def ckan_connect(api_key: str = CKAN_ADMIN_API) -> any:
	try:
		# this mean we already have api key
		if api_key is not None or api_key != '':
			return RemoteCKAN(CKAN_URL, api_key)
		else:
			return RemoteCKAN(CKAN_ADMIN_API, api_key)
	except:
		pass