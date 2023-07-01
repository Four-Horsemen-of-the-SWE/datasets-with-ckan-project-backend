import os
from ckanapi import RemoteCKAN
from dotenv import load_dotenv

# load .env file
load_dotenv()
CKAN_URL = os.getenv('CKAN_URL')
CKAN_ADMIN_API = os.getenv('CKAN_ADMIN_API')

def ckan_connect(api_key: str = CKAN_ADMIN_API) -> any:
	try:
		return RemoteCKAN(CKAN_URL, api_key)
	except:
		pass