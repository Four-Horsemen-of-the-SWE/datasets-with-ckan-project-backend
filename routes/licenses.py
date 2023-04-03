from flask import Blueprint, request, jsonify
from ckan.ckan_connect import ckan_connect
import requests

licenses_route = Blueprint('licenses_route', __name__)

# get all licenses
# error when get a licenses via ckan api, we gonna get all of licenses via ckan demo website instead. lol
@licenses_route.route('/', methods=['GET'])
def get_all_licenses():
	result = requests.get('https://demo.ckan.org/th/api/3/action/license_list')
	if result.status_code == 200:
		result = result.json()
	return {'ok': True, 'message': 'success', 'result': result['result']}