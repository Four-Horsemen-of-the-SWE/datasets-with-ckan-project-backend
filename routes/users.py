import sys, os
from flask import Blueprint, request
from ckan.ckan_connect import ckan_connect
from postgresql.User import User

users_route = Blueprint('users_route', __name__)

# check if user is admin
@users_route.route('/is_admin', methods=['GET'])
def check_if_user_is_admin():
	jwt_token = request.headers.get('Authorization')
	user = User(jwt_token=jwt_token)

	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			result = ckan.action.user_show(id=user.id)
			is_admin = result['sysadmin'] == True
			return {'ok': True, 'message': 'success', 'is_admin': is_admin}
		except:
			return  {'ok': False, 'message': 'failed'}

# get all users
@users_route.route('/', methods=['GET'])
def get_users():
	# get a authorization (api_key) from header
	api_key = request.headers.get('Authorization')
	
	with ckan_connect(api_key=api_key) as ckan:
		return ckan.action.user_list()

# create users
@users_route.route('/', methods=['POST'])
def create_users():
	payload = request.json
	with ckan_connect() as ckan:
		try:
			user = ckan.action.user_create(**payload)
			# if user was created, now create their apy token
			if user:
				token_payload = {'name': 'ckan_private_api_token', 'user': payload['name']}
				ckan.action.api_token_create(**token_payload)
			return {'ok': True, 'message': 'success', 'user': user}
		except :
			return {'ok': False, 'message': 'Username already exists'}

# delete users
@users_route.route('/<users_id>', methods=['DELETE'])
def delete_user(users_id):
	# api_key = request.headers.get('Authorization')
	'''
		@p, mangkorn
		in delete method we gonna use a admin's api-key
	'''

	with ckan_connect() as ckan:
		ckan.action.user_delete(id=users_id)
		return {'ok': True, 'message': 'success'}

# login
@users_route.route('/login', methods=['POST'])
def login():
	payload = request.json
	user = User()
	token = user.login(payload['name'], payload['password'])
	if token:
		return {'ok': True,'message': 'success', 'token': token}
	else:
		return {'ok': False,'message': 'failed to login'}

# get a user details (using a ckanapi)
@users_route.route('/<user_name>', methods=['GET'])
def get_user_details(user_name):
	# token = request.headers.get('Authorization')
	# user = User(jwt_token=token)
	with ckan_connect() as ckan:
		result = ckan.action.user_show(id=user_name, include_datasets=True, include_num_followers=True)
		return {'ok': True, 'message': 'success', 'result': result}

# get a package that user collab
@users_route.route('/packages', methods=['GET'])
def get_user_packages():
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)
	with ckan_connect() as ckan:
		return ckan.action.package_collaborator_list_for_user(id=user.id)

# get a datasets (aka datasets) that user bookmarked
@users_route.route('/bookmarked', methods=['GET'])
def get_users_bookmarked():
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)
	with ckan_connect() as ckan:
		result = ckan.action.dataset_followee_list(id=user.id)
		return result
	
# get a list of user's organization
@users_route.route('/organizations', methods=['GET'])
def get_user_organizations():
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)
	with ckan_connect() as ckan:
		return ckan.action.organization_list_for_user(id=user.id)