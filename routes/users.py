import sys, os
from flask import Blueprint, request
from ckan.ckan_connect import ckan_connect
from postgresql.User import User
from ckanapi import ValidationError, SearchError

users_route = Blueprint('users_route', __name__)

# change role
@users_route.route('/role', methods=['POST'])
def change_role():
	try:
		jwt_token = request.headers.get('Authorization')
		payload = request.json
		user = User(jwt_token=jwt_token)

		result = user.change_role(payload['user_id'], payload['role'])
		return result
	except:
		return {'ok': False, 'message': 'backend failed'}

# check if user is admin
@users_route.route('/admin', methods=['GET'])
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
	api_key = request.headers.get('Authorization')
	
	with ckan_connect(api_key=api_key) as ckan:
		result = ckan.action.user_list()
		return {'ok': True, 'result': result}

# search user (auto complete)
@users_route.route('/auto_complete', methods=['GET'])
def searhc_user():
	api_key = request.headers.get('Authorization')
	q = request.args.get('q')
	include_admin = request.args.get('include_admin', False)

	limit = request.args.get('limit', 10)

	with ckan_connect(api_key=api_key) as ckan:
		result = []
		if include_admin is True:
			result = ckan.action.user_autocomplete(q=q, limit=limit)
		else:
			print('')
			# get only member
			query_result = ckan.action.user_list(q=q)
			for user in query_result:
				if not user['sysadmin']:
					result.append(user)
		return {'ok': True, 'result': result}

# create users
@users_route.route('/', methods=['POST'])
def create_users():
	payload = request.json
	with ckan_connect() as ckan:
		# transform name to lowercase
		payload['name'] = payload['name'].lower()

		try:
			user = ckan.action.user_create(**payload)
			# if user was created, now create their apy token
			if user:
				token_payload = {'name': 'ckan_private_api_token', 'user': payload['name']}
				ckan.action.api_token_create(**token_payload)
			return {'ok': True, 'message': 'success', 'user': user}
		except ValidationError:
			return {'ok': False, 'message': 'ValidationError in backend'}

# delete users
@users_route.route('/<users_id>', methods=['DELETE'])
def delete_user(users_id):
	'''
		@p, mangkorn, phone
		in delete method we gonna use a admin's api-key
	'''

	with ckan_connect() as ckan:
		ckan.action.user_delete(id=users_id)
		return {'ok': True, 'message': 'success'}

# login
@users_route.route('/login', methods=['POST'])
def login():
	try:
		payload = request.json

		# if username or password not provided
		if 'username' not in payload or 'password' not in payload:
			return {'ok': False, 'message': 'username or password is not provided'}
		
		user = User()
		response = user.login(payload['username'], payload['password'])

		# if username not lowercase
		if not payload['username'].islower():
			return {'ok': False, 'message': 'username must be all lowercase.'}

		if response:
			# 2,592,000,000 milliseconds = 30 days
			return {'ok': True,'message': 'success', 'accessToken': response['accessToken'], 'expiresIn': 2592000000, 'user': response['user']}
		else:
			return {'ok': False,'message': 'invalid username or password'}
	except:
		return {'ok': False,'message': 'backend server failed'}

# get a user details (using a ckanapi)
@users_route.route('/<user_name>', methods=['GET'])
def get_user_details(user_name):
	with ckan_connect() as ckan:
		result = ckan.action.user_show(id=user_name, include_datasets=True, include_num_followers=True)
		return {'ok': True, 'message': 'success', 'result': result}

# get a package that user created
@users_route.route('/datasets', methods=['GET'])
def get_user_datasets():
	# try:
		token = request.headers.get('Authorization')		
		user = User(jwt_token=token)
		
		with ckan_connect() as ckan:
			# return ckan.action.package_collaborator_list_for_user(id=user.id)
			datasets = ckan.action.package_search(fq=f'creator_user_id:{user.id}')
			return {'ok': True, 'message': 'success', 'count': datasets['count'], 'result': datasets['results']}
	# except SearchError:
		# return {'ok': False, 'message': 'token not provided. user_is is empty or null'}
	# except:
		# return {'ok': False, 'message': 'token not provided'}

# get a datasets that user bookmark
@users_route.route('/bookmark', methods=['GET'])
def get_users_bookmarked():
	try:
		token = request.headers.get('Authorization')
		user = User(jwt_token=token)
		with ckan_connect() as ckan:
			result = ckan.action.dataset_followee_list(id=user.id)
			return {'ok': True, 'message': 'success', 'result': result}
	except:
		return {'ok': False, 'message': 'token not provided'}
	
# get a list of user's organization
@users_route.route('/organizations', methods=['GET'])
def get_user_organizations():
	try:
		token = request.headers.get('Authorization')
		user = User(jwt_token=token)
		with ckan_connect() as ckan:
			return ckan.action.organization_list_for_user(id=user.id)
	except:
		return {'ok': False, 'message': 'token not provided'}