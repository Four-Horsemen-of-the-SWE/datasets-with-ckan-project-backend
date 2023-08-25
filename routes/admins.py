import sys, os
from flask import Blueprint, request
from ckan.ckan_connect import ckan_connect
from postgresql.Admin import Admin
from ckanapi import ValidationError, SearchError

admins_route = Blueprint('admins_route', __name__)

# get all admin
@admins_route.route('', methods=['GET'])
def get_all_admin():
	jwt_token = request.headers.get('Authorization')
	return Admin(jwt_token=jwt_token).get_all_admin()

# change role
@admins_route.route('/role', methods=['POST'])
def change_role():
	try:
		jwt_token = request.headers.get('Authorization')
		payload = request.json
		admin = Admin(jwt_token=jwt_token)

		result = admin.change_role(payload['user_id'], payload['role'])
		return result
	except:
		return {'ok': False, 'message': 'backend failed'}