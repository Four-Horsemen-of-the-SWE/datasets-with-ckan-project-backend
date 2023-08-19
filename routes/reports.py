from flask import Blueprint, request, jsonify
from ckan.ckan_connect import ckan_connect
from postgresql.User import User
from postgresql.Report import Report

reports_route = Blueprint('reports_route', __name__)

# create a report
@reports_route.route('/', methods=['POST'])
def create_report():
	jwt_token = request.headers.get('Authorization')
	payload = request.json

	result = Report(jwt_token).create_report(payload)
	if result:
		return {'ok': True, 'message': 'success.'}
	else:
		return {'ok': False, 'message': 'failed.'}