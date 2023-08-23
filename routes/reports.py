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

# get user's reports
@reports_route.route('/me', methods=['GET'])
def get_all_report_for_user():
	jwt_token = request.headers.get('Authorization')

	ok, result = Report(jwt_token).get_all_report_for_user()
	if ok:
		return {'ok': True, 'message': 'success.', 'result': result}
	else:
		return {'ok': False, 'message': 'failed.'}

# get all notification for user
@reports_route.route('/notifications', methods=['GET'])
def get_all_report_notification():
	jwt_token = request.headers.get('Authorization')

	ok, result = Report(jwt_token).get_all_report_notification()
	if ok:
		return {'ok': True, 'message': 'success.', 'result': result}
	else:
		return {'ok': False, 'message': 'failed.'}