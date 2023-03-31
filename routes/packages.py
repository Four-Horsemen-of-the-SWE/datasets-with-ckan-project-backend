import sys, os
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin
from werkzeug.utils import secure_filename
from werkzeug.datastructures import  FileStorage
from ckan.ckan_connect import ckan_connect
from postgresql.User import User
from postgresql.Thumbnail import Thumbnail
import tempfile
import base64
import io

packages_route = Blueprint('packages_route', __name__)

# get all packages, (only necessary information)
@packages_route.route('/', methods=['GET'])
def get_packages():
	with ckan_connect() as ckan:
		result = []
		packages = ckan.action.current_package_list_with_resources(all_fields=True)
		for package in packages:
			# if package is public
			if package['private'] == False:
				result.append({
					'author': package['author'],
					'metadata_created': package['metadata_created'],
					'metadata_modified': package['metadata_modified'],
					'name': package['name'],
					'title': package['title'],
					'notes': package['notes'],
					'id': package['id'],
					'tags': package['tags'],
					'license_title': package['license_title'],
					'private': package['private']
				})
		return {'ok': True, 'message': 'success', 'result': result}

# create package
@packages_route.route('/', methods=['POST'])
@cross_origin()
def create_packages():
	token = request.headers.get('Authorization')
	payload = request.json
	user = User(jwt_token=token)

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.package_create(**payload)
		return {'ok': True, 'message': 'success', 'result': result}

# update package
@packages_route.route('/<package_name>', methods=['PUT'])
@cross_origin()
def update_package(package_name):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)
	payload = request.json

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.package_update(id=package_name, **payload)
		return {'ok': True, 'message': 'success', 'result': result}

# delete package
@packages_route.route('/<package_name>', methods=['DELETE'])
@cross_origin()
def delete_package(package_name):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.package_delete(id=package_name)
		return {'ok': True, 'message': 'success', 'result': result}

# create new resource
@packages_route.route('/resources', methods=['POST'])
@cross_origin()
def create_resource():
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	package_id = request.form['package_id']
	url = request.form['url']
	description = request.form['description']
	upload = request.files['upload']
	resource_name = request.form['name']

	payload = {
		'package_id': package_id,
		'url': url,
		'description': description,
		'format': upload.content_type,
		'name': upload.filename,
		'upload': upload
	}

	# save the file into temp folder
	filename = upload.filename
	file_path = os.path.join(os.path.abspath('file_upload_temp'), filename)
	upload.save(file_path)

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.resource_create(package_id=package_id, url=url, description=description, name=resource_name, upload=open(file_path, 'rb'))
		# if success, delete the temp file
		if result is not None:
			try:
				os.remove(file_path)
				return {'ok': True, 'message': 'upload resource success', 'result': result}
			except OSError as e:
				print(f'Error removing file: {e.filename} - {e.strerror}')
		else:
			return {'ok': False, 'message': 'failed to upload resource'}

# delete resource
@packages_route.route('/resources/<resource_id>', methods=['DELETE'])
def delete_resource(resource_id):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.resource_delete(id=resource_id)
		return {'ok': True, 'message': 'delete success'}

# get package deails, (giving a name to api, then return that package)
@packages_route.route('/<package_name>', methods=['GET'])
def get_package_datails(package_name):
	# token = request.headers.get('Authorization')
	# user = User(jwt_token=token)
	try:
		with ckan_connect() as ckan:
			result = ckan.action.package_show(id=package_name)
			if result:
				return {'ok': True, 'message': 'success', 'result': result}
			else:
				return {'ok': False, 'message': 'package not found'}
	except:
		return {'ok': False, 'message': 'flask api error'}

# get a number of packages
@packages_route.route('/number', methods=['GET'])
def get_number_of_packages():
	with ckan_connect() as ckan:
		result = ckan.action.package_list()
		return {'ok': True, 'message': 'success', 'number': len(result)}

# packages search
@packages_route.route('/search', methods=['GET'])
def search_packages():
	packages_name = request.args.get('q')
	tags_name = request.args.get('tags')

	with ckan_connect() as ckan:
		# if request come with query string
		result = ckan.action.package_search(q=packages_name, fq=tags_name, include_private=False, rows=1000)
		if(result['count'] > 0):
			return {'ok': True, 'message': 'success', 'result': result['results']}
		else:
			return {'ok': False, 'message': 'not found'}

# create follow datasets (bookmarked)
@packages_route.route('/bookmarked/<package_name>', methods=['POST'])
@cross_origin()
def create_packages_bookmarked(package_name):
	token = request.headers.get('Authorization')
	print('\n')
	print(f'token ==> {token}')
	print(f'package_name ==> {package_name}')
	print('\n')
	if token is None:
		return {'ok': False, 'message': 'token not provide'}
	user = User(jwt_token=token)
	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.follow_dataset(id=package_name)
		return {'ok': True,'message': 'success', 'result': result}

@packages_route.route('/bookmarked/<package_name>', methods=['GET'])
def check_package_bookmarked(package_name):
	token = request.headers.get('Authorization')
	if token is None:
		return {'ok': False, 'message': 'token not provide'}
	user = User(jwt_token=token)
	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.am_following_dataset(id=package_name)
		return {'ok': True,'message': 'success', 'result': result, 'bookmarked': result}
	
@packages_route.route('/bookmarked/<package_name>/', methods=['DELETE'])
def delete_package_bookmarked(package_name):
	token = request.headers.get('Authorization')
	if token is None:
		return {'ok': False, 'message': 'token not provide'}
	user = User(jwt_token=token)
	with ckan_connect(api_key=user.api_token) as ckan:
		ckan.action.unfollow_dataset(id=package_name)
		return {'ok': True,'message': 'success'}

# datasets (packages) thumbnail
@packages_route.route('/<package_id>/thumbnail', methods=['POST'])
def create_packages_thumbnail(package_id):
	token = request.headers.get('Authorization')
	image_data = request.files.get('thumbnail_image').read()

	new_thumbnail = Thumbnail(jwt_token=token)
	return new_thumbnail.create_thumbnail(package_id, image_data)

# get datasets (package) thumbnail -> return as base64
@packages_route.route('/<package_id>/thumbnail', methods=['GET'])
def get_package_thumbnail(package_id):
	thumbnail = Thumbnail()
	return thumbnail.get_thumbnail(package_id)

# update a thumbnail
@packages_route.route('/<package_id>/thumbnail', methods=['PUT'])
def update_package_thumbnail(package_id):
	token = request.headers.get('Authorization')
	image_data = request.files.get('thumbnail_image').read()

	new_thumbnail = Thumbnail(jwt_token=token)
	return new_thumbnail.update_thumbnail(package_id, image_data)