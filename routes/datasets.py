import sys, os
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin
from werkzeug.utils import secure_filename
from werkzeug.datastructures import  FileStorage
from ckanapi import NotAuthorized, NotFound, CKANAPIError
from ckan.ckan_connect import ckan_connect
from postgresql.User import User
from postgresql.Thumbnail import Thumbnail
import tempfile
import base64
import io
import uuid

datasets_route = Blueprint('datasets_route', __name__)

# get all datasets, (only necessary information)
# this function should called in homepage
@datasets_route.route('/', methods=['GET'])
def get_datasets():
	with ckan_connect() as ckan:
		result = []
		datasets = ckan.action.current_package_list_with_resources(all_fields=True)
		for dataset in datasets:
			# if dataset is public
			if dataset['private'] == False:
				thumbnail = Thumbnail().get_thumbnail(dataset['id'])
				result.append({
					'author': dataset['author'],
					'metadata_created': dataset['metadata_created'],
					'metadata_modified': dataset['metadata_modified'],
					'name': dataset['name'],
					'title': dataset['title'],
					'notes': dataset['notes'],
					'id': dataset['id'],
					'tags': dataset['tags'],
					'license_title': dataset['license_title'],
					'private': dataset['private'],
					'thumbnail': thumbnail['result']
				})
		return {'ok': True, 'message': 'success', 'result': result}

# get dataset deails, (giving a name to api, then return that dataset)
# since 27/6/2023 @JCSNP. this function will return a thumbnails
@datasets_route.route('/<dataset_name>', methods=['GET'])
def get_dataset_datails(dataset_name):
	try:
		with ckan_connect() as ckan:
			result = ckan.action.package_show(id=dataset_name)
			thumbnail = Thumbnail().get_thumbnail(result['id'])

			# insert thumbnail into result
			result['thumbnail'] = thumbnail['result']
			return {'ok': True, 'message': 'success', 'result': result}
	except NotFound:
		return {'ok': False, 'message': 'datasets not found'}
	except:
		return {'ok': False, 'message': 'flask api error'}

# create dataset
@datasets_route.route('/', methods=['POST'])
@cross_origin()
def create_datasets():
	token = request.headers.get('Authorization')
	payload = request.json
	user = User(jwt_token=token)

	if 'resouces' in payload:
		payload.json.getlist('files')

	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			result = ckan.action.package_create(**payload)
			return {'ok': True, 'message': 'success', 'result': result}
		except CKANAPIError:
			return {'ok': False, 'message': 'ckan api error'}
		except:
			return {'ok': False, 'message': 'api server error'}

# update dataset
@datasets_route.route('/<dataset_name>', methods=['PUT'])
@cross_origin()
def update_dataset(dataset_name):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)
	payload = request.json

	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			result = ckan.action.package_update(id=dataset_name, **payload)
			return {'ok': True, 'message': 'success', 'result': result}
		except CKANAPIError:
			return {'ok': False, 'message': 'ckan api error'}
		except NotAuthorized:
			return {'ok': False, 'message': 'access denied'}
		except NotFound:
			return {'ok': False, 'message': f'dataset = {dataset_name} not found'}

# delete dataset
@datasets_route.route('/<dataset_name>', methods=['DELETE'])
@cross_origin()
def delete_dataset(dataset_name):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			result = ckan.action.package_delete(id=dataset_name)
			return {'ok': True, 'message': 'success', 'result': result}
		except NotAuthorized:
			return {'ok': False, 'message': 'access denied'}
		except NotFound:
			return {'ok': False, 'message': f'dataset = {dataset_name} not found'}

# create new resource
@datasets_route.route('/<dataset_id>/resources', methods=['POST'])
@cross_origin()
def create_resource(dataset_id):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	if 'resources' not in request.files:
		return {'ok': False, 'message': 'file not provided'}

	resources = request.files.getlist('resources')
	with ckan_connect(api_key=user.api_token) as ckan:
	    for file in resources:
	        filename = file.filename
	        unique_filename = f'{str(uuid.uuid4())[:4]}_{filename}'
	        file_path = os.path.join(os.path.abspath('upload'), unique_filename)
        	file.save(file_path)

	        payload = {
	            'package_id': dataset_id,
	            'url': request.form.get('url', ''),
	            'description': request.form.get('description', ''),
	            'format': os.path.splitext(filename)[1][1:].lower(),
	            'name': filename,
	            'mimetype': file.mimetype,
	            'upload': open(file_path, 'rb')
	        }

	        # Save into CKAN
	        result = ckan.action.resource_create(**payload)
	        if result is not None:
            # Remove the file after successful upload
	            os.remove(file_path)
	            return {'ok': True, 'message': 'Upload resource success', 'result': result}
	        else:
	            return {'ok': False, 'message': 'Failed to upload resource'}

# update resource
@datasets_route.route('/resources/<resource_id>', methods=['PUT'])
@cross_origin()
def update_resource(resource_id):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	payload = {
		'id': resource_id,
	}

	if 'name' in request.form:
		payload['name'] = request.form['name']
	if 'description' in request.form:
		payload['description'] = request.form['description']
	if 'upload' in request.files:
		upload = request.files['upload']
		# save the file into temp folder
		filename = upload.filename
		file_path = os.path.join(os.path.abspath('file_upload_temp'), filename)
		upload.save(file_path)
		payload['upload'] = open(file_path, 'rb')

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.resource_patch(**payload)
		if result is not None:
			try:
				# close file
				if 'upload' in payload:
					payload['upload'].close()
					os.remove(file_path)
				return {'ok': True, 'message': 'update resource success', 'result': result}
			except OSError as e:
				print(f'Error removing file: {e.filename} - {e.strerror}')
				
# delete resource
@datasets_route.route('/resources/<resource_id>', methods=['DELETE'])
def delete_resource(resource_id):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.resource_delete(id=resource_id)
		return {'ok': True, 'message': 'delete success'}

# get a number of datasets
@datasets_route.route('/number', methods=['GET'])
def get_number_of_datasets():
	with ckan_connect() as ckan:
		result = ckan.action.package_list()
		return {'ok': True, 'message': 'success', 'number': len(result)}

# datasets search
@datasets_route.route('/search', methods=['GET'])
def search_datasets():
	datasets_name = request.args.get('q')
	tags = request.args.getlist('tags')
	tag_query = '';

	if datasets_name is None or datasets_name == 'null':
		datasets_name = "*:*"
	if len(tags):
		# fq='tags:(ambatukam OR medicine OR amazon)'
		tag_str = " OR ".join(f"{tag}" for tag in tags)
		tag_query = f'tags:({tag_str})'
	else:
		tag_query = "*:*"

	print(datasets_name, tag_query)

	with ckan_connect() as ckan:
		# if request come with query string
		result = ckan.action.package_search(q=datasets_name, fq=tag_query, include_private=False, rows=1000)
		if(result['count'] > 0):
			return {'ok': True, 'message': 'success', 'result': result['results']}
		else:
			return {'ok': False, 'message': 'not found'}

# dataset search but it's auto complete
@datasets_route.route('/search/auto_complete', methods=['GET'])
def search_datasets_auto_complete():
	dataset_name = request.args.get('q')

	with ckan_connect() as ckan:
		result = ckan.action.package_autocomplete(q=dataset_name, limit=10)

		if result:
			return {'ok': True, 'message': 'success', 'result': result}
		else:
			return {'ok': False, 'message': 'not found'}

# check if dataset bookmarked
@datasets_route.route('/bookmark/<dataset_name>', methods=['GET'])
def check_dataset_bookmarked(dataset_name):
	token = request.headers.get('Authorization')
	if token is None:
		return {'ok': False, 'message': 'token not provide'}
	user = User(jwt_token=token)
	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.am_following_package(id=dataset_name)
		return {'ok': True,'message': 'success', 'result': result, 'bookmarked': result}

# bookmark datasets
@datasets_route.route('/<dataset_name>/bookmark', methods=['POST'])
def create_dataset_bookmarked(dataset_name):
	token = request.headers.get('Authorization')
	# payload = request.json

	if dataset_name is None:
		dataset_name = payload['dataset_name']
	if token is None:
		return {'ok': False, 'message': 'token not provide'}

	user = User(jwt_token=token)
	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			result = ckan.action.follow_dataset(id=dataset_name)
			return {'ok': True,'message': 'success', 'result': result}
		except:
			return {'ok': False,'message': 'failed'}

# Un-bookmarked
@datasets_route.route('/bookmark/<dataset_name>/', methods=['DELETE'])
def delete_dataset_bookmarked(dataset_name):
	token = request.headers.get('Authorization')
	if token is None:
		return {'ok': False, 'message': 'token not provide'}
	user = User(jwt_token=token)
	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			ckan.action.unfollow_package(id=dataset_name)
			return {'ok': True,'message': 'success'}
		except:
			return {'ok': False,'message': 'failed', 'result': result}

# create datasets thumbnail
@datasets_route.route('/<dataset_id>/thumbnail', methods=['POST'])
def create_datasets_thumbnail(dataset_id):
	token = request.headers.get('Authorization')
	image_data = request.files.get('thumbnail_image')

	thumbnail = Thumbnail(jwt_token=token).create_thumbnail(dataset_id, image_data)
	return thumbnail

# get datasets (dataset) thumbnail
@datasets_route.route('/<dataset_id>/thumbnail', methods=['GET'])
def get_dataset_thumbnail(dataset_id):
	thumbnail = Thumbnail()
	return thumbnail.get_thumbnail(dataset_id)

# update a thumbnail
@datasets_route.route('/<dataset_id>/thumbnail', methods=['PUT'])
def update_dataset_thumbnail(dataset_id):
	token = request.headers.get('Authorization')
	image_data = request.files.get('thumbnail_image').read()

	new_thumbnail = Thumbnail(jwt_token=token)
	return new_thumbnail.update_thumbnail(dataset_id, image_data)