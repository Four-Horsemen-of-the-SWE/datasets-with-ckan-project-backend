import sys, os
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin
from werkzeug.utils import secure_filename
from werkzeug.datastructures import  FileStorage
from ckanapi import NotAuthorized, NotFound, CKANAPIError, ValidationError
from ckan.ckan_connect import ckan_connect
from postgresql.User import User
from postgresql.Thumbnail import Thumbnail
from postgresql.Dataset import Dataset
import tempfile
import base64
import io
import uuid

datasets_route = Blueprint('datasets_route', __name__)

@datasets_route.route('/visibility', methods=['POST'])
def change_visibility():
	jwt_token = request.headers.get('Authorization')
	payload = request.json
	return Dataset(jwt_token).change_visibility(payload['dataset_id'], payload['visibility'])

@datasets_route.route('is_private', methods=['GET'])
def is_private():
	dataset_id = request.args.get('id')
	result = Dataset().is_private(dataset_id)

	if result:
		return {'ok': True, 'message': f'{dataset_id} is private'}
	elif result is None:
		return {'ok': False, 'message': f'{dataset_id} not found'}
	else:
		return {'ok': True, 'message': f'{dataset_id} is public'}

# get all datasets, (only necessary information)
# this function should called in homepage
@datasets_route.route('/', methods=['GET'])
def get_datasets():
	limit = request.args.get('limit', 100)
	with ckan_connect() as ckan:
		result = []
		datasets = ckan.action.current_package_list_with_resources(all_fields=True, limit=limit)
		user = User()
		for dataset in datasets:
			# if dataset is public
			if Dataset().is_public(dataset['id']):
				thumbnail = Thumbnail().get_thumbnail(dataset['id'])
				result.append({
					'author': user.get_user_name(user_id = dataset['creator_user_id']),
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
# use in ViewDatasets page
@datasets_route.route('/<dataset_name>', methods=['GET'])
def get_dataset_datails(dataset_name):
	token = request.headers.get('Authorization')
	user_id = ''
	try:
		user = User(jwt_token=token)
		if user.api_token is not None or user.api_token != '':
			user_id = user.api_token
		else: None
	except:
		pass

	try:
		with ckan_connect(user_id) as ckan:
			result = ckan.action.package_show(id=dataset_name)

			# get thumbnail
			thumbnail = Thumbnail().get_thumbnail(result['id'])

			# insert thumbnail into result
			result['thumbnail'] = thumbnail['result']

			# insert author name into result
			result['author'] = user.get_user_name(result['creator_user_id'])

			# replace visibility value
			result['private'] = Dataset().is_private(result['id'])

			# check if datasets bookmarked
			try:
				isBookmark = ckan.action.am_following_dataset(id=dataset_name)
				result['is_bookmark'] = isBookmark
			except:
				result['is_bookmark'] = False

			return {'ok': True, 'message': 'success', 'result': result, 'is_authorized': True}
			
	except NotFound:
		return {'ok': False, 'message': 'datasets not found'}
	except NotAuthorized:
		return {'ok': False, 'message': 'notAuthorized to see this dataset', 'is_authorized': False}
	except:
		return {'ok': False, 'message': 'flask api error'}

# create dataset
@datasets_route.route('/', methods=['POST'])
@cross_origin()
def create_datasets():
	token = request.headers.get('Authorization')
	payload = request.json
	user = User(jwt_token=token)

	default_visibility = request.args.get('default_visibility', None)

	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			result = ckan.action.package_create(**payload)
			if default_visibility is not None:
				Dataset(jwt_token=token).change_visibility(result['id'], default_visibility)
			return {'ok': True, 'message': 'success', 'result': result}
		except ValidationError:
			return {'ok': False, 'message': 'name is already in use.'}
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

	if 'name' not in payload:
		payload['name'] = payload['title'].lower().replace(' ', '-')

	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			# if the dataset is private, turn into public then turn back
			dataset = Dataset(token)
			dataset_id = payload['id']
			payload.pop('id', None)
			payload.pop('private', None)
			is_private = False
			if dataset.is_private(dataset_id):
				dataset.change_visibility(dataset_id, 'public')
				is_private = True

			# then update dataset
			result = ckan.action.package_patch(id=dataset_name, **payload)
			# then turn it back to public
			if is_private is True:
				dataset.change_visibility(dataset_id, 'private')
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
    # try:
        token = request.headers.get('Authorization')
        user = User(jwt_token=token)

        if 'resources' not in request.files:
            return {'ok': False, 'message': 'file not provided'}

        resources = request.files.getlist('resources')
        results = []

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
                    'name': request.form.get('name', filename),
                    'mimetype': file.mimetype,
                    'upload': open(file_path, 'rb')
                }

                # Save into CKAN
                result = ckan.action.resource_create(**payload)
                if result is not None:
                    # Remove the file after successful upload
                    os.remove(file_path)
                    results.append(result)
                else:
                    return {'ok': False, 'message': 'Failed to upload resource'}

        return {'ok': True, 'message': 'create resource success.', 'result': results}
    # except:
    #    return {'ok': False, 'message': 'create resource failed.'}

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

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.resource_patch(**payload)
		if result is not None:
			return {'ok': True, 'message': 'update resource success', 'result': result}
				
# delete resource
@datasets_route.route('/resources/<resource_id>', methods=['DELETE'])
def delete_resource(resource_id):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	with ckan_connect(api_key=user.api_token) as ckan:
		result = ckan.action.resource_delete(id=resource_id)
		return {'ok': True, 'message': 'delete success', 'result': resource_id}

# get a number of datasets
@datasets_route.route('/number', methods=['GET'])
def get_number_of_datasets():
	with ckan_connect() as ckan:
		result = ckan.action.package_list()
		return {'ok': True, 'message': 'success', 'result': len(result)}

# search datasets
@datasets_route.route('/search', methods=['GET'])
def search_datasets():
    dataset_name = request.args.get('q', '')
    # tags
    tags = request.args.getlist('tags')
    filter_query = ''
    # license
    license = request.args.get('license', None)
    # sort
    sort = request.args.get('sort')

    if len(tags):
        filter_query = " AND ".join(f'tags:{tag}' for tag in tags)

    if license is not None:
        filter_query += f' AND license_id:{license}'

    with ckan_connect() as ckan:
        result = []
        user = User()
        searched_result = ckan.action.package_search(q=dataset_name, fq=filter_query, sort=sort, include_private=False, rows=1000)
        if searched_result['count'] > 0:
            for dataset in searched_result['results']:
            	if Dataset().is_public(dataset['id']):
	                # get thumbnail
	                thumbnail = Thumbnail().get_thumbnail(dataset['id'])

	                # get user name (author)
	                dataset['author'] = user.get_user_name(user_id = dataset['creator_user_id'])

	                # insert thumbnail into the dataset
	                dataset['thumbnail'] = thumbnail['result']

	                result.append(dataset)
            return jsonify({'ok': True, 'message': 'success', 'result': result})
        else:
            return jsonify({'ok': True, 'message': 'not found', 'result': [], 'dataset_name': dataset_name})



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
		result = ckan.action.am_following_dataset(id=dataset_name)
		return {'ok': True,'message': 'success', 'result': result, 'bookmarked': result}

# bookmark datasets
@datasets_route.route('/<dataset_name>/bookmark/', methods=['POST'])
def create_dataset_bookmarked(dataset_name):
	token = request.headers.get('Authorization')
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
@datasets_route.route('/<dataset_name>/bookmark', methods=['DELETE'])
def delete_dataset_bookmarked(dataset_name):
	token = request.headers.get('Authorization')

	if token is None:
		return {'ok': False, 'message': 'token not provide'}
	user = User(jwt_token=token)
	with ckan_connect(api_key=user.api_token) as ckan:
		try:
			ckan.action.unfollow_dataset(id=dataset_name)
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

# collect download statistic
@datasets_route.route('/<dataset_id>/download', methods=['POST'])
def collect_download_static(dataset_id):
	jwt_token = request.headers.get('Authorization')
	dataset = Dataset(jwt_token=jwt_token)

	result = dataset.collect_download_static(dataset_id)
	if result:
		return {'ok': True, 'message': 'success.'}
	else:
		return {'ok': False, 'message': 'failed.'}

# get download statistic, (for dataset view page)
@datasets_route.route('/<dataset_id>/download', methods=['GET'])
def get_download_statistic(dataset_id):
	dataset = Dataset()

	result = dataset.get_download_statistic(dataset_id)
	if result['ok']:
		return {'ok': True, 'message': 'success.', 'result': result['result'], 'total_download': result['total_download']}
	else:
		return {'ok': False, 'message': 'failed.'}