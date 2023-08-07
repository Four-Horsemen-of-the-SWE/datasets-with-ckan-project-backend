from flask import Blueprint, request
from ckan.ckan_connect import ckan_connect
from postgresql.User import User

tags_route = Blueprint('tags_route', __name__)

# get all tags
@tags_route.route('/', methods=['GET'])
def get_tags():
	with ckan_connect() as ckan:
		result = ckan.action.tag_list(all_fields=True)
		return {'ok': True, 'message': 'success', 'result': result, 'count': len(result)}

# create a tags
@tags_route.route('/', methods=['POST'])
def create_tags():
	payload = request.json
	with ckan_connect() as ckan:
		return ckan.action.tag_create(**payload)

# tags search
@tags_route.route('/search', methods=['GET'])
def search_tags():
	tags_name = request.args.get('q')
	with ckan_connect() as ckan:
		return ckan.action.tag_search(query=tags_name)

# tags delete
@tags_route.route('/<tag_name>', methods=['DELETE'])
def delete_tags(tag_name):
	token = request.headers.get('Authorization')
	user = User(jwt_token=token)

	with ckan_connect(user.api_token) as ckan:
		result = ckan.action.tag_delete(id=tag_name)
		return {'ok': True, 'message': 'success', 'result': result}
