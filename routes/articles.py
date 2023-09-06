import sys, os
from flask import Blueprint, request, jsonify
from postgresql.Article import Article
from flask_cors import cross_origin

article_route = Blueprint('article_route', __name__)

# get a article by article_id
@article_route.route('/<article_id>', methods=['GET'])
def get_article(article_id):
    jwt_token = request.headers.get('Authorization')
    result = Article(jwt_token).get_article(article_id)

    return result

# create article
@article_route.route('/', methods=['POST'])
def create_article():
	jwt_token = request.headers.get('Authorization')
	payload = request.json

	result = Article(jwt_token).create_article_by_package(payload)

	if result:
		return {'ok': True, 'message': 'success.'}
	else:
		return {'ok': False, 'message': 'create failed.'}

# delete article
@article_route.route('/<article_id>', methods=['DELETE'])
def delete_article(article_id):
	jwt_token = request.headers.get('Authorization')

	result = Article(jwt_token).delete_article_by_id(article_id)
	if result:
		return {'ok': True, 'message': 'success.'}
	else:
		return {'ok': False, 'message': 'failed.'}

# get all comment by article_id
@article_route.route('/<article_id>/comments', methods=['GET'])
def get_comment(article_id):
	jwt_token = request.headers.get('Authorization')
	return Article(jwt_token).get_comment_by_article(article_id)

# create comment for article
@article_route.route('/comments', methods=['POST'])
def create_comment():
	jwt_token = request.headers.get('Authorization')
	payload = request.json

	ok, result = Article(jwt_token).create_comment(payload)

	if ok:
		return {'ok': True, 'message': 'success.', 'result': result}
	else:
		return {'ok': False, 'message': 'create failed.'}

# delete comment by id
@article_route.route('/comments/<comment_id>', methods=['DELETE'])
def delete_comment(comment_id):
	jwt_token = request.headers.get('Authorization')

	result = Article(jwt_token=jwt_token).dalete_comment(comment_id)
	if result:
		return {'ok': True, 'message': 'success.'}
	else:
		return {'ok': False, 'message': 'failed.'}

# update comment by id
@article_route.route('/comments/<comment_id>', methods=['PUT'])
def update_comment(comment_id):
	jwt_token = request.headers.get('Authorization')
	payload = request.json

	ok, result = Article(jwt_token=jwt_token).update_comment(comment_id, payload)
	if ok:
		return {'ok': True, 'message': 'success.', 'result': result}
	else:
		return {'ok': False, 'message': 'failed.'}