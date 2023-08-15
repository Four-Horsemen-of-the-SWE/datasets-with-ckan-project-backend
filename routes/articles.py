import sys, os
from flask import Blueprint, request, jsonify
from postgresql.Article import Article
from flask_cors import cross_origin

article_route = Blueprint('article_route', __name__)

# get a article by package_id
@article_route.route('/<package_id>', methods=['GET'])
def get_article(package_id):
    jwt_token = request.headers.get('Authorization')
    result = Article(jwt_token).get_article_by_package(package_id)

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