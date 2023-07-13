import sys, os
from flask import Blueprint, request, jsonify
from postgresql.Discussion import Discussion
from flask_cors import cross_origin

discussion_route = Blueprint('discussion_route', __name__)

# get all topics by package
@discussion_route.route('/<package_id>/topics/', methods=['GET'])
def get_topics(package_id):
    jwt_token = request.headers.get('Authorization')
    topic = Discussion(jwt_token, None)
    result = topic.get_topic(package_id)
    return {'ok':True, 'message': 'success', 'result': result}

# create topic
@discussion_route.route('/topics', methods=['POST'])
@cross_origin()
def create_topic():
    # get a authorization (api key) from header
    jwt_token = request.headers.get('Authorization')
    payload = request.json

    new_topic = Discussion(jwt_token, payload)
    return new_topic.create_topic()

# view topic details
@discussion_route.route('/topic/<topic_id>', methods=['GET'])
def get_topic(topic_id):
    result = Discussion().get_topic_details(topic_id)
    return {'ok':True, 'message': 'success', 'result': result}

# view comment by topic
@discussion_route.route('/comments/<topic_id>', methods=['GET'])
def view_topics(topic_id):
    jwt_token = request.headers.get('Authorization')
    topic = Discussion(jwt_token, None)
    result = topic.get_comment(topic_id)
    return {'ok':True, 'message': 'success', 'result': result}

# create comment, comment into the topic
@discussion_route.route('/comments/<topic_id>', methods=['POST'])
@cross_origin()
def create_comment(topic_id):
    jwt_token = request.headers.get('Authorization')
    print(f'token => {jwt_token}')
    payload = request.json
    topic = Discussion(jwt_token=jwt_token)
    return topic.create_comment(topic_id, payload)
