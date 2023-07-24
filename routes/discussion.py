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
@discussion_route.route('/<package_id>/topics', methods=['POST'])
@cross_origin()
def create_topic(package_id):
    # get a authorization (api key) from header
    jwt_token = request.headers.get('Authorization')
    payload = request.json

    new_topic = Discussion(jwt_token, payload)
    return new_topic.create_topic(package_id=package_id)

# delete topic
@discussion_route.route('/<topic_id>/topics', methods=['DELETE'])
@cross_origin()
def delte_topic(topic_id):
    # get a authorization (api key) from header
    jwt_token = request.headers.get('Authorization')

    new_topic = Discussion(jwt_token)
    return new_topic.delete_topic(topic_id=topic_id)

# view topic details, include comments
@discussion_route.route('/topic/<topic_id>', methods=['GET'])
def get_topic_and_comments(topic_id):
    result = Discussion().get_topic_and_comments(topic_id)
    return {'ok': True, 'result': result}

# create comment, comment into the topic
@discussion_route.route('/comments/<topic_id>', methods=['POST'])
@cross_origin()
def create_comment(topic_id):
    jwt_token = request.headers.get('Authorization')
    payload = request.json
    topic = Discussion(jwt_token=jwt_token)
    return topic.create_comment(topic_id, payload)

# update comment
@discussion_route.route('/comments/<comment_id>', methods=['PUT'])
@cross_origin()
def update_comment(comment_id):
    jwt_token = request.headers.get('Authorization')
    payload = request.json
    topic = Discussion(jwt_token=jwt_token)
    return topic.update_comment(comment_id, payload)

# delete comment
@discussion_route.route('/comments/<comment_id>', methods=['DELETE'])
@cross_origin()
def delete_comment(comment_id):
    jwt_token = request.headers.get('Authorization')
    topic = Discussion(jwt_token=jwt_token)
    return topic.delete_comment(comment_id)