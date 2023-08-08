from flask import Blueprint, request
from postgresql.Vote import Vote
from flask_cors import cross_origin

votes_route = Blueprint('votes_route', __name__)

# VOTE
@votes_route.route('/', methods=['POST'])
@cross_origin()
def vote():
  jwt_token = request.headers.get('Authorization')
  # target_id=XXX-XXX-XXX-XXX, target_type=topic, vote_type=upvote
  payload = request.json

  return Vote(jwt_token=jwt_token).vote(payload['target_id'], payload['target_type'], payload['vote_type'])

@votes_route.route('/<target_id>', methods=['GET'])
@cross_origin()
def get_vote(target_id):
  # target_id = request.args.get('target_id')

  return Vote().get_vote(target_id)