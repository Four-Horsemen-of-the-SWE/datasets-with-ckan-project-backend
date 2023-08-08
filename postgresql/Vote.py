import os
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from .core.PostgreSQL import PostgreSQL
from .User import User
from datetime import datetime

class Vote(PostgreSQL):
  def __init__(self, jwt_token: str = None):
    super().__init__()
    if jwt_token is not None:
      self.user = User(jwt_token=jwt_token)

  def _vote_type_error(self, vote_type):
  	if vote_type not in ['upvote', 'downvote']:
  		return True
  	else:
  		return False

  # VOTE
  def vote(self, target_id: str = None, target_type: str = None, vote_type: str = None):
    try:
      if target_id is None:
        return {'ok': False, 'message': 'target_id is not provided.'}
      if target_type is None:
        return {'ok': False, 'message': 'target_type is not provided.'}
      if vote_type is None:
        return {'ok': False, 'message': 'vote_type is not provided.'}

      if self._vote_type_error(vote_type):
      	return {'ok': False, 'message': 'vote_type not valid.'}

      with self.engine.connect() as connection:
        _id = str(uuid.uuid4())
        query_string = text("INSERT INTO public.vote( id, target_id, target_type, vote_type, user_id) VALUES (:id, :target_id, :target_type, :vote_type, :user_id)")
        connection.execute(query_string.bindparams(id=_id, target_id=target_id, target_type=target_type, vote_type=vote_type, user_id=self.user.id))
        connection.commit()

      return {'ok': True, 'message': f'{vote_type} success.', 'vote_type': vote_type}
    except:
      return {'ok': False, 'message': 'backend failed.'}

  # GET VOTE
  def get_vote(self, target_id: str = None):
    try:
      if target_id is None:
        return {'ok': False, 'message': 'target_id is not provided.'}

      with self.engine.connect() as connection:
        query_string = text("SELECT target_id, SUM(CASE WHEN vote_type = 'upvote' THEN 1 WHEN vote_type = 'downvote' THEN -1 ELSE 0 END) AS vote_score FROM public.vote WHERE target_id = :target_id GROUP BY target_id")
        result = connection.execute(query_string.bindparams(target_id=target_id)).mappings().one()
        vote_result = []
        return {'ok': True, 'message': 'success', 'vote': result['vote_score']}

    except:
      return {'ok': False, 'message': 'backend failed.'}
