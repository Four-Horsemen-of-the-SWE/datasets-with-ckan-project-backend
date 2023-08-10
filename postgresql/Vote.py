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
  	if vote_type not in ['upvote', 'downvote', 'neutral']:
  		return True
  	else:
  		return False

  def _is_already_voted(self, target_id, user_id):
  	with self.engine.connect() as connection:
  		try:
	  		query_string = text("SELECT id, target_id, target_type, vote_type, user_id FROM public.vote WHERE target_id = :target_id AND user_id = :user_id")
	  		result = connection.execute(query_string.bindparams(target_id = target_id, user_id = self.user.id)).mappings().one()

	  		return {'voted': True, 'id':  result['id'], 'target_id': result['target_id'], 'vote_type': result['vote_type'], 'user_id': result['user_id']}
	  	except:
	  		return {'voted': False}

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
	      # Check if already voted
	      result = self._is_already_voted(target_id, self.user.id)
	      if result['voted']:
	        if vote_type == result['vote_type']:
	          return {'ok': False, 'message': 'user already voted'}
	        else:
	          query_string = text("UPDATE public.vote SET vote_type = :vote_type WHERE id = :_id")
	          connection.execute(query_string.bindparams(vote_type=vote_type, _id=result['id']))
	          connection.commit()
	          
	          return {'ok': True, 'message': f'{vote_type} success.', 'vote_type': vote_type}
	      else:
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

	    is_voted = False
	    voted_type = ''
	    with self.engine.connect() as connection:
	      if hasattr(self, 'user'):
	        # then return with voted
	        result = self._is_already_voted(target_id, self.user.id)
	       	if result['voted']:
	          is_voted = True
	          voted_type = result['vote_type']

	      query_string = text("SELECT target_id, SUM( CASE WHEN vote_type = 'upvote' THEN 1 WHEN vote_type = 'downvote' THEN -1 WHEN vote_type = 'neutral' THEN 0 ELSE 0 END ) AS vote_score FROM public.vote WHERE target_id = :target_id GROUP BY target_id")
	      result = connection.execute(query_string.bindparams(target_id=target_id)).mappings().one()

	      if is_voted:
	      	return {'ok': True, 'message': 'success', 'vote': result['vote_score'], 'is_voted': is_voted, 'voted_type': voted_type}
	      else:
	      	return {'ok': True, 'message': 'success', 'vote': result['vote_score'], }
	  except NoResultFound:
	    return {'ok': True, 'message': 'success', 'vote': 0}
	  except:
	    return {'ok': False, 'message': 'backend failed.'}