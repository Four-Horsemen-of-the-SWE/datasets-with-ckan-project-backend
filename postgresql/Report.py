import os
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from .core.PostgreSQL import PostgreSQL
from .User import User
from datetime import datetime

class Report(PostgreSQL):
  def __init__(self, jwt_token: str = None):
    super().__init__()
    if jwt_token is not None:
      self.user = User(jwt_token=jwt_token)

  # create a report
  def create_report(self, payload):
	  _id = uuid.uuid4()
	  try:
	    with self.engine.connect() as connection:
	      sql_query = text("INSERT INTO public.report(id, topic, description, entity_id, entity_type, user_id) VALUES (:id, :topic, :description, :entity_id, :entity_type, :user_id)")
	      connection.execute(sql_query.bindparams(
	        id=_id,
	        topic=payload.get('topic'),
	        description=payload.get('description', ''),
	        entity_id=payload.get('entity_id'),
	        entity_type=payload.get('entity_type'),
	        user_id=self.user.id
	      ))
	      connection.commit()
	      return True
	  except:
	    return False

  def get_all_report_for_user(self):
  	with self.engine.connect() as connection:
  		sql_query = text("SELECT id, topic, description, entity_id, entity_type, user_id, created_at, updated_at, status FROM public.report WHERE user_id = :user_id")
  		sql_result = connection.execute(sql_query.bindparams(user_id = self.user.id)).mappings().all()
  		result = []
  		for row in sql_result:
  			result.append({
  				'id': row['id'],
  				'topic': row['topic'],
  				'description': row['description'],
  				'entity_id': row['entity_id'],
  				'entity_type': row['entity_type'],
  				'user_id': row['user_id'],
  				'created_at': row['created_at'].isoformat(),
  				'updated_at': row['updated_at'].isoformat(),
  				'status': row['status']
  			})
  		return True, result