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

  def create_report_notification(self, report_id, user_id, message):
  	_id = uuid.uuid4()
  	try:
  		with self.engine.connect() as connection:
  			sql_query = text("INSERT INTO public.report_notification( id, user_id, report_id, message) VALUES (:id, :user_id, :report_id, :message)")
  			connection.execute(sql_query.bindparams(id=_id, user_id=user_id, report_id=report_id, message=message))
  			connection.commit()

  			return True
  	except:
  		pass

  # create a report
  def create_report(self, payload):
	  try:
	    with self.engine.connect() as connection:
	      _id = uuid.uuid4()
	      sql_query = text("INSERT INTO public.report(id, topic, description, entity_id, entity_type, entity_url, user_id) VALUES (:id, :topic, :description, :entity_id, :entity_type, :entity_url, :user_id)")
	      connection.execute(sql_query.bindparams(
	        id=_id,
	        topic=payload.get('topic'),
	        description=payload.get('description', ''),
	        entity_id=payload.get('entity_id'),
	        entity_type=payload.get('entity_type'),
	        entity_url=payload.get('entity_url'),
	        user_id=self.user.id
	      ))
	      connection.commit()

	      # then create a notification
	      entity_type = payload.get('entity_type')
	      description = payload.get('description')
	      result = self.create_report_notification(report_id=_id, user_id=payload.get('entity_owner'), message=f'You have a notification about a complaint on your {entity_type}. {description}')
	      if result:
	      	return True
	      else:
	      	return False
	  except:
	    return False

  def get_all_report_for_user(self):
  	with self.engine.connect() as connection:
  		sql_query = text("SELECT id, topic, description, entity_id, entity_type, entity_url, user_id, created_at, updated_at, status FROM public.report WHERE user_id = :user_id")
  		sql_result = connection.execute(sql_query.bindparams(user_id = self.user.id)).mappings().all()
  		result = []
  		for row in sql_result:
  			result.append({
  				'id': row['id'],
  				'topic': row['topic'],
  				'description': row['description'],
  				'entity_id': row['entity_id'],
  				'entity_type': row['entity_type'],
  				'entity_url': row['entity_url'],
  				'user_id': row['user_id'],
  				'created_at': row['created_at'].isoformat(),
  				'updated_at': row['updated_at'].isoformat(),
  				'status': row['status']
  			})
  		return True, result

  def get_all_report_notification(self):
  	with self.engine.connect() as connection:
  		try:
  			sql_query = text("SELECT public.report_notification.id, public.report_notification.user_id, public.report_notification.report_id, public.report.entity_id, public.report.entity_url, public.report.entity_type, message, is_read, public.report_notification.created_at, public.report_notification.updated_at FROM public.report_notification LEFT JOIN public.report ON report_id = public.report.id WHERE public.report_notification.user_id = :user_id")
  			sql_result = connection.execute(sql_query.bindparams(user_id = self.user.id)).mappings().all()
  			result = []
  			for row in sql_result:
  				result.append({
  					'id': row['id'],
  					'user_id': row['user_id'],
  					'report_id': row['report_id'],
  					'entity_id': row['entity_id'],
  					'entity_type': row['entity_type'],
  					'entity_url': row['entity_url'],
  					'message': row['message'],
  					'is_read': row['is_read'],
  					'created_at': row['created_at'].isoformat(),
  					'updated_at': row['updated_at'].isoformat()
  				})
  			return True, result
  		except NoResultFound:
  			return True, ['asd']
  		except:
  			return False, ['asdasd']