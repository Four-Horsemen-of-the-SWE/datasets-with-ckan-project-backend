import os
import uuid
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from .core.PostgreSQL import PostgreSQL
from .User import User
from datetime import datetime

class Article(PostgreSQL):
	def __init__(self, jwt_token: str = None):
		super().__init__()
		if jwt_token is not None:
			self.user = User(jwt_token=jwt_token)

	def get_article_by_package(self, package_id):
		with self.engine.connect() as connection:
			try:
				query_string = text("SELECT id, title, content, user_id, package_id, reference_url, created_at, updated_at FROM public.article WHERE package_id = :package_id")
				result = dict(connection.execute(query_string.bindparams(package_id = package_id)).mappings().one())

				# format date
				article_data = {
          'id': result.get('id'),
          'title': result.get('title'),
          'content': result.get('content'),
          'user_id': result.get('user_id'),
          'package_id': result.get('package_id'),
          'reference_url': result.get('reference_url'),
          'created_at': result.get('created_at').isoformat(),
          'updated_at': result.get('updated_at').isoformat()
	      }
				return {'ok': True, 'message': 'success', 'result': result['content'], 'is_created': True}
			except NoResultFound:
				return {'ok': True, 'message': 'article not created.', 'is_created': False}
			except:
				return {'ok': False, 'message': 'backend failed.', 'is_created': None}


	def create_article_by_package(self, payload):
		_id = uuid.uuid4()

		with self.engine.connect() as connection:
			try:
				query_string = text("INSERT INTO public.article( id, title, content, user_id, package_id, reference_url) VALUES (:id, :title, :content, :user_id, :package_id, :reference_url) ON CONFLICT (package_id) DO UPDATE SET title = :title, content = :content, user_id = :user_id, reference_url = :reference_url")
				connection.execute(query_string.bindparams(id = _id, title = payload.get('title', None), content = json.dumps(payload.get('content')), user_id = self.user.id ,package_id = payload.get('package_id'), reference_url =payload.get('reference_url', None)))
				connection.commit()

				return True
			except:
				return False