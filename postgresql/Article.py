import os
import uuid
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from .core.PostgreSQL import PostgreSQL
from .User import User
from datetime import datetime
import json

THUMBNAIL_FOLDER = os.getenv('THUMBNAIL_FOLDER')
THUMBNAIL_HOST = os.getenv('THUMBNAIL_HOST')

class Article(PostgreSQL):
	def __init__(self, jwt_token: str = None):
		super().__init__()
		if jwt_token is not None:
			self.user = User(jwt_token=jwt_token)

	def get_article_by_package(self, package_id):
		with self.engine.connect() as connection:
			try:
				query_string = text("SELECT id, title, content, user_id, package_id, reference_url, thumbnail, created_at, updated_at FROM public.article WHERE package_id = :package_id")
				query_result = connection.execute(query_string.bindparams(package_id = package_id)).mappings().all()

				article_data = []
				for row in query_result:
					article_data.append({
			          'id': row.get('id'),
			          'title': row.get('title'),
			          'content': row.get('content'),
			          'user_id': row.get('user_id'),
			          'package_id': row.get('package_id'),
			          'reference_url': row.get('reference_url'),
			          'created_at': row.get('created_at').isoformat(),
			          'updated_at': row.get('updated_at').isoformat(),
			          'thumbnail': str(THUMBNAIL_HOST) + '/' + str(row.get('thumbnail')) if row.get('thumbnail') is not None else None
					})
				return {'ok': True, 'message': 'success', 'result': article_data, 'is_created': True}
			except NoResultFound:
				return {'ok': True, 'message': 'article not created.', 'is_created': False}
			except:
				return {'ok': False, 'message': 'backend failed.', 'is_created': None}

	def get_article(self, article_id):
		with self.engine.connect() as connection:
			#try:
				query_string = text("SELECT id, title, content, user_id, package_id, reference_url, created_at, updated_at FROM public.article WHERE id = :article_id")
				query_result = connection.execute(query_string.bindparams(article_id = article_id)).mappings().one()
				result = dict(query_result)
				result['created_at'] = query_result['created_at'].isoformat()
				result['updated_at'] = query_result['updated_at'].isoformat()

				return {'ok': True, 'message': 'success', 'result': result}
			#except:
				#return {'ok': False, 'message': 'failed'}

	def create_article_by_package(self, title, content, package_id, file = None, article_id = None):
		_id = uuid.uuid4()

		if file is not None:
			# create file name
			file.filename = str(_id) + '_articles-thumbnail_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + os.path.splitext(file.filename)[1]
			file_path = os.path.join(THUMBNAIL_FOLDER, file.filename)
			file.save(file_path)

		with self.engine.connect() as connection:
			#try:
				raw = json.loads(content)
				if file is not None:
					query_string = ""
					if article_id is not None:
						query_string = text("UPDATE public.article SET title = :title, content = :content, user_id = :user_id, reference_url = :reference_url, thumbnail = :thumbnail WHERE id = :article_id")
						connection.execute(query_string.bindparams(title = title, content = json.dumps(raw), user_id = self.user.id, reference_url = '', thumbnail = file.filename if file is not None else None, article_id = article_id))
					else:
						query_string = text("INSERT INTO public.article( id, title, content, user_id, package_id, reference_url, thumbnail) VALUES (:id, :title, :content, :user_id, :package_id, :reference_url, :thumbnail)")
						connection.execute(query_string.bindparams(id = _id, title = title, content = json.dumps(raw), user_id = self.user.id ,package_id = package_id, reference_url = '', thumbnail = file.filename if file is not None else None))
				else:
					query_string = ""
					if article_id is not None:
						query_string = text("UPDATE public.article SET title = :title, content = :content, user_id = :user_id, reference_url = :reference_url WHERE id = :article_id")
						connection.execute(query_string.bindparams(title = title, content = json.dumps(raw), user_id = self.user.id, reference_url = '', article_id = article_id))
					else:
						query_string = text("INSERT INTO public.article( id, title, content, user_id, package_id, reference_url) VALUES (:id, :title, :content, :user_id, :package_id, :reference_url)")
						connection.execute(query_string.bindparams(id = _id, title = title, content = json.dumps(raw), user_id = self.user.id ,package_id = package_id, reference_url = ''))
				connection.commit()

				return True
			#except FileNotFoundError:
				#return False
			#except:
				#return False

	def delete_article_by_id(self, article_id):
		with self.engine.connect() as connection:
			print(article_id)
			try:
				query_string = text("DELETE FROM public.article WHERE id = :article_id")
				connection.execute(query_string.bindparams(article_id = article_id))
				connection.commit()

				return True
			except:
				return False

	def create_comment(self, payload):
		_id = uuid.uuid4()
		with self.engine.connect() as connection:
			try:
				query_string = text("INSERT INTO public.article_comment( id, article_id, body, user_id) VALUES (:id, :article_id, :body, :user_id)")
				connection.execute(query_string.bindparams(id = _id, article_id = payload.get('article_id'), body = payload.get('body'), user_id = self.user.id))
				connection.commit()

				created_comment = self._get_comment_by_id(_id)
				
				return True, created_comment
			except:
				return False, None

	def dalete_comment(self, comment_id):
		with self.engine.connect() as connection:
			#try:
				if self.user.is_admin():
					query_string = text("DELETE FROM public.article_comment WHERE id = :comment_id")
					connection.execute(query_string.bindparams(comment_id = comment_id))
				else:
					query_string = text("DELETE FROM public.article_comment WHERE id = :comment_id AND user_id = :user_id")
					connection.execute(query_string.bindparams(comment_id = comment_id, user_id = self.user.id))
				connection.commit()

				return True
			#except:
				#return False

	def update_comment(self, comment_id, payload):
		with self.engine.connect() as connection:
			try:
				current_time = datetime.now()
				query_string = text("UPDATE public.article_comment SET body=:body, updated_at=:updated_at WHERE id = :comment_id AND user_id = :user_id")
				connection.execute(query_string.bindparams(body = payload['body'], updated_at = current_time, comment_id = comment_id, user_id = self.user.id))
				connection.commit()

				# the comment that updated
				updated_comment = self._get_comment_by_id(comment_id)

				return True, updated_comment
			except:
				return False, None

	def get_comment_by_article(self, article_id):
		with self.engine.connect() as connection:
			try:
				# query_string = text("SELECT id, article_id, body, created_at, updated_at, user_id FROM public.article_comment WHERE article_id = :article_id")
				query_string = text("SELECT article_comment.id AS comment_id, article_comment.id as article_id, article_comment.body, article_comment.created_at AS comment_created, article_comment.updated_at as comment_updated, article_comment.user_id AS comment_user_id, public.user.id AS user_id, public.user.name, public.user.image_url, public.user.sysadmin, COALESCE( SUM( CASE WHEN vote.vote_type = 'upvote' THEN 1 WHEN vote.vote_type = 'downvote' THEN -1 ELSE 0 END ), 0 ) AS vote_score FROM public.article_comment INNER JOIN public.user ON article_comment.user_id = public.user.id LEFT JOIN public.vote ON article_comment.id = vote.target_id AND vote.target_type = 'comment' WHERE article_comment.article_id = :article_id GROUP BY article_comment.id, public.user.id ORDER BY article_comment.created_at ASC")
				query_results = connection.execute(query_string.bindparams(article_id = article_id)).mappings().all()
				result = []
				for comment in query_results:
					is_voted = False
					voted_type = None
					if hasattr(self, 'user'):
						r = self._is_already_voted(comment['article_id'], self.user.id)
						is_voted = r['is_voted']
						if 'voted_type' in r:
							voted_type = r['voted_type']
					result.append({
						'id': comment['comment_id'],
						'article_id': comment['article_id'],
						'body': comment['body'],
						'user_id': comment['user_id'],
						'user_name': comment['name'],
						'user_image_url': comment['image_url'],
						'is_admin': comment['sysadmin'],
						'created_at': comment['comment_created'].isoformat(),
						'updated_at': comment['comment_updated'].isoformat(),
						'vote': comment['vote_score'],
						'is_voted': is_voted,
						'voted_type': voted_type
					})

				return {'ok': True, 'message': 'success', 'result': result}
			except NoResultFound:
				return {'ok': True, 'message': 'no comment', 'result': []}
			except:
				return {'ok': False, 'message': 'failed'}

	def _is_already_voted(self, target_id, user_id):
	  with self.engine.connect() as connection:
	    try:
	      query_string = text("SELECT id, target_id, target_type, vote_type, user_id FROM public.vote WHERE target_id = :target_id AND user_id = :user_id")
	      result = connection.execute(query_string.bindparams(target_id=target_id, user_id=user_id)).mappings().one()

	      return {'is_voted': True, 'id': result['id'], 'target_id': result['target_id'], 'voted_type': result['vote_type'], 'user_id': result['user_id']}
	    except:
	    	return {'is_voted': False}

	def _get_comment_by_id(self, comment_id: str):
		with self.engine.connect() as connection:
			try:
				query_string = text("SELECT article_comment.id as comment_id, article_comment.body, article_comment.created_at, article_comment.updated_at, article_comment.user_id, public_user.id as user_id, public_user.name, public_user.image_url FROM public.article_comment AS article_comment INNER JOIN public.user AS public_user ON article_comment.user_id = public_user.id WHERE article_comment.id = :comment_id")
				query_result = connection.execute(query_string.bindparams(comment_id = str(comment_id))).mappings().one()
				result = {
					'id': query_result['comment_id'],
					'body': query_result['body'],
					'created_at': query_result['created_at'].isoformat(),
					'updated_at': query_result['updated_at'].isoformat(),
					'user_id': query_result['user_id'],
					'user_name': query_result['name'],
        			'user_image_url': query_result['image_url']
				}

				return result
			except NoResultFound:
				return []
			except:
				return  []