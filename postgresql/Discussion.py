import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from passlib.hash import pbkdf2_sha512
import uuid
from bson import json_util
from .User import User
import json
from datetime import datetime
# load env file
load_dotenv()

class Discussion(User):
  def __init__(self, jwt_token:str = None, payload:dict = None):
    super(Discussion, self).__init__(jwt_token=jwt_token)
    self.payload = payload

  def create_topic(self, package_id):
    try:
      unique_id = uuid.uuid4()
      with self.engine.connect() as connection:
        query_string = "INSERT INTO public.topic (id, package_id, title, body, user_id) VALUES (:unique_id, :package_id, :title, :body, :id)"
        connection.execute(text(query_string), {
            'unique_id': unique_id,
            'package_id': package_id,
            'title': self.payload['title'],
            'body': self.payload['body'],
            'id': self.id
          }
        )
        # อย่าลืม commit ไม่งั้นมันไม่เซฟ
        connection.commit()

        result = self._get_topic(topic_id = unique_id)

        return {'ok': True, 'message': 'success', 'result': result}
    except:
      return {'ok': False, 'message': 'backend error'}

  def _get_topic(self, topic_id:str = None):
    try:
      with self.engine.connect() as connection:
        query_string = "SELECT topic.id as topic_id, topic.package_id, topic.title, topic.body, topic.created, topic.user_id as user_id, public.user.name, public.user.image_url FROM public.topic INNER JOIN public.user ON topic.user_id = public.user.id WHERE topic.id = '%s'" % topic_id
        result = connection.execute(text(query_string)).mappings().one()
        return {
          'id': result['topic_id'],
          'package_id': result['package_id'],
          'title': result['title'],
          'body': result['body'],
          'created': result['created'].isoformat(),
          'user_id': result['user_id'],
          'user_name': result['name'],
          'user_image_url': result['image_url']
        }
    except:
      return None
  
  def get_topic(self, package_id:str = None):
    if package_id is None:
      return {'ok': False, 'message': 'cannot fetch topics'}
    with self.engine.connect() as connection:
      query_string = "SELECT topic.id, topic.package_id, topic.title, topic.body, topic.created, topic.user_id, public.user.name, public.user.image_url FROM public.topic INNER JOIN public.user ON topic.user_id = public.user.id WHERE topic.package_id = '%s' ORDER BY topic.created ASC" % package_id
      results = connection.execute(text(query_string)).mappings().all()
      response = []
      for topic in results:
        response.append({
          'id': topic['id'],
          'package_id': topic['package_id'],
          'title': topic['title'],
          'body': topic['body'],
          'created': topic['created'].isoformat(),
          'user_id': topic['user_id'],
          'user_name': topic['name'],
          'user_image_url': topic['image_url']
        })
      return response

  def _get_comment(self, comment_id: str = None):
    if comment_id is None:
      return False

    with self.engine.connect() as connection:
      query_string = "SELECT comment.id as comment_id, comment.body, comment.created, comment.user_id, public.user.id as user_id, public.user.name, public.user.image_url FROM public.comment INNER JOIN public.user ON public.comment.user_id = public.user.id WHERE comment.id = '%s'" % comment_id
      result = connection.execute(text(query_string)).mappings().one()
      created_comment = {
        'id': result['comment_id'],
        'body': result['body'],
        'created': result['created'].isoformat(),
        'user_id': result['user_id'],
        'user_name': result['name'],
        'user_image_url': result['image_url']
      }
      return created_comment

  def create_comment(self, topic_id: str = None, payload: dict = None):
      try:
        comment_id = str(uuid.uuid4())
        if topic_id is None:
            return {'ok': False, 'message': 'cannot fetch topics'}

        with self.engine.connect() as connection:
            query_string = "INSERT INTO public.comment (id, topic_id, body, user_id) VALUES (:comment_id, :topic_id, :body, :user_id)"
            connection.execute(
                text(query_string), {
                  'comment_id': comment_id,
                  'topic_id': topic_id,
                  'body': payload['body'],
                  'user_id': self.id
                }
            )
            connection.commit()

            # get comment that was created
            created_comment = self._get_comment(comment_id=comment_id)

            return {'ok': True, 'message': 'success', 'result': created_comment}
      except:
          return {'ok': False, 'message': 'create failed'}


  def update_comment(self, comment_id:str = None, payload: dict = None):
    try:
      if comment_id is None:
        return {'ok': False, 'message': 'cannot fetch topics'}
      with self.engine.connect() as connection:
        query_string = text("UPDATE public.comment SET body=:body WHERE id=:comment_id")
        connection.execute(query_string.bindparams(body=payload['body'], comment_id=comment_id))
        connection.commit()

        # get comment that created
        update_comment = self._get_comment(comment_id = comment_id)

        return {'ok': True, 'message': 'success', 'result': update_comment}
    except:
      return {'ok': False, 'message': 'update failed'}

  def delete_comment(self, comment_id:str = None):
    try:
      if comment_id is None:
        return {'ok': False, 'message': 'cannot fetch topics'}

      with self.engine.connect() as connection:
        query_string = text("DELETE FROM public.comment WHERE id = :id AND user_id = :user_id")
        connection.execute(query_string.bindparams(id = comment_id, user_id = self.id))
        connection.commit()

        return {'ok': True, 'message': 'success', 'result': comment_id}
    except:
        return {'ok': False, 'message': 'delete failed'}   
        
  def delete_topic(self, topic_id: str = None):
    if topic_id is None:
      return {'ok': False, 'message': 'cannot delete topic'}

    with self.engine.connect() as connection:
      query_string = text("DELETE FROM public.topic WHERE id = :id AND user_id = :user_id")
      connection.execute(query_string.bindparams(id = topic_id, user_id = self.id))
      connection.commit()

      return {'ok': True, 'message': 'success', 'result': topic_id}

  def get_topic_and_comments(self, topic_id:str = None):
    if topic_id is None:
      return 'cannot fetch topics'
    try:
      with self.engine.connect() as connection:
        result = None
        # get topic details
        topic_query_string = "SELECT topic.id, topic.package_id, topic.title, topic.body, topic.created, topic.user_id, public.user.name, public.user.image_url FROM public.topic INNER JOIN public.user ON topic.user_id = public.user.id WHERE topic.id = '%s'" % topic_id
        topic_query_result = connection.execute(text(topic_query_string)).mappings().one()
        result = {
            'id': topic_query_result['id'],
            'package_id': topic_query_result['package_id'],
            'title': topic_query_result['title'],
            'body': topic_query_result['body'],
            'created': topic_query_result['created'].isoformat(),
            'user_id': topic_query_result['user_id'],
            'user_name': topic_query_result['name'],
            'user_image_url': topic_query_result['image_url'],
            'comments': [],
            'comments_count': 0
        }

        # get topic's comments
        comment_query_string = "SELECT comment.id as comment_id, comment.body, comment.created, comment.user_id, public.user.id as user_id, public.user.name, public.user.image_url FROM public.comment INNER JOIN public.user ON public.comment.user_id = public.user.id WHERE comment.topic_id = '%s' ORDER BY comment.created ASC" % topic_id
        comment_query_result = connection.execute(text(comment_query_string)).mappings().all()
        for comment in comment_query_result:
            result['comments'].append({
                'id': comment['comment_id'],
                'body': comment['body'],
                'created': comment['created'].isoformat(),
                'user_id': comment['user_id'],
                'user_name': comment['name'],
                'user_image_url': comment['image_url']
            })

        result['comments_count'] = len(comment_query_result)

        return result
    except:
      return 'error'