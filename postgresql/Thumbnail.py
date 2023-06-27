import os
from sqlalchemy import text
from dotenv import load_dotenv
from flask import request, redirect, url_for
from werkzeug.utils import secure_filename
from .User import User
import json
from datetime import datetime
import psycopg2
import base64
import uuid
# load env file
load_dotenv()

class Thumbnail(User):
  ALLOWED_EXTENSIONS:set = set(['png', 'jpg', 'jpeg', 'gif'])
  def __init__(self, jwt_token:str = None):
    super(Thumbnail, self).__init__(jwt_token=jwt_token)

  def _check_authorization(self, package_id:str = None):
    # check who create a package
    with self.engine.connect() as connection:
      package_query_string = "SELECT id, creator_user_id FROM public.package WHERE id = '%s'" % package_id
      package_result = connection.execute(text(package_query_string)).mappings().one()
      # add admin checking here
      if package_result['creator_user_id'] == self.id or self.is_admin():
        return True
      else:
        return False

  def _check_thumbnail_exist(self, package_id:str = None):
    with self.engine.connect() as connection:
      query_string = "SELECT id, package_id, created, image_data FROM public.package_thumbnail WHERE package_id = '%s'" % package_id
      result = connection.execute(text(query_string)).mappings().all()
      print(result)
      if(len(result)):
        return True
      else:
        return False

  # update thumbnail
  def update_thumbnail(self, package_id:str = None, image:any = None):
    if self._check_authorization(package_id):
      image_bytes = base64.b64encode(image)
      with self.engine.connect() as connection:
          # now store into databse
          query_string = text("UPDATE public.package_thumbnail SET image_data=:image_bytes WHERE package_id = :package_id")
          # query_string = text("INSERT INTO public.package_thumbnail(id, package_id, image_data) VALUES (:id, :package_id, :image_bytes)")

          connection.execute(query_string.bindparams(package_id=package_id, image_bytes=image_bytes))
          connection.commit()
          return {'ok': True, 'message': 'update success'}

  # i coded that u can use create_thumbnail to update and create thumbnail, this useful if you not sure about your thumbnail @jcsnp
  def create_thumbnail(self, package_id:str = None, image:any = None):
    if self._check_authorization(package_id):
      image_bytes = base64.b64encode(image)
      if self._check_thumbnail_exist(package_id):
        with self.engine.connect() as connection:
          # now store into databse
          query_string = text("UPDATE public.package_thumbnail SET image_data=:image_bytes WHERE package_id = :package_id")
          # query_string = text("INSERT INTO public.package_thumbnail(id, package_id, image_data) VALUES (:id, :package_id, :image_bytes)")

          connection.execute(query_string.bindparams(package_id=package_id, image_bytes=image_bytes))
          connection.commit()
          return {'ok': True, 'message': 'update success'}
      else:
        with self.engine.connect() as connection:
          # now store into databse
          query_string = text("INSERT INTO public.package_thumbnail(id, package_id, image_data) VALUES (:id, :package_id, :image_bytes)")

          connection.execute(query_string.bindparams(id=uuid.uuid4(), package_id=package_id, image_bytes=image_bytes))
          connection.commit()
          return {'ok': True, 'message': 'create success'}

  @staticmethod
  def get_thumbnail(package_id:str = None):
    with Thumbnail.engine.connect() as connection:
      # get image data from database
      try:
        query_string = "SELECT id, package_id, created, image_data FROM public.package_thumbnail WHERE package_id = '%s'" % package_id
        result = connection.execute(text(query_string)).mappings().one()
        image = (result['image_data']).tobytes().decode('utf-8')
        return {'ok': True, 'result': image}
      except:
        return {'ok': False, 'result': None}

"""
  def get_thumbnail(self, package_id:str = None):
    with self.engine.connect() as connection:
      # get image data from database
      try:
        query_string = "SELECT id, package_id, created, image_data FROM public.package_thumbnail WHERE package_id = '%s'" % package_id
        result = connection.execute(text(query_string)).mappings().one()
        image = (result['image_data']).tobytes().decode('utf-8')
        return {'ok': True, 'result': image}
      except:
        return {'ok': False}
"""
