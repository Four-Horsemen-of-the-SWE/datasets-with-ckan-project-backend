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

  def get_all_report(self):
  	with self.engine.connect() as connection:
  		sql_query = text("SELECT id, topic, description, entity_id, entity_type, user_id, created_at, status FROM public.report")