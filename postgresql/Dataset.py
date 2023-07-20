import os
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from .core.PostgreSQL import PostgreSQL
from .User import User

class Dataset(PostgreSQL):
    def __init__(self, jwt_token: str = None):
        super().__init__()
        if jwt_token is not None:
            self.user = User(jwt_token=jwt_token)

    # store a download statistic
    def collect_download_static(self, dataset_id: str = None, resource_id: str = None):
        with self.engine.connect() as connection:
            user_id = None
            if hasattr(self, 'user'):
            	user_id = self.user.id if hasattr(self.user, 'id') else None

            query_string = "INSERT INTO public.package_download_log(id, package_id, resource_id, user_id) VALUES ('%s', '%s', '%s', '%s')" % (uuid.uuid4(), dataset_id, resource_id, user_id)
            connection.execute(text(query_string))
            connection.commit()
            return True