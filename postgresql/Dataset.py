import os
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from .core.PostgreSQL import PostgreSQL
from .User import User
from datetime import datetime

class Dataset(PostgreSQL):
    def __init__(self, jwt_token: str = None):
        super().__init__()
        if jwt_token is not None:
            self.user = User(jwt_token=jwt_token)

    # change visibility
    def change_visibility(self, dataset_id, visibility):
        try:
            is_private = True if visibility == "private" else False
            with self.engine.connect() as connection:
                query_string = text("UPDATE public.package SET private=:visibility WHERE id=:dataset_id AND creator_user_id=:user_id")
                connection.execute(query_string.bindparams(visibility = is_private, dataset_id = dataset_id, user_id = self.user.id))
                connection.commit();

                return {'ok': True, 'message': f'dataset_id = dataset_id is now {"private" if is_private else "public"}'}
        except:
            return {'ok': False, 'message': 'failed'}

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

    # get a download statistic from selected dataset_id.
    # it's return date by date
    def get_download_statistic(self, dataset_id: str = None):
    	if dataset_id is None:
    		return False

    	with self.engine.connect() as connection:
    		query_string = "SELECT DATE(download_date) AS download_date, COUNT(id) AS download_count FROM public.package_download_log WHERE package_id = '%s' GROUP BY DATE(download_date) ORDER BY download_date ASC" % dataset_id
    		result = connection.execute(text(query_string)).mappings().all()
    		download_result = []
    		download_total = 0

    		for item in result:
    			download_total += item['download_count']
    			download_result.append({
    				'download_date': item['download_date'].isoformat(),
    				'download_count': item['download_count']
    			})


    		return {'ok': True, 'result': download_result, 'total_download': download_total}

