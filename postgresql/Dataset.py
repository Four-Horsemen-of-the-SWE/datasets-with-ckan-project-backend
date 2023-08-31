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

    # is private dataset
    def is_private(self, dataset_id):
        try:
            with self.engine.connect() as connection:
                query_string = text("SELECT private FROM public.package WHERE id = :dataset_id")
                result = connection.execute(query_string.bindparams(dataset_id = dataset_id)).mappings().one()
                if result['private']:
                    return True
                else:
                    return False
        except:
            return None

    # is public dataset
    def is_public(self, dataset_id):
        try:
            with self.engine.connect() as connection:
                query_string = text("SELECT private FROM public.package WHERE id = :dataset_id")
                result = connection.execute(query_string.bindparams(dataset_id = dataset_id)).mappings().one()
                if not result['private']:
                    return True
                else:
                    return False
        except:
            return None

    # if dataset active
    def is_active(self, dataset_id):
        try:
            with self.engine.connect() as connection:
                query_string = text("SELECT id, name, state FROM public.package WHERE id = :dataset_id")
                result = connection.execute(query_string.bindparams(dataset_id = dataset_id)).mappings().one()
                if result['state'] == 'active':
                    return True
                else:
                    return False
        except:
            return None

    # change visibility
    def change_visibility(self, dataset_id, visibility):
        try:
            is_private = True if visibility == "private" else False
            with self.engine.connect() as connection:
                query_string = text("UPDATE public.package SET private=:visibility WHERE id=:dataset_id AND creator_user_id=:user_id")
                connection.execute(query_string.bindparams(visibility = is_private, dataset_id = dataset_id, user_id = self.user.id))
                connection.commit();

                return {'ok': True, 'message': f'dataset_id = {dataset_id} is now {"private" if is_private else "public"}'}
        except NoResultFound:
            return {'ok': False, 'message': 'failed'}

    # store a download statistic
    def collect_download_static(self, dataset_id: str = None, resource_id: str = None):
        try:
            with self.engine.connect() as connection:
                _id = uuid.uuid4()
                user_id = None
                if hasattr(self, 'user'):
                	user_id = self.user.id if hasattr(self.user, 'id') else None

                query_string = "INSERT INTO public.package_download_log(id, package_id, resource_id, user_id) VALUES ('%s', '%s', '%s', '%s')" % (_id, dataset_id, resource_id, user_id)
                connection.execute(text(query_string))
                connection.commit()

                # get the download statistic then return to client
                result = self.get_download_statistic(dataset_id = dataset_id)

                return {'ok': True, 'message': 'success', 'result': result['result'], 'total_download': result['total_download']}
        except:
            return {'ok': False, 'message': 'failed'}

    # get a download statistic from selected dataset_id.
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


    		return {'ok': True, 'message': 'success', 'result': download_result, 'total_download': download_total}

    # get number of active dataset (include private)
    def get_number_of_datasets(self):
        with self.engine.connect() as connection:
            query_string = text("SELECT COUNT(id) FROM public.package WHERE state = 'active'")
            result = connection.execute(query_string).mappings().one()
            return result['count']