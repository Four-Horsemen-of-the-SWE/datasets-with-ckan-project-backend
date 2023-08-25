import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from dotenv import load_dotenv
from passlib.hash import pbkdf2_sha512
import jwt
from .User import User
import json
# load env file
load_dotenv()

class Admin(User):
    def __init__(self, jwt_token: str = None):
        super().__init__()
        if jwt_token is not None:
            self.user = User(jwt_token=jwt_token)

    def get_all_admin(self):
    	if self.is_admin:
    		#try:
    			with self.engine.connect() as connection:
    				query_string = text("SELECT id, name, fullname, email, sysadmin, image_url FROM public.user WHERE sysadmin = :is_admin")
    				query_result = connection.execute(query_string.bindparams(is_admin = True)).mappings().all()
    				result = []
    				for row in query_result:
    					result.append({
    						'id': row['id'],
    						'name': row['name'],
    						'fullname': row['fullname'],
    						'email': row['email'],
    						'sysadmin': row['sysadmin'],
    						'image_url': row['image_url']
    					})
    				return {'ok': True, 'message': 'success.', 'result': result}
    		#except:
    			#return []

    def change_role(self, user_id: str = None, role: str = None):
        if user_id is None:
            return {'ok': False, 'message': 'user_id is not provided.'}

        is_admin = True if role == "admin" else False

        # if current user is admin, then can make another user an admin
        if self.is_admin:
            with self.engine.connect() as connection:
                query_string = text("UPDATE public.user SET sysadmin=:is_admin WHERE id = :user_id")
                connection.execute(query_string.bindparams(is_admin=is_admin, user_id=user_id))
                connection.commit()
            return {'ok': True, 'message': 'success'}
        else:
            return {'ok': False, 'message': 'current user is not admin.'}
