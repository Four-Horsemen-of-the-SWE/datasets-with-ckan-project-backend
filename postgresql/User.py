import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm.exc import NoResultFound
from dotenv import load_dotenv
from passlib.hash import pbkdf2_sha512
import jwt
# load env file
load_dotenv()

from .core.PostgreSQL import PostgreSQL

class User(PostgreSQL):
	last_active: str
	secret:str =  os.getenv('SECRET')
	token_secret:str = os.getenv('TOKEN_SECRET')

	def __init__(self, jwt_token: str = None):
		super().__init__()
		self.id: str
		self.name: str
		self.api_key: str
		self.api_token: str
		self.create: str
		self.about: str
		self.fullname: str
		self.email: str
		self.sysadmin: bool
		self.state: str
		self.image: str
		self.jwt_token: str
		if jwt_token is not None:
			self._verify_token(jwt_token)

	def _verify_password(self, password, pasword_hashed):
		if pbkdf2_sha512.verify(password, pasword_hashed):
			return True
		else:
			return False
		
	def _verify_token(self, token:str = None):
		# if success set the user id
		try:
			result = jwt.decode(token, self.secret, algorithms=["HS256"])
			if result:
				self.jwt_token = token
				self.id = result['id']
				# get a api_token from database
				self._get_api_token()
				return True
			else:
				return False
		except jwt.exceptions.DecodeError:
			return False

	# check incoming password with in database
	def valid_password(self, password):
		with self.engine.connect() as connection:
			try:
				# fist get a password in database with user_id
				query_string = text("SELECT id, password FROM public.user WHERE id = :user_id")
				result = connection.execute(query_string.bindparams(user_id = self.id)).mappings().one()
				if self._verify_password(password, result['password']):
					return True
				else:
					return False
			except:
				return False


	def _check_authorize(self, token):
		self._get_api_token(token)

	def _get_api_token(self):
		with self.engine.connect() as connection:
			query_string = "SELECT id, name FROM public.api_token WHERE user_id = '%s' AND name='%s'" % (self.id, 'ckan_private_api_token')
			try:
				result = connection.execute(text(query_string)).one()
				token = jwt.encode({"jti": result[0], "iat": 1679160636}, self.token_secret, algorithm="HS256")
				self.api_token = token
				return token	
			except:
				return None

	def login(self, name, password):
		with self.engine.connect() as connection:
			query_string = "SELECT id, name, apikey, created, about, password, fullname, email, reset_key, sysadmin, activity_streams_email_notifications, state, plugin_extras, image_url, last_active FROM public.user WHERE name = '%s'" % name
			try:

				# query the user
				result = connection.execute(text(query_string)).one()
				# then check a name and hashed passwors
				# result[0] = id
				# result[1] = name
				# result[5] = password

				if result[1] == name and self._verify_password(password, result[5]):
					# set user details
					self.id = result[0]
					self.name = result[1]
					self.fullname = result[6]
					# generate a jwt token or something
					token = jwt.encode({"id": self.id, "name": self.name, "fullname": self.fullname}, self.secret, algorithm="HS256")
					# then, return token and user detail to client
					return {
						'accessToken': token,
						'user': self.get_user_details(token)
					}
			except NoResultFound:
				return False

	def get_user_details(self, token):
		if self._verify_token(token):
			with self.engine.connect() as connection:
				query_string = "SELECT id, name, apikey, created, about, password, fullname, email, sysadmin, activity_streams_email_notifications, state, image_url, last_active FROM public.user WHERE id = '%s'" % self.id
				# query the user details
				result = connection.execute(text(query_string)).one()
				if result is not None:
					result_as_dict = {
						'id': result[0],
						'name': result[1],
						'apikey': result[2],
						'created': result[3],
						'about': result[4],
						'fullname': result[6],
						'email': result[7],
						'image_url': result[11],
						'is_admin': result[8]
					}
					return result_as_dict

	def is_admin(self):
		user_details = self.get_user_details(self.jwt_token)
		return user_details['is_admin'] == True

	# get user name by id
	def get_user_name(self, user_id: str = None):
		if user_id is None:
			return False

		with self.engine.connect() as connection:
			query_string = "SELECT name FROM public.user WHERE id = '%s'" % user_id
			result = connection.execute(text(query_string)).mappings().one()
			return result['name']
			
	# check if username exists
	def _check_if_username_exists(self, username):
		try:
			with self.engine.connect() as connection:
				query_string = text("SELECT name FROM public.user WHERE name = :username")
				result = connection.execute(query_string.bindparams(username = username)).mappings().one()
				if result:
					return True
				else:
					return False
		except:
			return None
  
  # get change username
	def change_username(self, new_username):
		try:
			if self._check_if_username_exists(new_username):
				return None
			with self.engine.connect() as connection:
				query_string = text("UPDATE public.user SET name=:new_username WHERE id = :user_id")
				connection.execute(query_string.bindparams(user_id = self.id, new_username = new_username))
				connection.commit()

				return True
		except:
			return False