import os
from flask import Flask
from flask_cors import CORS
from routes.users import users_route
from routes.datasets import datasets_route
from routes.tags import tags_route
from routes.organizations import organizations_route
from routes.discussion import discussion_route
from routes.groups import groups_route
from routes.licenses import licenses_route
from routes.votes import votes_route
from routes.articles import article_route
from routes.reports import reports_route
from dotenv import load_dotenv

# load .env file
load_dotenv()
API_HOST = os.getenv('API_HOST')
API_ENDPOINT = os.getenv('API_ENDPOINT')

# Cors
config = {
  'ORIGINS': [
    'http://localhost:3000',  # React
    'http://127.0.0.1:3000',  # React
  ]
}

# create a flask app
app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
CORS(app, resources={r"/*": {"origins": "*"}}, expose_headers='Authorization')

# return to client when they entering 127.0.0.1:5001.
@app.route('/')
def hello():
  return {'ok': True, 'message': 'Hello there.'}, 200

# register the blueprints
app.register_blueprint(users_route, url_prefix=f'{API_ENDPOINT}/users')
app.register_blueprint(datasets_route, url_prefix=f'{API_ENDPOINT}/datasets')
app.register_blueprint(tags_route, url_prefix=f'{API_ENDPOINT}/tags')
app.register_blueprint(organizations_route, url_prefix=f'{API_ENDPOINT}/organizations')
app.register_blueprint(discussion_route, url_prefix=f'{API_ENDPOINT}/discussions')
app.register_blueprint(groups_route, url_prefix=f'{API_ENDPOINT}/groups')
app.register_blueprint(licenses_route, url_prefix=f'{API_ENDPOINT}/licenses')
app.register_blueprint(votes_route, url_prefix=f'{API_ENDPOINT}/votes')
app.register_blueprint(article_route, url_prefix=f'{API_ENDPOINT}/articles')
app.register_blueprint(reports_route, url_prefix=f'{API_ENDPOINT}/reports')

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5001, debug=True)