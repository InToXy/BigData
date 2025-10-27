import os

# Superset specific config
ROW_LIMIT = 5000

# Flask App Builder configuration
# Your App secret key will be used for securely signing the session cookie
SECRET_KEY = 'thisISaSECRET_1234567890abcdefghijklmnopqrstuvwxyz'

# The SQLAlchemy connection string to your database backend
# This connection defines the path to the database that stores your
# superset metadata (slices, connections, tables, dashboards, ...).
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset123@superset-db:5432/superset'

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = False
WTF_CSRF_EXEMPT_LIST = ['.*']
WTF_CSRF_TIME_LIMIT = None

# Disable Talisman
TALISMAN_ENABLED = False

# Set the authentication type to DATABASE
AUTH_TYPE = 1

# Uncomment to setup Setup default views
SUPERSET_WEBSERVER_TIMEOUT = 300
