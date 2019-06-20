import json

with open('settings.json') as f:
    settings = json.load(f)

def set_setting(field):
    return settings['settings'].get(field, None) or settings['default_settings'][field]

# Blizz API
CLIENT_ID = set_setting('client_id')
CLIENT_SECRET = set_setting('client_secret')

# Databases
REALMS = set_setting('realm_db')
ITEMS = set_setting('item_db')
CURRENT_DATA = set_setting('current_data')
HISTORICAL_DATA = set_setting('historical_data')

# Paths
TEMP_FOLDER = set_setting('temp_folder')
LUA_PATH = set_setting('lua_path')

