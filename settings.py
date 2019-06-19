import json

with open('settings.json') as f:
    settings = json.load(f)

def set_setting(field):
    return settings['settings'].get(field, None) or settings['default_settings'][field]

# Blizz API
CLIENT_ID = set_setting('client_id')
CLIENT_SECRET = set_setting('client_secret')

# Paths
REALM_DB = set_setting('realm_db')
ITEM_DB = set_setting('item_db')
AUCTION_DB = set_setting('auction_db')
TEMP_FOLDER = set_setting('temp_folder')
LUA_PATH = set_setting('lua_path')

