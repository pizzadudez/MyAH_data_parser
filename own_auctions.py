import os
from slpp import slpp as lua

LUA_FILES = [
    "C:\\_games\\WoW\\wow_0\\_retail_\\WTF\\Account\\461526792#1\\SavedVariables\\Multiboxer_Data.lua",
    "C:\\_games\\WoW\\wow_5\\_retail_\\WTF\\Account\\461537559#1\\SavedVariables\\Multiboxer_Data.lua"
]
STRING_TO_REMOVE = "Multiboxer_DataDB = "


class OwnAuctions:
    """self.ids = my_auctions_hashtable['realm_name']['auc_id']"""

    def __init__(self):
        def table_to_dict(path):
            """Decodes lua SavedVariables table as dict."""
            if not os.path.exists(path):
                return {}
            data = {}
            with open(path, encoding="utf-8") as file:
                if file:
                    data = lua.decode(
                        file.read().replace(STRING_TO_REMOVE, ''))
                    return data

        ids = {}
        for path in LUA_FILES:
            data = table_to_dict(path)
            for realm_name, realm in data['realmData'].items():
                for char_auctions in realm.get('auctionIds', {}).values():
                    ids[realm_name] = ids.get(realm_name, {})
                    for auc_id in char_auctions:
                        ids[realm_name][auc_id] = True

        self.ids = ids


if __name__ == '__main__':
    oa = OwnAuctions()
    print(oa.ids)
