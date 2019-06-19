import datetime
import json
import os
import pickle
import requests
import sqlite3
import time
from multiprocessing import Process, Queue

from slpp import slpp as lua
from wowapi import WowApi as wowapi

from settings import *


class Realm:
    def __init__(self, row):
        self.name = row[0]
        self.slug = row[1]
        self.code = row[2]
        self.update_interval = row[3]
        self.last_update = row[4]
        self.last_check = row[5]
        self.json_link = row[6]
        self.seller = row[7]
        self.region = row[8]
        self.position = row[9]
        self.account = row[10]

    def update_db(self):
        """Updates Realm instance's row"""
        conn = sqlite3.connect(REALM_DB)
        c = conn.cursor()
        c.execute("""UPDATE realms SET last_update = ?, last_checked = ?
                WHERE name = ? AND (last_Update != ? OR last_update IS NULL)""",
                (self.last_update, self.last_check, self.name, self.last_update))
        conn.commit()


class Item:
    def __init__(self, row):
        self.item_id = row[0]
        self.name = row[1]
        self.short_name = row[2]
        self.category_id = row[3]
        self.position = row[4]
        self.stack_sizes = tuple(row[5])


class DataParser:
    """Parses json files provided by Blizard's Auction API into useful data
    for 'MyAH' (django webapp) and 'Multiboxer' (auction house addon).
    """
    def __init__(self):
        def load_last_session():
            try:
                with open(f"{TEMP_FOLDER}/_serialized_data.pickle", 'rb') as old:
                    return pickle.load(old)
            except:
                return {}

        def realm_objects_dict():
            conn = sqlite3.connect(REALM_DB)
            c = conn.cursor()
            c.execute("SELECT * FROM realms")

            realms = {}
            for row in c.fetchall():
                realms[row[0]] = Realm(row)
            conn.close()
            return realms

        def item_objects_dict():
            conn = sqlite3.connect(ITEM_DB)
            c = conn.cursor()
            c.execute("SELECT * FROM items")

            items = {}
            for row in c.fetchall():
                c.execute("SELECT stack_size FROM stack_sizes WHERE category_id = ?",
                        (row[3], ))
                stack_sizes = [x[0] for x in c.fetchall()]
                items[row[1]] = row + (stack_sizes, )
            return items

        self.wowapi = wowapi(CLIENT_ID, CLIENT_SECRET)
        self.parsed_data = load_last_session()
        self.realms = realm_objects_dict()
        self.items = item_objects_dict()

    def update_all(self, force_update=False):
        """Concurently update all realms with old data.\n
        Set 'force_update=True' to bypass update constraints. This will
        update all realms, even those that are already up to date!
        """
        pass

    def update_loop(self):
        """Infinite update loop. Not concurent."""
        pass

    def update_realm(self, realm, queue=None):
        """Fetches the latest API json dump and parses it.\n
        Multiprocessing Queue is None by default.
        """
        pass

    def check_for_update(self, realm):
        """Checks for new json dump.\n
        Updates Realm instance's 'last_update' field but not in the source file.
        """
        pass

    def write_output(self):
        """Updates model with up to date parsed data.\n
        Serializes parsed data in memory for later use.\n
        Encodes data in Lua Table format for Multiboxer (WoW addon).
        """
        pass


if __name__ == '__main__':
    dp = DataParser()
    print(dp.parsed_data)
