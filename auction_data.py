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


class Realm:
    def __init__(self):
        pass

    def update_db(self):
        """Updates Realm instance's row"""
        pass


class Item:
    def __init__(self):
        pass


class DataParser:
    """Parses json files provided by Blizard's Auction API into useful data
    for 'MyAH' (django webapp) and 'Multiboxer' (auction house addon).
    """
    def __init__(self):
        pass

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
    dp.update_all()
