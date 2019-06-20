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
        self.name = row[1]
        self.slug = row[2]
        self.code = row[3]
        self.update_interval = row[4]
        self.last_update = row[5]
        self.last_check = row[6]
        self.json_link = row[7]
        
        # Seller Hashmap
        self.sellers = {}
        for seller in row[11]:
            self.sellers[seller] = True;

    def __str__(self):
        return self.name

    def update_db(self):
        """Updates Realm instance's row"""
        conn = sqlite3.connect(REALMS)
        c = conn.cursor()
        c.execute("""UPDATE realms SET last_update = ?, last_check = ?
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

    def __str__(self):
        return self.name


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
            conn = sqlite3.connect(REALMS)
            c = conn.cursor()
            c.execute("SELECT * FROM realms")

            realms = {}
            for row in c.fetchall():
                c.execute("SELECT full_name FROM sellers WHERE realm_id = ?",
                        (row[0], ))
                sellers = [x[0] for x in c.fetchall()]
                realms[row[1]] = Realm(row + (sellers, ))
            conn.close()
            return realms

        def item_objects_dict():
            conn = sqlite3.connect(ITEMS)
            c = conn.cursor()
            c.execute("SELECT * FROM items")

            items = {}
            for row in c.fetchall():
                c.execute("SELECT stack_size FROM stack_sizes WHERE category_id = ?",
                        (row[3], ))
                stack_sizes = [x[0] for x in c.fetchall()]
                items[row[1]] = Item(row + (stack_sizes, ))
            return items
        
        def create_output_databases():
            """Creates output dbs tables if they don't exist."""

            conn = sqlite3.connect(CURRENT_DATA)
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS auction_chunks (
                chunk_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                realm TEXT,
                item_id INTEGER,
                quantity INTEGER,
                price INTEGER,
                stack_size INTEGER,
                owner TEXT,
                time_left TEXT)""")
            conn.close()

            conn = sqlite3.connect(HISTORICAL_DATA)
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                    realm TEXT,
                    timestamp INTEGER)""")
            c.execute("""CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                    first_seen INTEGER,
                    item_id INTEGER,
                    owner TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS snapshot_chunks_events (
                    snapshot_id INTEGER,
                    chunk_id INTEGER,
                    event TEXT,
                    quantity TEXT,
                    FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id),
                    FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id),
                    PRIMARY KEY (snapshot_id, chunk_id))""")
            c.execute("""CREATE TABLE IF NOT EXISTS auctions (
                    auc_id INTEGER NOT NULL,
                    chunk_id INTEGER,
                    realm TEXT,
                    FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id),
                    PRIMARY KEY (auc_id, chunk_id))""")
            conn.close()

        # Initialization starts here
        create_output_databases()
        self.wowapi = wowapi(CLIENT_ID, CLIENT_SECRET)
        self.parsed_data = load_last_session()
        self.realms = realm_objects_dict()
        self.items = item_objects_dict()

    def update_all(self, force_update=False):
        """Concurently update all realms with old data.\n
        Set 'force_update=True' to bypass update constraints. This will
        update all realms, even those that are already up to date!
        """
        processes = []
        queue = Queue() # process return values go here

        # Start multiprocessing
        print(">> Starting concurent update...")
        for realm in self.realms.values():
            if self.check_for_update(realm) or force_update:
                process = Process(target=self.update_realm, args=(realm, queue))
                processes.append(process)
                process.start()
        # Join processes
        for process in processes:
            process.join()

        # Deserialize data from finished worker processes
        updated_realms = [] # Realm list for output writing
        while not queue.empty():
            realm = self.realms[queue.get()]  
            # Load subprocess data
            with open(f"{TEMP_FOLDER}/{realm.slug}.pickle", 'rb') as file:
                self.parsed_data[realm.name] = pickle.load(file)
            realm.update_db() # update Realm's db record
            updated_realms.append(realm)

        self.write_output(updated_realms)
        print("\nFinished concurent update!")

    def update_loop(self):
        """Infinite update loop. No concurrent updates."""
        # List of Realm objects, sorted by their expected next update timestamp
        update_queue = [x for x in self.realms.values()]
        update_queue.sort(key=lambda x: x.last_update + x.update_interval)

        print("\n>> Starting update loop...")
        while True:
            sleep_ammount = None
            next_update = None
            for realm in update_queue:
                time_delta = realm.last_update + realm.update_interval - round(time.time())
                # We only update 3 seconds after the anticipated next_update
                # to avoid late updates from blizz and time.time() rounding 
                if time_delta >= -3:
                    sleep_ammount = time_delta + 4
                    next_update = realm
                    break
                elif time_delta < -3 and self.check_for_update(realm):
                    self.parsed_data[realm.name] = self.update_realm(realm)
                    self.write_output([realm, ])
                    realm.update_db() # everything went well, update Realm's db record
                    break # Should this break even be here???
                else:
                    pass
                    # TODO: do something with realms that dont have new data even if they should

            # Resort the queue after breaking the loop
            update_queue.sort(key=lambda x: x.last_update + x.update_interval)

            if sleep_ammount:
                sleep_time = time.strftime("%M:%S", time.gmtime(sleep_ammount))
                print(f"\n> Next update: {next_update.name} in {sleep_time}.")
                time.sleep(sleep_ammount)

    def update_realm(self, realm, queue=None):
        """Fetches the latest API json dump and parses it.\n
        Multiprocessing Queue is None by default.
        """
        print(f"{realm.name} updating...")
        conn = sqlite3.connect(f"{TEMP_FOLDER}/{realm.slug}.sqlite3")
        c = conn.cursor()
        json_data = requests.get(realm.json_link).json()
        # TODO: catch errors here

        # Create or truncate table then dump json content in it
        c.execute("""CREATE TABLE IF NOT EXISTS auctions (
                auc_id INTEGER,
                item_id INTEGER,
                owner TEXT,
                buyout INTEGER,
                stack_size INTEGER,
                time_left TEXT)""")
        c.execute("DELETE FROM auctions")
        for row in json_data['auctions']:
            owner_and_realm = "-".join([row['owner'], row['ownerRealm'].replace(' ', '')])
            c.execute("""INSERT INTO auctions (auc_id, item_id, owner, buyout, stack_size, time_left)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (row['auc'], row['item'], owner_and_realm, row['buyout'], row['quantity'], row['timeLeft']))
        conn.commit()

        # Cluster relevant auctions into chunks based on (price, stack_size, owner, time_left)
        auction_chunks = []
        sellers_auction_chunks = []
        for item in self.items.values():
            # Find all auction Chunks
            c.execute(f"""SELECT DISTINCT buyout, stack_size, owner, time_left FROM auctions
                    WHERE item_id=? AND stack_size IN {item.stack_sizes}
                    ORDER BY (buyout/stack_size) ASC""", (item.item_id, ))
            # Loop through auction chunks found
            for chunk in c.fetchall():
                buyout = chunk[0]
                stack_size = chunk[1]
                owner = chunk[2]
                time_left = chunk[3]
                price = round(buyout / stack_size) / 10000

                chunk_data = {
                    'item_id': item.item_id,
                    'price': price, 
                    'stack_size': stack_size,
                    'owner': owner, 
                    'time_left': time_left,}

                c.execute("SELECT auc_id FROM auctions WHERE item_id=? AND buyout=? AND stack_size=? AND owner=? AND time_left=?",
                        (item.item_id, buyout, stack_size, owner, time_left))

                # Store auc_ids only for tracked sellers
                if realm.sellers[owner]:
                    chunk_data['auc_ids'] = [x[0] for x in c.fetchall()]
                    chunk_data['quantity'] = len(chunk_data['auc_ids'])
                    sellers_auction_chunks.append(chunk_data)
                else:
                    chunk_data['quantity'] = len(c.fetchall())
                auction_chunks.append(chunk_data)

        print(f"> Finished updating: {realm.name}")

        parsed_data = (auction_chunks, sellers_auction_chunks)
        if queue:
            # If subprocess: serialize data and add realm name to queue
            with open(f"{TEMP_FOLDER}/{realm.slug}.pickle", 'wb') as file:
                pickle.dump(parsed_data, file)
            queue.put(realm.name)
        else:
            return parsed_data

    def check_for_update(self, realm):
        """Checks for new json dump and returns True if an update is needed.\n
        Updates Realm instance's 'last_update' field but not in the source file.
        """
        headers = self.wowapi.get_auctions('eu', realm.slug, locale='en_US')['files'][0]
        last_update = headers['lastModified'] // 1000
        realm.last_check = round(time.time())

        if realm.last_update and realm.last_update == last_update:   
            return False # update not available
        
        # Update realm's attribute in the db only after updating is done
        realm.last_update = last_update
        return True # update available

    def write_output(self, updated_realms):
        """Updates model with up to date parsed data.\n
        Serializes parsed data in memory for later use.\n
        Encodes data in Lua Table format for Multiboxer (WoW addon).
        """
        with open(f"{TEMP_FOLDER}/_serialized_data.pickle", 'wb') as file:
            pickle.dump(self.parsed_data, file)
        conn = sqlite3.connect(CURRENT_DATA)
        c = conn.cursor()

        # Update auction_chunks table with new data from updated_realms
        for realm in updated_realms:
            c.execute("DELETE FROM auction_chunks WHERE realm = ?", (realm.name, ))
            for auction_chunk in self.parsed_data[realm.name]:
                values = (realm.name,
                          auction_chunk['item_id'], 
                          auction_chunk['quantity'], 
                          auction_chunk['price'],
                          auction_chunk['stack_size'],
                          auction_chunk['owner'],
                          auction_chunk['time_left'],)
                c.execute("""INSERT INTO auction_chunks
                        (realm, item_id, quantity, price, stack_size, owner, time_left)
                        VALUES(?, ?, ?, ?, ?, ?, ?)""",
                        values)
        conn.commit()
        conn.close()
        

if __name__ == '__main__':
    dp = DataParser()
    #dp.update_all(force_update=True)
    #dp.update_loop()
