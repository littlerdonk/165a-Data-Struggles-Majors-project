from lstore.page import Page
from lstore.table import Table
from lstore.db import Database
import os
import io

# Implementation Idea:
# - To write something onto a page
# - Code some disk management function that gets the page from a physical file
# - Insert into bufferpool
# - Then page.py write data onto page
# - after merge and stuff we need to write it back to the disk (page is dirty)
# - We need file management to update the physical location of the page (file)

class DiskManager(): # Iris
    def __init__(self, path):
        self.path = path # file path
        self.keys = [] # keep track of a list of keys that's in the drive

    # This class should help with the transition of a page from disk (physical file) to the bufferpool (RAM)

    def write_page(self, table_name, page_type, r_idx, col, page): 
        # Creates a new file with the inputted information, this input is also the key of the page
        key = table_name + "/" + page_type + "/range_" + str(r_idx) + "/col_" + str(col) + ".bin"
        self.keys.append(tuple(table_name, page_type, r_idx, col)) # appends the key into a list so it can be used in bufferpool later
        file = self.path + "/" + key
        file_open = io.open(file, 'wb') # opens a file (page) prepares to write it
        file_open.write(page.data) # we input the page (from Page.py) into write_page so we can write the data (that should be written in page.py) into the disk
        file_open.close() # Once the updated data is written back into the disk, close the file

        # note: if file path doesn't exist, it writes a new file at that path (new page)

    def get_page(self, table_name, page_type, r_idx, col):
        key = table_name + "/" + page_type + "/range_" + str(r_idx) + "/col_" + str(col) + ".bin"
        self.keys.append(tuple(table_name, page_type, r_idx, col)) # appends the key into a list so it can be used in bufferpool later
        file = self.path + "/" + key
        if not os.path.exists(file):
            # if the file path does not exist, then return none
            return None
        page = Page(capacity = 512) # set standard 2 bytes capacity
        file_open = io.open(file, 'rb') # opens a file (page) prepares it for read 
        page.data = bytearray(file_open.read()) # specified bytes
        file_open.close() # once page is read, file is closed, but the page is now in the buffer pool
        return page
        

class BufferPool():
    def __init__(self, capacity=100, path = None):
        self.disk_manager = DiskManager(path) # initializes diskmanager so we can pull pages into bufferpool
        # initializes buffer pool and sets capacity for it
        self.pool = {} # key calls to the page (value) of pool --> also acts as a key to the page for storage
        # key template: table_name/page_type/rangeindex/column
        self.buffer_capacity = capacity
        self.dirty = set()
        self.buffer_order = []

    def buffer_insert(self, key, value):  # Nicholas
        # Note from Iris: key is a tuple of (table_name, page_type, r_idx, col)
        if key not in self.pool:  # checks if requested key is already in buffer pool and only moves forward if key is not in buffer pool
            if self.buffer_at_capacity():  # if bufferpool is at capacity then we must replace our oldest value with a new one
                # were going to use Least Recently Used for deciding which page to evict from the buffer pool
                oldest_key = self.buffer_order.pop(0)
                if oldest_key in self.dirty:
                    # If the oldest value in the buffer pool is not written to the storage drive then we need to write it before eviction
                    # Iris:
                    table_name = oldest_key[0] # since key is a tuple, i'm deconstructing it for disk_manager
                    page_type = oldest_key[1]
                    r_idx = oldest_key[2]
                    col = oldest_key[3]
                    self.disk_manager.write_page(table_name, page_type, r_idx, col, self.pool[oldest_key]) # writing page into drive 
                    self.dirty.remove(oldest_key)
                    self.evict_key(oldest_key)
                    self.pool[key] = value
                    self.buffer_order.append(key)

                else:  # If the oldest value in buffer pool is in storage drive then it is safe to evict it from buffer pool
                    self.evict_key(oldest_key)  # evicts non-dirty data from pool
                    self.pool[key] = value
                    self.buffer_order.append(key)
            else:  # If buffer pool not at capacity then it safe to add new value without the need for any evictions
                self.pool[key] = value
                self.buffer_order.append(key)

        elif key in self.pool:  # if requested key is already in the buffer pool (RAM) then we simply grab that value
            self.pool[key] = value
            self.mark_dirty(key)
            self.buffer_order.remove(key)  # removes accessed key from its current age in the buffer pool
            self.buffer_order.append(key) # adds the key back to the order so that it is now the newest key
            return self.pool[key]

    # buffer_get is used for guaranteeing that we always get a page
    def buffer_get(self, key): # Nicholas and Iris
        # In order to ensure that we always get a page we check both the pool and the drive
        if key in self.disk_manager.keys and key in self.pool: # Iris: checks if key is in the drive
            # here we just need to reset the requested pages position in the buffer_order and then return it from the buffer pool
            self.buffer_order.remove(key)
            self.buffer_order.append(key)
            return self.pool[key]
        else:
            if key not in self.disk_manager.keys:
                # this is just in case the requested page is not in the storage drive either
                return None
            # Iris: if key is in drive but not bufferpool, bring it into the bufferpool
            table_name = oldest_key[0] # since key is a tuple, i'm deconstructing it for disk_manager
            page_type = oldest_key[1]
            r_idx = oldest_key[2]
            col = oldest_key[3]
            page = self.disk_manager.get_page(table_name, page_type, r_idx, col)
            self.buffer_insert(key, page)
            return page  # returns the page that we are trying to access

    def mark_dirty(self, key):
        self.dirty.add(key)  # adds key to dirty value's set

    def buffer_at_capacity(self):  # Nicholas
        return len(
            self.pool) >= self.buffer_capacity  # checks if there is capacity available in bufferpool and returns true/false

    def is_page_pinned(self, key):  # Iris
        # checking if the pin count of the page is more than 0, if so then the page is locked from merging, eviction, etc.
        if self.pool[key].pin_count > 0:
            # assuming self.pool[key] is calling the page??
            return True  # returns true if page is pinned
        else:
            return False  # other wise pin_count = 0, so the page is safe to evict, merge, etc.

    def evict_key(self, key):
        if self.is_page_pinned(key):  # Iris: checks if page is pinned before evicting it
            raise Exception("Eviction failed: page is currently being accessed.")
        # Checking if the page is dirty or not is included in buffer_insert, so I won't add it here
        del self.pool[key]  # deletes key from buffer pool
