from lstore.page import Page

class BufferPool():
    def __init__(self, capacity=100):
        # initializes buffer pool and sets capacity for it
        self.pool = {}
        self.buffer_capacity = capacity
        self.dirty = set()
        self.fake_drive = {}
        self.buffer_order = []

    def buffer_insert(self, key, value):  # Nicholas
        if key not in self.pool:  # checks if requested key is already in buffer pool and only moves forward if key is not in buffer pool
            if self.buffer_at_capacity():  # if bufferpool is at capacity then we must replace our oldest value with a new one
                # were going to use Least Recently Used for deciding which page to evict from the buffer pool
                oldest_key = self.buffer_order.pop(0)
                if oldest_key in self.dirty:
                    # If the oldest value in the buffer pool is not written to the storage drive then we need to write it before eviction
                    self.fake_drive[oldest_key] = self.pool[oldest_key]  # real storage drive functionality doesnt exist yet but we'll add it later
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
    def buffer_get(self, key): # Nicholas
        # In order to ensure that we always get a page we check both the pool and the drive
        if key in self.pool:
            # here we just need to reset the requested pages position in the buffer_order and then return it from the buffer pool
            self.buffer_order.remove(key)
            self.buffer_order.append(key)
            return self.pool[key]
        else:
            if key not in self.fake_drive:
                # this is just in case the requested page is not in the storage drive either
                return None
            page = self.fake_drive.get(key)
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
