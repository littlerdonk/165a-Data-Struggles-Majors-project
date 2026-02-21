class BufferPool():
    def __init__(self, capacity=100):
        # initializes buffer pool and sets capacity for it
        self.pool = {}
        self.buffer_capacity = capacity
        self.dirty = set()
        self.drive = {}

    def buffer_insert(self, key, value):  # Nicholas
        if key not in self.pool:  # checks if requested key is already in buffer pool and only moves forward if key is not in buffer pool
            if self.buffer_at_capacity(): # if bufferpool is at capacity then we must replace our oldest value with a new one
                oldest_key = list(self.pool.keys())[0]  # grabs oldest key from bufferpool for eviction
                if oldest_key in self.dirty: # If the oldest value in the buffer pool is not written to the secondary storage then we need to write it before eviction
                    # Put code here to put dirty value in storage drive
                    self.drive[oldest_key] = self.pool[oldest_key] # self.drive doesnt exist yet but we'll add it later
                    self.dirty.remove(oldest_key)
                    self.evict_key(oldest_key)  # evicts data from pool after writing it to storage
                    self.pool[key] = value
                else: # If oldest value in buffer pool is already written to secondary storage then it is safe to evict data from buffer pool
                    self.evict_key(oldest_key)  # evicts non-dirty data from pool
                    self.pool[key] = value
            else: # If buffer pool not at capacity then it safe to add new value without the need for any evictions
                self.pool[key] = value

        elif key in self.pool: # if requested key is already in the buffer pool (RAM) then we simply grab that value
            existing_value = self.pool[key]  # grabs value from pool
            return existing_value

    def mark_dirty(self, key):
        self.dirty.add(key) #adds key to dirty value's set

    def buffer_at_capacity(self):  # Nicholas
        return len(self.pool) >= self.buffer_capacity  # checks if there is capacity available in bufferpool and returns true/false

    def is_page_pinned(self, key): # Iris
        # checking if the pin count of the page is more than 0, if so then the page is locked from merging, eviction, etc.
        if self.pool[key].pin_count > 0:
            # assuming self.pool[key] is calling the page??
            return True # returns true if page is pinned 
        else:
            return False # other wise pin_count = 0, so the page is safe to evict, merge, etc.
    
    def evict_key(self, key):
        if self.is_page_pinned(key): # Iris: checks if page is pinned before evicting it
            raise Exception("Eviction failed: page is currently being accessed.")
        # Checking if the page is dirty or not is included in buffer_insert, so I won't add it here
        del self.pool[key]  # deletes key from buffer pool


    


