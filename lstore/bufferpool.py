class BufferPool():
    def __init__(self, capacity=100):
        # initializes buffer pool and sets capacity for it
        self.pool = {}
        self.buffer_capacity = capacity
        self.dirty = set()

    def buffer_at_capacity(self):  # Nicholas
        return len(self.pool) >= self.buffer_capacity  # checks if there is capacity available in bufferpool and returns true/false

    def evict_key(self, key):
        del self.pool[key]  # deletes key from buffer pool

    def buffer_insert(self, key, value):  # Nicholas
        if key not in self.pool:  # checks if requested key is already in buffer pool
            if self.buffer_at_capacity():
                oldest_value = list(self.pool.keys())[0]  # grabs oldest key from bufferpool for eviction
                for i in self.pool:
                    if i not in drive:  # checks if data is dirty
                        self.mark_dirty(value)  # Marks data in buffer pool as dirty if not in storage drive
                    else:
                        pass
                if oldest_value in self.dirty:  # checks if oldest value in buffer pool is dirty
                    # Put code here to put dirty value in storage drive
                    self.evict_key(oldest_value)  # evicts data from pool after writing it to storage
                    self.pool[key] = value
                else:
                    self.evict_key(oldest_value)  # evicts non-dirty data from pool
                    self.pool[key] = value
            else:
                self.pool[key] = value



        elif key in self.pool:
            existing_value = self.pool[key]  # grabs value from pool

    def mark_dirty(self, key):
        self.dirty.add(key) #adds key to dirty value's set


    


