class BufferPool():
    def __init__(self, capacity=100):
        # initializes buffer pool and sets capacity for it
        self.pool = {}
        self.buffer_capacity = capacity


    def buffer_insert(self, key, value): # Nicholas
        if key not in self.pool: #checks if requested key is already in buffer pool
            if self.buffer_at_capacity():
                # look to evict data from pool
                # check if value that is about to be evicted is dirty
                    # if value is dirty then write value to storage drive and remove from buffer pool

                self.pool[key] = value #adds value to pool once old value is evicted

        elif key in self.pool:
            existing_value = self.pool[key] #grabs value from pool



    def buffer_at_capacity(self): # Nicholas
        return len(self.pool) >= self.buffer_capacity #checks if there is capacity available in bufferpool

    def evict(self, key):
        del self.pool[key] # simple delete from buffer pool but we might have to change this later to add more to it
        pass

    def mark_dirty(self):
        pass


