class BufferPool():
    def __init__(self, capacity=100):
        # initializes buffer pool and sets capacity for it
        self.pool = {}
        self.buffer_capacity = capacity
        self.dirty = set()
    
    def buffer_insert(self, key, value): # Nicholas
        if key not in self.pool: #checks if requested key is already in buffer pool
            if self.buffer_at_capacity():
                oldest_value = list(self.pool.keys())[0] #grabs oldest key from bufferpool for eviction
                for i in self.pool:
                    if i not in drive: #checks if data is dirty
                        self.mark_dirty(value) #Marks data in buffer pool as dirty if not in storage drive
                     else:
                        pass
                if self.pool[-1] is dirty #checks if oldest value in buffer pool is dirty
                    # Put code here to put dirty value in storage drive
                    self.evict(self.pool[-1]) #evicts data from pool after writing to storage
                    self.pool[-1] = key
                else:
                    self.evict(self.pool) #evicts data from pool after writing to storage
                    self.pool[-1] = key
            else: 
                self.pool[-1] = key
                    
                    

        elif key in self.pool:
            existing_value = self.pool[key] #grabs value from pool



    def buffer_at_capacity(self): # Nicholas
        return len(self.pool) >= self.buffer_capacity #checks if there is capacity available in bufferpool

    def evict(self, key):
        del self.pool[key] # simple delete from buffer pool but we might have to change this later to add more to it
        pass

    def mark_dirty(self):
        self.dirty.add(key)
        pass

    


