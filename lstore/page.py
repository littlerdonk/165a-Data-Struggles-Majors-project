
class Page:

    def __init__(self, capacity = None):
        self.num_records = 0 # Nicholas
        self.data = bytearray(PAGE_SIZE)
        if capacity = None: # Sage 
            self.capacity = PAGE_SIZE // RECORD_SIZE # 512 records for a 4kb page
        else: 
            self.capacity = capacity

    def has_capacity(self):
        return self.num_records < self.capacity # Nicholas

    
    def write(self, value):
        if self.has_capacity() is True: # Nicholas
            #Calculates offset like standard Lstore 
            offset = self.num_records * RECORD_SIZE # Sage
            if value is None: 
                value = 0
                #stores data as a 64-bit integer
            self.data[offset + RECORD_SIZE] = value.to_bytes(RECORD_SIZE, byteorder='big', signed=True)
            self.num_records += 1  # Nicholas
            return offset 
        else:
            return -1
            
    def read()

