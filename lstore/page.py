
class Page:

    def __init__(self, capacity = None):
        self.num_records = 0 # Nicholas
        self.data = bytearray(PAGE_SIZE)
        if capacity is None: # Sage 
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
                #stores data as a 64-bit integer from offset to end of record as bytes 
            self.data[offset:offset + RECORD_SIZE] = value.to_bytes(RECORD_SIZE, byteorder='big', signed=True)
            self.num_records += 1  # Nicholas
            return offset 
        else:
            return -1
            
    def read(self, offset):
        #Sage
        #Read a value from the page at the given offset.
        #Returns the integer value stored at that offset.
        value_bytes = self.data[offset:offset + RECORD_SIZE]# grabs the data from the offset to the end of the record 
        value = int.from_bytes(value_bytes, byteorder = 'big', signed=True)#changes value_bytes to ints and returns them 
        return value 

    def update(self, offset, value): 
        #Sage
        #update a value at a specific offset in the page
        if value is None: 
            value = 0 

        self.data[offset:offset + RECORD_SIZE] = value.to_bytes(RECORD_SIZE, byteorder'big', signed=True)


    def get_num_records(self):
        #Sage
        return self.num_records

