
class Page:

    def __init__(self, capacity):
        self.num_records = 0 # Nicholas
        self.data = bytearray(4096)
        self.capacity = capacity

    def has_capacity(self):
        return self.num_records < len(self.data) # Nicholas
        pass

    def write(self, value):
        if self.has_capacity() is True: # Nicholas
            self.num_records += 1
            pass
        else:
            pass

