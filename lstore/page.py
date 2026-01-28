
class Page:

    def __init__(self, capacity):
        self.num_records = capacity # Nicholas
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records > len(self.data) # Nicholas
        pass

    def write(self, value):
        if self.has_capacity() is True: # Nicholas
            self.num_records += 1
            pass
        else:
            pass

