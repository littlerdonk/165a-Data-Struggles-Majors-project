from lstore.table import Table
from lstore.page import Page

class Database():

    def __init__(self):
        self.tables = []
        pass

    # loads all the table data from disk back into memory so the database can pick up where it left off
    def open(self, path): # naomi
        self.path = path
        
        pass

    def close(self): #naomi
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
