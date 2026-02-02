from lstore.index import Index
from lstore.index import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.indirection = None 
        self.schema_encoding = 0 
        self.start_time = None
        self.last_updated_time = None
        self.columns = columns
        

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.rid = 0
        self.base_pages = [[] for col in range(self.num_columns)] # we should add a way to import data from Page.py

    def insert(self, values): # Nicholas
        if len(values) == self.num_columns:
            rid = self.rid
            self.rid += 1
            for col, val in range(values):
                # Add code to iterate through each column in self.base_pages and add value if there is space for it.

        else:
            return False

    def delete(self, rid): # Nicholas
        if rid in self.page_directory:
            location = self.page_directory[rid]
            for col in range(self.num_columns):
                page = self.base_pages[location[0]]
                page.values[location[1]] = None
            return
        else:
            print("RID Not Found in Page Directory")

    def __merge(self):
        print("merge is happening")
        pass
 
