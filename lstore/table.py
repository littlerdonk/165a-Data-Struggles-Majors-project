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

    def insert(self, values): # Nicholas & Sage 
        if len(values) == self.num_columns:
            rid = self.rid
            self.rid += 1
            #find the page to insert into using the first to check capacity 
            page_index = 0 
            current_page = self.base_pages[0][page_index]
            
            #check for full capacity and create new pages for all columns if full 
            if not current_page.has_capacity():
                page_index = len(self.base_pages[0])
                for col in range(self.num_columns):self.base_pages[col].append(Page(capacity=512))
                    
            # Insert the value into each column's page
            offset = None 
            for col in range(self.num_columns):
                page = self.base_pages[col][page_index]
                offset = page.write(values[col])

            self.page_directory[rid] = (page_index, offset)
            return rid 
        else:
            return False

    
    def update(self, rid, values): # Sage 
        if rid not in self.page_directory:
            return False
        # Get location
        page_index, offset = self.page_directory[rid]
        
        # update each column
        for col in range(self.num_columns):
            if values[col] is not None:  # Only update non-None values
                page = self.base_pages[col][page_index]
                page.update(offset, values[col])
        
        return True
        

        
    def delete(self, rid): # Nicholas
        if rid in self.page_directory:
            location = self.page_directory[rid]
            for col in range(self.num_columns):
                page = self.base_pages[location[0]]
                page.values[location[1]] = None
            return
        else:
            print("RID Not Found in Page Directory")

    def get_rid(self, rid): # Sage
        return Record(rid, key, columns)

    def __merge(self):
        print("merge is happening")
        pass
 
