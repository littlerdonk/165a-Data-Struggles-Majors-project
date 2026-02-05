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
        self.total_columns = 4 + num_columns 
        self.tail_pages = []
        self.current_tail_range_index = -1
        self.current_base_range_index = -1
        self.next_rid = 0

        self.new_base_page_range()

    
    def insert(self, values): # Nicholas & Sage 
        if len(values) == self.num_columns:
            rid = self.rid
            self.rid += 1
            #find the page to insert into using the first to check capacity 
            page_index = 0 
            current_page = self.base_pages[0][page_index]
            current_pages = self.base_pages[self.current_base_range_index]
            #check capacity
            if not current_pages[0].has_capacity():
                self.new_base_page_range()
                current_pages = self.base_pages[self.current_base_range_index]
        
            #check for full capacity and create new pages for all columns if full 
            if not current_page.has_capacity():
                page_index = len(self.base_pages[0])
                for col in range(self.num_columns):self.base_pages[col].append(Page(capacity=512))
                    
            # Insert the value into each column's page
            all_columns = [0, rid, int(time()), 0] + list(values)
            offset = None 
            for col, in value in enumerate(all_columns):
                offset = current_pages[col].write(value)

            self.page_directory[rid] = (self.current_base_range_index, offset)
            return rid 
        else:
            return False

    
    def update(self, rid, values): # Sage 
        #get current Values from base or any existing tails 
        current_record = self.get_record(rid) 
        
        base_range_index, base_offset = self.page_directory[rid]
        
        base_pages = self.base_pages[base_range_index]
        
        old_direction = base_pages[INDIRECTION_COLUMN].read(base_offset)
        
        current_record = self.get_record(rid)
        
        tail_columns = current_record.columns.copy()
        
        #get current tail pages
        for col, value in enumerate(columns):# iterativly apply updates through columns 
            if value is not None:
                tail_columns[col] = value
                
        tail_pages = self.get_current_tail_pages()
        #Finish FINISH FINISH 
        
        #create new tail RID 
        tail_rid = self.next_rid 
        self.next_rid  += 1
        
        if rid not in self.page_directory:#check if its not in the page directory 
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

    def new_base_page_range(self):#Sage 
        # create page range
        page_range = [] 
        for _ in range(self.total_columns): #iterate through every column
            page_range.append(Page(capacity=512))#make new pages
        self.base_pages.append(page_range)#append that to the base page
        self.current_base_range_index = len(self.base_pages) - 1 #increase the range index by one 
            
    def new_tail_page_range(self):#Sage 
         #create tail page range
        page_range = [] 
        for _ in range(self.total_columns): #iterate through every column
            page_range.append(Page(capacity=512))# make new columns 
        self.tail_pages.append(page_range)#append to tail pages 
        self.current_tail_range_index = len(self.tail_pages) - 1 # increase tail range index by one 
        
            
    def get_current_tail_pages(self):#Sage 
        #get current tail page range and create if needed
        if self.current_tail_range_index < 0 :# if this is the first page
            self.new_tail_page_range()# make new page range
        current_pages = self.tail_pages[self.current_tail_range_index]#add index range page to current pages
        if not current_pages[0].has_capacity():#check capacity and add if needed 
            self.new_tail_page_range()
            current_pages = self.tail_pages[self.current_tail_range_index]
        return current_pages

    def get_record(self, rid):#incomplete
        if rid in self.page_directory:#in the page directory 
            base_range_index, base_offset = self.page_directory[rid]# set the index and offset simultaniously via RID
            base_pages = self.base_pages[base_range_index]# add to base page 
            indirection = base_pages[INDIRECTION_COLUMN].read(base_offset) # set indirection 
            columns = []
            for col in range(4,self.total_columns): # iterate through each column. Change 4 to METADATA COLUMN 
                value = base_pages[col].read(base_offset)#grab value from the read of the offset
                columns.append(value)#append it to columns 
            if indirection != 0: #has direction
                columns = self.tail_update(columns, indirection)#FIX FIX FIX 
            key = columns[self.key] # set Key to make record with Key value stored
            return Record(rid, key, columns) #return the full record 
        else:
            return None#not in the page directory
            
    def get_rid(self, rid): # Sage
        return Record(rid, key, columns)

    def __merge(self):
        print("merge is happening")
        pass
 
