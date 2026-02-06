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
        self.base_pages = []
        self.total_columns = 4 + num_columns 
        self.tail_pages = []
        self.current_tail_range_index = -1
        self.current_base_range_index = -1
        self.next_rid = 0

        self.new_base_page_range()

    
    def insert(self, values): # Nicholas & Sage 
        if len(values) == self.num_columns:
            
            #find the page to insert into using the first to check capacity 
            current_pages = self.base_pages[self.current_base_range_index]
            
            #check capacity
            if not current_pages[0].has_capacity():#if there is no capacity 
                self.new_base_page_range()#make a new page range
                current_pages = self.base_pages[self.current_base_range_index]# updates current pages to point to new page range 
                
            rid = self.rid
            self.rid += 1
                
                    
            # Insert the value into each column's page
            all_columns = [0, rid, int(time()), 0] + list(values) # this is the all column which stores [indirection, RID, time made, schema encoding] 
            offset = None # reset offset 
            for col, value in enumerate(all_columns):#iterate though each part of all columns and stores value and METADATA in col 
                offset = current_pages[col].write(value) # constantly points to last datapoint in list
            #store the range index and the offset to the page directory 
            self.page_directory[rid] = (self.current_base_range_index, offset)
            return rid 
        else:
            return False

    
    def update(self, rid, values): # Sage 
        if rid not in self.page_directory:#check if its not in the page directory 
            return False
        #get current Values from base or any existing tails 
        current_record = self.get_record(rid) 
        # get base range index and base offset from the page directory 
        base_range_index, base_offset = self.page_directory[rid]
        # get base pages from the range index
        base_pages = self.base_pages[base_range_index]
        # store the old direction 
        old_indirection = base_pages[INDIRECTION_COLUMN].read(base_offset)
        #get the current record via the RID
        current_record = self.get_record(rid)
        #store a copy of current record into tail columns 
        tail_columns = current_record.columns.copy()
        
        #get current tail pages
        for col, value in enumerate(values):# iterativly apply updates through columns 
            if value is not None:
                tail_columns[col] = value
                
        tail_pages = self.get_current_tail_pages()
        
        #create new tail RID 
        tail_rid = self.next_rid 
        self.next_rid  += 1

        # schema encoding calculation
        schema_encoding = 0
        for i, val in enumerate(values):
            if val is not None:
                schema_encoding += (1 << i)# reads the value of i as a bit map

        all_columns = [old_indirection, tail_rid, int(time()), schema_encoding] + tail_columns
        #do the update
        tail_offset = None 
        for col, value in enumerate(all_columns): 
            tail_offset = tail_pages[col].write(value)
            
        if rid not in self.page_directory:#check if its not in the page directory 
            return False
            
        # Get location
        page_index, offset = self.page_directory[rid]
        
        # update each column
        for col in range(self.num_columns):
            if values[col] is not None:  # Only update non-None values
                page = self.base_pages[col][page_index]
                page.update(offset, values[col])
        #store tail in directory 
        self.page_directory[tail_rid] = (self.current_tail_range_index, tail_offset)
        #update base indirection 
        base_pages[INDIRECTION_COLUMN].update(base_offset, tail_rid)
        
        return True
        

        
    def delete(self, rid): # Nicholas
        #deletes base page as well as tail pages of record associated with rid
        if rid in self.page_directory:
            # Locates slot including base page range and offset with rid in page directory
            location = self.page_directory[rid]
            # Locates page range of desired slot
            base_pages = self.base_pages[location[0]]
            # Updates indirection column of slot with -1 to denote that the slot has been deleted
            base_pages[INDIRECTION_COLUMN].update(location[1], -1)
            return True
        else:
            return False


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
            if indirection > 0: #has direction
                columns = self.tail_update(columns, indirection)#take all the columns and the in direction to update tail
            if indirection == -1: # record has been deleted
                return None
            key = columns[self.key] # set Key to make record with Key value stored
            return Record(rid, key, columns) #return the full record
        else:
            return None#not in the page directory
            
    def tail_update(self, base_columns, tail_rid):# sage 
        #updates the tail pages using in get record
        if tail_rid == 0 or tail_rid not in self.page_directory:#checks if the rid exists and is not 0 
            return base_columns
    
        tail_range_index, tail_offset = self.page_directory[tail_rid]#grab the range index and tail offset from the directory via RID
        tail_pages = self.tail_pages[tail_range_index]#grab tail pages from the range index
    
        updated_columns = []
        for col in range(4, self.total_columns):#iterate through each column in total columsn 
            value = tail_pages[col].read(tail_offset)# read values from tail offset and set into values
            updated_columns.append(value)# store updated values 
    
        return updated_columns
    
        
    def get_rid(self, rid): # Sage
        return Record(rid, key, columns)

    def __merge(self):
        print("merge is happening")
        pass
 
