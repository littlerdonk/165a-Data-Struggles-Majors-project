from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid 
        self.key = key 
        self.indirection = None #set to None 
        self.schema_encoding = 0 
        self.start_time = None # setted later in insert and update 
        self.last_updated_time = None #set in update
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
        self.page_directory = {} # dictionary to store data and offset under RIDS
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge: M2 
        self.rid = 0
        self.base_pages = []
        self.total_columns = 4 + num_columns 
        self.tail_pages = []
        self.cur_tail_range_index = -1 # the greater range index for base pages
        self.cur_base_range_index = -1 # the greater range index for base pages
        
        self.new_base_page_range()# make the first base page range 

    
    def insert(self, values): # Nicholas & Sage 
        if len(values) == self.num_columns:#check 
            
            #find the page to insert into using the first to check capacity 
            current_pages = self.base_pages[self.cur_base_range_index]
            
            #check capacity
            if not current_pages[0].has_capacity():#if there is no capacity 
                self.new_base_page_range()#make a new page range
                current_pages = self.base_pages[self.cur_base_range_index]# updates current pages to point to new page range 
                
            rid = self.rid # set rid for insert
            self.rid += 1 # increase rid by one to indicate new rid
                
                    
            # Insert the value into each column's page
            all_columns = [0, rid, int(time()), 0] + list(values) # this is the all column which stores [indirection, RID, time made, schema encoding] 
            offset = None # reset offset 
            for col, value in enumerate(all_columns):#iterate though each part of all columns and stores value and METADATA in col 
                offset = current_pages[col].write(value) # constantly points to last datapoint in list
            #store the range index and the offset to the page directory 
            self.page_directory[rid] = (self.cur_base_range_index, offset)
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
        old_direction = base_pages[INDIRECTION_COLUMN].read(base_offset)
        #get the current record via the RID
        current_record = self.get_record(rid)
        #store a copy of current record into tail columns 
        tail_columns = current_record.columns.copy()
        
        #get current tail pages
        for col, data in enumerate(values):# iterativly apply updates through columns 
            if data is not None:#if data is not not 
                tail_columns[col] = data#add data to tail columns 
                
        tail_pages = self.get_current_tail_pages()
        
        #create new tail RID 
        tail_rid = self.rid 
        self.rid  += 1

        # schema encoding calculation
        schema_encoding = 0
        for i, val in enumerate(values):
            if val is not None:
                schema_encoding += (1 << i)# reads the value of i as a bit map

        all_columns = [old_direction, tail_rid, int(time()), schema_encoding] + tail_columns# create all columns wit metadata and tail_columsn
        #do the update
        tail_offset = None #set offset to none to update after adding page for speed
        for col, value in enumerate(all_columns): #enumerate through all columns metadata
            tail_offset = tail_pages[col].write(value)#update offset to point ot latest page
            
        # Get location
        page_index, offset = self.page_directory[rid]
        
        #store tail in directory 
        self.page_directory[tail_rid] = (self.cur_tail_range_index, tail_offset)
        #update base indirection 
        base_pages[INDIRECTION_COLUMN].update(base_offset, tail_rid)
        
        return True
        

        
    def delete(self, rid): # Nicholas
        
        #deletes base page as well as tail pages of record associated with rid
        #FIX BY Sage to include support for tail files and Btree Indexing 
        if rid in self.page_directory:#check if the RID exists in page directory
             # Locates slot including base page range and offset with rid in page directory
            location, offset = self.page_directory[rid]#Update Sage adds offset and index to location search 
            for col in range(self.total_columns):# iterate through self.total columns 
                page = self.base_pages[location][col]#find the page requested
                page.update(offset, None)#update to None from the offset which deletes the entry 
            del self.page_directory[rid]#delete rid from page directory 
            return True
           
        else:
            print("RID Not Found in Page Directory")#not found in page directory 
            return False


    def new_base_page_range(self):#Sage 
        # create page range
        page_range = [] 
        for blank in range(self.total_columns): #iterate through every column
            page_range.append(Page(capacity=512))#make new pages
        self.base_pages.append(page_range)#append that to the base page
        self.cur_base_range_index = len(self.base_pages) - 1 #increase the range index by one 
            
    def new_tail_page_range(self):#Sage 
         #create tail page range
        page_range = [] 
        for blank in range(self.total_columns): #iterate through every column
            page_range.append(Page(capacity=512))# make new pages again 
        self.tail_pages.append(page_range)#append to tail pages 
        self.cur_tail_range_index = len(self.tail_pages) - 1 # set tail range index by the length of the pages -1 
        
            
    def get_current_tail_pages(self):#Sage 
        #get current tail page range and create if needed
        if self.cur_tail_range_index < 0 :# if this is the first page
            self.new_tail_page_range()# make new page range
        current_pages = self.tail_pages[self.cur_tail_range_index]#add index range page to current pages
        if not current_pages[0].has_capacity():#check capacity and add if needed 
            self.new_tail_page_range()#add new page range for more capacity 
            current_pages = self.tail_pages[self.cur_tail_range_index] # point to new page range
        return current_pages

    def get_record(self, rid, page_version=0):# Sage and Nicholas
        # Grabs a record from using its RID. If page is less than 0 then we grab a tail record instead.
        if rid in self.page_directory:#in the page directory 
            base_range_index, base_offset = self.page_directory[rid]# set the index and offset simultaniously via RID
            base_pages = self.base_pages[base_range_index]# add to base page 
            rid_value = base_pages[RID_COLUMN].read(base_offset)
            if rid_value == 0 or rid_value is None:
                return None  # Record was deleted
            indirection = base_pages[INDIRECTION_COLUMN].read(base_offset) # set indirection 
            columns = []
            
            for col in range(4,self.total_columns): # iterate through each column. Change 4 to METADATA COLUMN 
                value = base_pages[col].read(base_offset)#grab value from the read of the offset
                columns.append(value)#append it to columns 
            if indirection != 0 and page_version < 0: #If version of record is requested and record has tail pages then we apply tail updates.
                columns = self.tail_update(columns, indirection, page_version)#take all the columns and the in direction to update tail
            key = columns[self.key] 
            # Creates record and its indirection then returns full record
            record = Record(rid, key, columns)
            record.indirection = indirection 
            return record #return the full record
        else:
            return None#not in the page directory
            
    def tail_update(self, base_columns, tail_rid, version=0):# sage tail update and merge becasue select versions requires a tail update? 
         #updates the tail pages using in get record
        if tail_rid == 0 or tail_rid not in self.page_directory:#checks if the rid exists and is not 0 
            return base_columns
    
        # Build the tail chain to understand versions
        tail_chain = []
        current_tail = tail_rid
        while current_tail != 0 and current_tail in self.page_directory:
            tail_chain.append(current_tail)
            tail_range_index, tail_offset = self.page_directory[current_tail]
            tail_pages = self.tail_pages[tail_range_index]
            current_tail = tail_pages[INDIRECTION_COLUMN].read(tail_offset)
        
        # Determine how many tails to apply based on version
        # version 0 = all tails (most recent)
        # version -1 = all but the most recent tail (one version back)
        # version -2 = all but the two most recent tails (two versions back), etc.
        if version == 0:
            # Apply all tails
            num_tails_to_apply = len(tail_chain)
        else:
            # version -1 means skip the 1st tail, -2 means skip 2 tails, etc.
            num_tails_to_skip = abs(version)
            num_tails_to_apply = max(0, len(tail_chain) - num_tails_to_skip)
        
        # Start with base columns
        merged_columns = base_columns.copy()
        
        # Apply tails from oldest to newest (reverse order) up to num_tails_to_apply
        tails_to_process = tail_chain[::-1][:num_tails_to_apply]
        
        for tail_rid_item in tails_to_process:
            tail_range_index, tail_offset = self.page_directory[tail_rid_item]
            tail_pages = self.tail_pages[tail_range_index]
            schema_encoding = tail_pages[SCHEMA_ENCODING_COLUMN].read(tail_offset)
            
            # Apply updates from this tail record based on schema encoding
            for col_idx in range(self.num_columns):
                if schema_encoding & (1 << col_idx):  # This column was updated in the tail
                    tail_value = tail_pages[col_idx + 4].read(tail_offset)
                    merged_columns[col_idx] = tail_value
        
        return merged_columns
    
        
    def get_rid(self, rid): # Sage and Nicholas
        return Record(rid, key, columns) # Grabs record using get_record function above using RID

    def __merge(self): # Milestone 2 requiremnt 
        print("merge is happening")
        pass
 
