from lstore.table import Table
from lstore.page import Page
from lstore.bufferpool import BufferPool
import os
import json
import io

class Database():

    def __init__(self):
        self.tables = []
        self.path = None
        self.bufferpool = None

    # loads all the table data from disk back into memory so the database can pick up where it left off
    # should load pages into the bufferpool instead of directly into the table
    def open(self, path): # naomi
        self.path = path
        # create bufferpool when database is opened
        self.bufferpool = BufferPool(capacity=100, path=path)

        # create the folder where all our database files will live
        # example: if path is "./my_database", it makes that folder
        if not os.path.exists(path):
            os.makedirs(path)
        
        # if there's no metadata file, this is a brand new database, so nothing to load
        meta_path = path + '/metadata.json' # builds the full path to where the metadata file would be
        if not os.path.exists(meta_path):
            return

        # read the metadata file which has all the saved table info
        meta_file = io.open(meta_path, 'r')
        meta = json.load(meta_file)
        meta_file.close()

        # recreate the table object with the same name, columns, and key as before
        for table_data in meta['tables']: # # loop through each table that was saved
            # get all the saved table info for this table
            name = table_data['name']
            num_columns = table_data['num_columns']
            key = table_data['key']
            rid = table_data['rid']
            page_directory = table_data['page_directory']
    
            # recreate the table object with the same info as before
            table = Table(name, num_columns, key, loading = True)
            
            # restore the rid counter so we dont reuse old rids
            table.rid = rid
            # restore page directory, converting keys back to integerss and values back to tuples
            directory = {}
            for k, v in page_directory.items():
                directory[int(k)] = tuple(v)
            table.page_directory = directory

            # FIX THIS load base pages and tail pages back from disk
            table.base_pages = self.load_pages(table, table_data, 'base')
            table.tail_pages = self.load_pages(table, table_data, 'tail')
    
            # restore the indexes so we know which page range is the current one
            table.cur_base_range_index = max(len(table.base_pages) - 1, 0)
            table.cur_tail_range_index = len(table.tail_pages) - 1
            
            #Sage: bug fixed logic below ?
            for base_rid, location in table.page_directory.items():
                page_type, range_index, offset = location
                if page_type != 'base':
                    continue
                record = table.get_record(base_rid)
                if record is None:
                    continue
                for col in range(table.num_columns):
                    if table.index.indices[col] is not None:
                        table.index.insert_btree(col, record.columns[col], base_rid)
                self.tables.append(table)
            '''
            r_idx = 0 # this is range index
            for page_range in table.base_pages:
                for col in range(table.total_columns):
                    page = page_range[col]
                    # key is (table name, page type, range index, column)
                    self.bufferpool.buffer_insert((table.name, 'base', r_idx, col), page)
                r_idx += 1

            r_idx = 0
            for page_range in table.tail_pages:
                for col in range(table.total_columns):
                    page = page_range[col]
                    self.bufferpool.buffer_insert((table.name, 'tail', r_idx, col), page)
                r_idx += 1
            self.tables.append(table)
            '''

    def close(self): #naomi
        # if no path is set, nothing to save
        if not self.path:
            return
        if not hasattr(self, 'bufferpool') or self.bufferpool is None:
            return
        # this will hold all the info we need to save for every table
        meta = {'tables': []}
        for table in self.tables:
            # save base and tail pages to disk and get back num_records for each page
            base_num_records = self.save_pages(table, 'base')
            tail_num_records = self.save_pages(table, 'tail')

            # save everything to rebuild the table later in open function
            table_data = {
                'name': table.name,
                'num_columns': table.num_columns,
                'key': table.key,
                'rid': table.rid,  # save rid so we dont reuse old rids
                'base_num_records': base_num_records,
                'tail_num_records': tail_num_records,
            }
            # convert page directory keys to strings because json requires string keys
            directory = {}
            for k, v in table.page_directory.items():
                directory[str(k)] = list(v)
            table_data['page_directory'] = directory

            meta['tables'].append(table_data)
            # flush dirty pages for this table to disk before closing
            for key in list(self.bufferpool.dirty):
                # only flush pages that belong to this table
                if key[0] == table.name:
                    page = self.bufferpool.buffer_get(key)
                    if page:
                        self.bufferpool.fake_drive[key] = page
                    self.bufferpool.dirty.remove(key)
                    
        # write metadata to a file so we can load it back later in open
        meta_path = self.path + '/metadata.json'
        meta_file = io.open(meta_path, 'w')
        json.dump(meta, meta_file) # converts Python data structures into the standardized JSON format
        meta_file.close()    

    
        
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index): # naomi
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name): # naomi
        # loop through tables and remove the one with the matching name
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                return

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name): # naomi
        for table in self.tables:
            if table.name == name:
                return table
        return None


    
    def load_pages(self, table, table_data, page_type):#Sage: loads tables from disk into bufferpool becasue it didnt look like it was here
    # figure out which key holds num_records info
    num_records_key = page_type + '_num_records'# makes records
    num_records_list = table_data.get(num_records_key, [])#make a list of all records

    pages = []#holds pages

    # loop through each page range that was saved
    for range_index, range_num_records in enumerate(num_records_list):
        page_range = []  # holds all the column pages for this range

        for col in range(table.total_columns):#loop through every column 
            page = self.bufferpool.get_page(table.name, page_type, range_index, col)#grab pages from disk 

            if page is None:
                page = Page(capacity=512)#page not in disk make a new one 

            if col < len(range_num_records):
                page.num_records = range_num_records[col]#restore the count of num records

            page_range.append(page)#add this column's page to the range

        pages.append(page_range)#add the full range to our pages list

    return pages#return all loaded page ranges



    def save_pages(self, table, page_type):
        num_records_list = []#will hold num_records for every range and column
    
        #figure out how many ranges exist for this page type
        if page_type == 'base':
            num_ranges = table.cur_base_range_index + 1#index starts at 0
        else:
            num_ranges = table.cur_tail_range_index + 1
    
        for range_index in range(num_ranges):
            range_num_records = []#num_records for each column in this range
    
            for col in range(table.total_columns):
                #grab the page from the bufferpool
                page = self.bufferpool.get_page(table.name, page_type, range_index, col)
    
                if page is not None:
                    #write the page to disk using the disk manager
                    self.bufferpool.disk_manager.write_page(
                        table.name, page_type, range_index, col, page
                    )
                    range_num_records.append(page.num_records)#save how full this page was
                else:
                    range_num_records.append(0)#page didn't exist, record 0
    
            num_records_list.append(range_num_records)
    
        return num_records_list
