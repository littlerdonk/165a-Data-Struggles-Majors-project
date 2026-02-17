from lstore.table import Table
from lstore.page import Page
import os
import json

class Database():

    def __init__(self):
        self.tables = []
        pass

    # loads all the table data from disk back into memory so the database can pick up where it left off
    # should load pages into the bufferpool instead of directly into the table
    def open(self, path): # naomi
        self.path = path

        # create the folder where all our database files will live
        # example: if path is "./my_database", it makes that folder
        if not os.path.exists(path):
            os.makedirs(path)
        
        # if there's no metadata file, this is a brand new database, so nothing to load
        meta_path = os.path.join(path, 'metadata.json') # builds the full path to where the metadata file would be
        if not os.path.exists(meta_path):
            return

        # open and read the metadata file which stores all table info
        with open(meta_path, 'r') as f:
            meta = json.load(f) # converts metadata file into a python dict we can use

        # recreate the table object with the same name, columns, and key as before
        for table_data in meta['tables']: # # loop through each table that was saved
            # get all the saved table info for this table
            name = table_data['name']
            num_columns = table_data['num_columns']
            key = table_data['key']
            rid = table_data['rid']
            page_directory = table_data['page_directory']
    
            # recreate the table object with the same info as before
            table = Table(name, num_columns, key)
            
            # restore the rid counter so we dont reuse old rids
            table.rid = rid
            # restore page directory, converting keys back to integerss and values back to tuples
            fixed_directory = {}
            for k, v in page_directory.items():
                fixed_directory[int(k)] = tuple(v)
            table.page_directory = fixed_directory

            # load base pages and tail pages back from disk
            table.base_pages = self.load_pages(table, table_data, 'base')
            table.tail_pages = self.load_pages(table, table_data, 'tail')
    
            # restore the indexes so we know which page range is the current one
            table.cur_base_range_index = len(table.base_pages) - 1
            table.cur_tail_range_index = len(table.tail_pages) - 1
    
            # TODO: load pages into bufferpool instead of directly into table
            # for page_range in table.base_pages:
            #     for col, page in enumerate(page_range):
            #         self.bufferpool.buffer_insert((table.name, 'base', r_idx, col), page)
    
            self.tables.append(table)


    def close(self): #naomi
        # if no path is set, nothing to save
        if not self.path:
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
            fixed_directory = {}
            for k, v in table.page_directory.items():
                fixed_directory[str(k)] = list(v)
            table_data['page_directory'] = fixed_directory

            meta['tables'].append(table_data)
            # TODO: flush dirty pages from bufferpool to disk before closing
            # for key in self.bufferpool.dirty:
            #     self.bufferpool.evict_key(key)

        # write metadata to a file so we can load it back later in open
        meta_path = os.path.join(self.path, 'metadata.json')
        with open(meta_path, 'w') as f:
            json.dump(meta, f)

    # helper for open, loads pages from disk back into memory
    def load_pages(self, table, table_data, page_type):
        pass

    # helper for close, save pages from memory to disk
    def save_pages(self, table, page_type):
        
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
