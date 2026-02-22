from lstore.table import Table
from lstore.page import Page
from lstore.bufferpool import BufferPool
import os
import json
import io

class Database():

    def __init__(self):
        self.tables = []
        pass

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
            table = Table(name, num_columns, key)
            
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
            table.cur_base_range_index = len(table.base_pages) - 1
            table.cur_tail_range_index = len(table.tail_pages) - 1
    
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
