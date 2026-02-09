from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table # store reference to table we'll be querying

    
    """
    # internal Method
    # Delete a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key): # naomi
        try:
            key_column = self.table.key  # Get which column is the key
            # B-Tree index to find all RIDs that have primary_key value
            matching_rids = self.table.index.locate(key_column, primary_key) # returns a list of RIDs
            if not matching_rids:
                return False
            rid = matching_rids[0] # get first matching RID
            self.table.delete(rid) # call on the delete method to remove record
            # delete from index 
            btree = self.table.index.indices[key_column] 
            rid_list = btree[primary_key]
            rid_list.remove(rid)
            # if no RIDs remain for this key, remove the key entirely
            if len(rid_list) == 0: # if list is empty
                del btree[primary_key] # delete the key (so no empty lists remain)
            return True 
        except: 
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns): # naomi
        # tracks updated columns (will be used in milestone 2); function takes just the column values, not schema encoding
        schema_encoding = '0' * self.table.num_columns
        try:
            #sage: check for duplicate keys
            key_value = columns[self.table.key]
            existing = self.table.index.locate(self.table.key, key_value)
            if existing:# Key already exists
                return False
            # call table's insert method, returns RID on success or False on failure
            rid = self.table.insert(list(columns))
            if rid is not False and rid is not None: # see if insert successful
                key_value = columns[self.table.key] # primary key values from columns
                self.table.index.insert_btree(self.table.key, key_value, rid) # Adds an entry to the B-Tree index
                return True
            else: # insert failed return False
                return False
        except Exception: # crash return false
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index): # naomi
        try: # use Btree to find all RIDs with search_key value
            matching_rids = self.table.index.locate(search_key_index, search_key)
            if not matching_rids:
                return False
            # store list of record objects
            results = []
            for rid in matching_rids: #go through each matching rid
                # table's get_record method from table.py to get the full Record object
                record = self.table.get_record(rid)
                if record is None: #skip if record dne or is none
                    continue
                results.append(record)
        
            # if we found records, return them; return false if not
            if results:
                return results
            else:
                return False
        except Exception:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        #Sage: for milestone 1 return the current version I think so its just the same as sum
        # copy pasted back in here becasue it broke if not in here idk why 
        try:
            # Use B-tree to find all RIDs with search_key value
            matching_rids = self.table.index.locate(search_key_index, search_key)
            if not matching_rids:
                return False
            
            # Store list of record objects
            results = []
            for rid in matching_rids:
                # Get the record with the specified version
                record = self.table.get_record(rid, relative_version)
                if record is None:
                    continue
                results.append(record)
            
            # If we found records, return them; return false if not
            if results:
                return results
            else:
                return False
        except Exception:
            return False
    

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns): # Iris
        try:
            # get rid from self.table using primary key
            # if rid not found --> return false
            matching_rid = self.table.index.locate(self.table.key, primary_key) # locating matching rid using key
            if not matching_rid:#Sage: minor potential bug fix check if they are the same 
                return False
            rid = matching_rid[0]#set rid from matching rids 
        

            new_key_value = columns[self.table.key]#Sage: Bug fix for new eval:  Check if primary key column is being updated
        
            if new_key_value is not None and new_key_value != primary_key:#check if the primary key is changing
            
                # Check if new key already exists to avoid duplicates
                existing = self.table.index.locate(self.table.key, new_key_value)
                if existing:#if it already exists return false as you cant duplicate a key 
                    return False 
                
                #Sage: Remove old key from index
                btree = self.table.index.indices[self.table.key]
                rid_list = btree[primary_key]
                rid_list.remove(rid)
                if len(rid_list) == 0:
                    del btree[primary_key]
                
                updating = self.table.update(rid, list(columns))#Sage minor bug fix since matching_rid is a list 
            # Iris's simple example:
            # student_id = 12345 <- Primary Key in this case
            # updated_columns = [32, 88, 90, 30, 22] <-  every column wants to be updated in this case
            # user calls update(student_id, updated_columns)
            # matching_rid <- a list of rids of the columns found using primary key and index locate
            # self.table.update(matching_rid, columns) should be going through each rid and updating the column with the specified values in updated_columns
            # If that's successful this update() will return True
                if updating: # Returns True
                    self.table.index.insert_btree(self.table.key, new_key_value, rid) # Updates the value in thebtree
                    return True
                else:
                    self.table.index.insert_btree(self.table.key, primary_key, rid) # if its not updating ie updating = None roll back to avoid data corruption 
                    return False
            else: #primary key not changing push normal update
                updating = self.table.update(rid, list(columns))
                return updating
            
        except:
            return False
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index): # Iris
    # find the record id based on the input index:
        try:
            key_column = self.table.key # store the key of the table to be used in locate_range
            matching_rids = self.table.index.locate_range(start_range, end_range, key_column) # assuming the inputs are valid
            sum_range = 0 # set up sum_range variable for later
            for rid in matching_rids: # for loop iterates through every rid in the matching_rids list and gets the record from the table
                record = self.table.get_record(rid) # getting the current record using rid
                if record is not None: # if record exists, add it to sum_range
                    sum_range += record.columns[aggregate_column_index] # getting the specified columns using column index
            return sum_range
        except Exception:
            return False # if inputs are invalid.

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version): # Iris
        try: 
            # The code should be the same as sum, but get_record will include relative_version in the parameters this time
            key_column = self.table.key
            matching_rids = self.table.index.locate_range(start_range, end_range, key_column)
            sum_range = 0
            for rid in matching_rids:
                record = self.table.get_record(rid, relative_version)
                if record is not None:
                    sum_range += record.columns[aggregate_column_index]
            return sum_range
        except:
            return False
        # User inputs the range of values they're looking for, the column for what they want to sum, and the relative version of all the data
        # -1 = 1 version backwards
        # 0 = Most updated version (current version)

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
