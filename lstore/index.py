"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        #Alvin: This makes primary key column into a Btree for indexing
        self.table = table
        self.indices[table.key] = OOBTree()
        pass

    #Alvin: adding an insert function for the b-tree that appends values instead of replaces, this way keys (column values) can refer to multiple values (multiple RIDS)
    def insert_btree(self, column, key, value):
        btree = self.indices[column]
        if btree.has_key(key):
            btree[key].append(value)
        else:
            btree[key] = [value]
    """
    # returns the location of all records with the given value on column "column"
    """
    #Alvin: (WORK IN PROGRESS)
    def locate(self, column, value):
        #self.indices[column]
        #if value in self.indices[columns]:
            #return 
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    # I think I need this for sum()?
    def locate_range(self, begin, end, column): # Iris??
        # rid_list = []
        # for value, rid in self.indices[column][begin:end]:
            # rid_list.append(rid)
        #return rid_list
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
