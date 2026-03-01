class LockManager():
    def __init__(self):
        self.locks = {}
        self.mutex = Lock() # prevents two transactions from creating a lock at the same time

    def get_lock(self, rid:):
        with self.mutex:
            # we need to check if rid currently exists in self.locks and create object for it if not
            if rid in self.locks:
                return self.locks[rid] # returns transactions that have shared lock on rid and whether or not an exclusive lock exists for the rid
            else:
                # returns the lock object for the rid which tracks transactions with a shared and exclusive lock on rid
                self.locks[rid] = {
                    "exclusive": None,
                    "shared": set(),
                    "mutex": Lock()
                }
                return self.locks[rid]


    def acquire_exclusive(self, Xact_id, rid):
        # We check if rid has exclusive lock already assigned from transaction
        lock = self.get_lock(rid)
        lock["mutex"].acquire()
        if lock["exclusive"] == None:
            # add transaction id to rid's "exclusive" object
            lock["exclusive"] = Xact_id
            lock["mutex"].release()
        else:
            lock["mutex"].release()
            print("Record ID currently has exclusive lock.")

    def acquire_shared(self, Xact_id, rid):
        lock = self.get_lock(rid)
        lock["mutex"].acquire()
        # we can have multiple shared locks on an rid but we cant assign a shared lock to an rid that has been assigned an exclusive lock
        if lock["exclusive"] == None:
            lock["shared"].add(Xact_id)
            lock["mutex"].release()
        else:
            lock["mutex"].release()
            print("Record ID currently has exclusive lock.")

    def release(self, Xact_id, rid):
        # releases the transactions lock by checking if transaction is shared or exclusive
        try:
            lock = self.get_lock(rid)
            lock["mutex"].acquire()
            # we check if the lock being released is a shared or exclusive lock and then remove it from locks object
            if lock["exclusive"] == Xact_id:
                lock["exclusive"] = None
                lock["mutex"].release()
            else:
                lock["shared"].remove(Xact_id)
                lock["mutex"].release()
        except:
            print("Record ID does not currently have any shared or exclusive locks.")
