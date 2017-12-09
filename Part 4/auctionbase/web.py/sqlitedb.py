import web

db = web.database(dbn='sqlite',
<<<<<<< HEAD
        db='[YOUR SQLite DATABASE FILENAME]' #TODO: add your SQLite database filename
=======
        db='AuctionBase' #TODO: add your SQLite database filename
>>>>>>> 4d35fc03275122e51ad2bbba35991f3d93c00f12
    )

######################BEGIN HELPER METHODS######################

# Enforce foreign key constraints
# WARNING: DO NOT REMOVE THIS!
def enforceForeignKey():
    db.query('PRAGMA foreign_keys = ON')

# initiates a transaction on the database
def transaction():
    return db.transaction()
# Sample usage (in auctionbase.py):
#
# t = sqlitedb.transaction()
# try:
#     sqlitedb.query('[FIRST QUERY STATEMENT]')
#     sqlitedb.query('[SECOND QUERY STATEMENT]')
# except Exception as e:
#     t.rollback()
#     print str(e)
# else:
#     t.commit()
#
# check out http://webpy.org/cookbook/transactions for examples

# returns the current time from your database
def getTime():
    # TODO: update the query string to match
    # the correct column and table name in your database
<<<<<<< HEAD
    query_string = 'select currenttime from Time'
    results = query(query_string)
    # alternatively: return results[0]['currenttime']
    return results[0].currenttime # TODO: update this as well to match the
                                  # column name
=======
    query_string = 'select Time from CurrentTime'
    results = query(query_string)
    # alternatively: return results[0]['currenttime']
    return results[0].Time # TODO: update this as well to match the
                                  # column name
    
# sets the Time from selecttime input
def setTime(time):
    query_string = 'update CurrentTime set Time = $time'
    
    # try to set the time
    t = transaction()
    try:
        db.query(query_string, {'time': time})
    except Exception as e:
        t.rollback()
        print str(e)
        return False
    else:
        t.commit() 
        return True
>>>>>>> 4d35fc03275122e51ad2bbba35991f3d93c00f12

# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will throw an Exception!
def getItemById(item_id):
    # TODO: rewrite this method to catch the Exception in case `result' is empty
    query_string = 'select * from Items where item_ID = $itemID'
    result = query(query_string, {'itemID': item_id})
    return result[0]

# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}):
    return list(db.query(query_string, vars))

#####################END HELPER METHODS#####################

#TODO: additional methods to interact with your database,
# e.g. to update the current time
