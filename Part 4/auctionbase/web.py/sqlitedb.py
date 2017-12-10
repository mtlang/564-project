import web

db = web.database(dbn='sqlite',
				  db='AuctionBase' #TODO: add your SQLite database filename
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
    query_string = 'select Time from CurrentTime'
    results = query(query_string)
    # alternatively: return results[0]['currenttime']
    return results[0].Time
    
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
	# close all auctions now that time elapsed
    close_auctions_time();
    close_auctions_price();

# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will return None!
def getItemById(item_id):
	query_string = 'select * from Items where item_ID = $itemID'
    try:
		result = query(query_string, {'itemID': item_id})
	except Exception as e:
		return None
    return result[0]

# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}):
    return list(db.query(query_string, vars))

#####################END HELPER METHODS#####################

#TODO: additional methods to interact with your database,
# e.g. to update the current time

def close_auctions_time():

    query_string = 'select itemID from Items where Ends < $curr_time'
    list = query(query_string, {'curr_time': getTime()});
    for item in list:
        t = db.transaction()
        try:
            db.query('delete from Items where itemID = $item', {'item': item});
        except Exception as e:
            t.rollback()
            print str(e)
            return False
        else:
            t.commit()
            return True



def close_auctions_price():
    query_string = 'select itemID from Items where Currently > Buy_Price or Currnetly = Buy_Price'
    list = query(query_string);
    for item in list:
        t = db.transaction()
        try:
            db.query('delete from Items where itemID = $item', {'item': item});
        except Exception as e:
            t.rollback()
            print str(e)
            return False
        else:
            t.commit()
            return True


def get_item_attributes(ItemID):
    return db.query('select * from Items where ItemID = $ItemID', {'ItemID' : ItemID})

def get_item_categories(ItemID):
    return db.query('select Categories from Items where ItemID = $ItemID', {'ItemID': ItemID});

def get_item_status(ItemID):
    list = db.query('select Categories from Items where ItemID = $ItemID', {'ItemID': ItemID});
    if (len(list) == 0): return 'closed';
    else: return 'open';

def get_auction_bids(ItemID):
    if (get_item_status(ItemID) == 'open'):
        return db.query('select UserID, Time, Amount from Bids where ItemID = $ItemID', {'ItemID': ItemID});
    else:
        return db.query('select UserID from Bids where Amount = (select MAX(Amount) from Bids)')

