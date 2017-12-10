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

# searches for an item given the input
def search(itemID, category, userID, minPrice, maxPrice, status, description):
    query_string = 'select distinct i.Name, i.ItemID, i.Seller_UserID, i.Currently, i.Number_of_Bids, i.Buy_Price, i.Started, i.Ends, '
    query_string += 'i.Description from Items i join Categories c on i.ItemID = c.ItemID where '
    inVars = {}
    time = getTime()
    
    #put the query together using the input parameters   
    if (itemID != ''):
        query_string += 'i.ItemID = $itemID and '
        inVars['itemID'] = itemID
        
    if (category != ''):
        query_string += 'c.Category = $category and '
        inVars['category'] = category
        
    if (userID != ''):
        query_string += 'i.Seller_UserID = $userID and '
        inVars['userID'] = userID
    
    try:
        if (minPrice != ''):
            minInt = int(minPrice)
        if (maxPrice != ''):
            maxInt = int(maxPrice)
    except Exception as e:
        print str(e)
        return None
    
    if (minPrice != '' and maxPrice != '' and maxInt < minInt):
        return None
    
    if (minPrice != ''):
        query_string += '(i.Currently >= $minPrice or i.Buy_Price >= $minPrice) and '
        inVars['minPrice'] = minInt
        
    if (maxPrice != ''):
        query_string += '(i.Currently <= $maxPrice or i.Buy_Price <= $maxPrice) and '
        inVars['maxPrice'] = maxInt
        
    if (description != ''):
        description = '%' + description + '%'
        query_string += 'i.Description like $description and '
        inVars['description'] = description
        
    if (status != 'All'):
        if (status == 'open'):
            query_string += '$time >= i.Started and $time < i.Ends'
            
        elif (status == 'close'):
            query_string += '$time >= i.Ends'
            
        elif (status == 'notStarted'):
            query_string += '$time < i.Started'
            
        inVars['time'] = time
        
    if query_string.endswith(' and '):
        query_string = query_string[:-5]
    #adding where gives all results -- memory overload
    #elif query_string.endswith(' where '):
        #query_string = query_string[:-7]
        
    #query_string += ' group by i.ItemID'
                       
    # try to do the search
    t = transaction()
    try:
        results = query(query_string, inVars)
    except Exception as e:
        t.rollback()
        print str(e)
        return None
    else:
        t.commit() 
        return results

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


def get_item_attributes(ItemID):
    return db.query('select * from Items where ItemID = $ItemID', {'ItemID' : ItemID})

def get_item_categories(ItemID):
    return db.query('select Category from Categories where ItemID = $ItemID', {'ItemID': ItemID});

def get_item_status(ItemID):
    end_time = query('select Ends from Items where ItemID = $ItemID', {'ItemID': ItemID})
    buy_price = query('select Buy_Price from Items where ItemID = $ItemID', {'ItemID': ItemID})
    currently = query('select currently from Items where ItemID = $ItemID', {'ItemID': ItemID})
    if (end_time >= getTime() or currently >= buy_price): return 'closed'
    else: return 'open';

def get_auction_bids(ItemID):
    return db.query('select UserID, Time, Amount from Bids where ItemID = $ItemID', {'ItemID': ItemID});

def get_auction_winner(ItemID):
    if (get_item_status(ItemID) == 'closed'):
        return db.query('select UserID from Bids where Amount = (select MAX(Amount) from Bids where ItemID = $ItemID)', {'ItemID': ItemID});
    else: return 'Auction is still open'

		
def add_bid(ItemID, UserID, Price):
	current_time = db.query('select Time from CurrentTime')
	query_string = 'insert into Bids (ItemID, UserID, Amount, Time) values ($ItemID, $UserID, $Price, $current_time)'
    
    # try to add the bid
    t = transaction()
    try:
        db.query(query_string, {'ItemID': ItemID, 'UserID': UserID, 'Price': Price, 'current_time': current_time})
    except Exception as e:
        t.rollback()
        return False
    else:
        t.commit() 
        return True
	
