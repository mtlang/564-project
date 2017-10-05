
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014
Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:
1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.
Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"
Items_s = ""
Category_s = ""
Bid_s = ""
Users_s = ""
Users_d = {}


# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)



"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        for item in items:
            global Items_s
            global Category_s
            global Bid_s
            global Users_s
            global Users_d

            #create Items String
            name = item['Name']
            name = name.replace('"','""')

            Items_s += item['ItemID'] + '|' + '"' + name + '"' + '|' + transformDollar(item['Currently']) + '|'
            try:
                Items_s += transformDollar(item['Buy_Price']) + '|'
            except KeyError:
                Items_s += '\"NULL\"' + '|'

            seller = item['Seller']['UserID']
            seller = seller.replace('"','""')
            location = item['Location']
            location = location.replace('"', '""')
            country = item['Country']
            country = country.replace('"', '""')
            #description = item['Description']

            Items_s += transformDollar(item['First_Bid']) + '|' + item['Number_of_Bids'] + '|' + transformDttm(item['Started']) + '|' + transformDttm(item['Ends'])+ '|' + '"' + seller + '"' + '|'
            Items_s += '"' + location + '"' + '|' + '"' + country + '"' + '|'

            """
            try:
                description = description.replace('"', '""')
                Items_s += '"' + description + '"' + '\n'
            except:
                Items_s += '\"NULL\"' + '\n'
            """
            #create Category String and Users String
            for categories in item['Category']:
                categories = categories.replace('"','""')
                Category_s += item['ItemID'] + '|' + '"' + categories + '"' + '\n'

            #create Bids String
            if item['Number_of_Bids'] != '0':
                for bid in item['Bids']:
                    bidder = bid['Bid']['Bidder']['UserID']
                    bidder = bidder.replace('"','""')
                    Bid_s += item['ItemID'] + '|' + '"' + bidder + '"' + '|' + transformDttm(bid['Bid']['Time']) + '|' + transformDollar(bid['Bid']['Amount']) + '\n'
                    if Users_d.has_key(bid['Bid']['Bidder']['UserID']):
                        pass
                    else:
                        try:
                            location = bid['Bid']['Bidder']['Location']
                            location = location.replace('"','""')
                            country = bid['Bid']['Bidder']['Country']
                            country = country.replace('"','""')
                            Users_d[bid['Bid']['Bidder']['UserID']] = '"' + location + '"' + '|' + '"' + country + '"' + '|' + bid['Bid']['Bidder']['Rating']
                        except KeyError:
                                try:
                                    country = bid['Bid']['Bidder']['Country']
                                    country = country.replace('"','""')
                                    Users_d[bid['Bid']['Bidder']['UserID']] = '\"NULL\"' + '|' + '"' + country + '"' + '|' + bid['Bid']['Bidder']['Rating']
                                except KeyError:
                                    try:
                                        location = bid['Bid']['Bidder']['Location']
                                        location = location.replace('"','""')
                                        Users_d[bid['Bid']['Bidder']['UserID']] = '"' + location + '"' + '|' + '\'NULL\'' + '|' + bid['Bid']['Bidder']['Rating']
                                    except:
                                        Users_d[bid['Bid']['Bidder']['UserID']] = '\"NULL\"' + '|' + '\"NULL\"' + '|' + bid['Bid']['Bidder']['Rating']

                        Users_s += '\"' +bid['Bid']['Bidder']['UserID'] + '\"' + '|' + Users_d[bid['Bid']['Bidder']['UserID']] + '\n'


            #create Users Dictionary
            if Users_d.has_key(item['Seller']['UserID']):
                pass
            else:
                Users_d[item['Seller']['UserID']] = '\"NULL\"' + '|' + '\"NULL\"' + '|' + item['Seller']['Rating']
                seller = item['Seller']['UserID']
                seller = seller.replace('"','""')
                Users_s += '"' + seller + '"' + '|' + '\"NULL\"' + '|' + '\"NULL\"' + '|' + item['Seller']['Rating'] + '\n'
    f.close()


"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):

    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)

    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print "Success parsing " + f

    #writes global strings to .dat files
    file = open('Items.dat', 'w')
    file.write(Items_s)
    file.close()

    file = open('Categories.dat', 'w')
    file.write(Category_s)
    file.close()

    file = open('Users.dat', 'w')
    file.write(Users_s)
    file.close()

    file = open('Bids.dat', 'w')
    file.write(Bid_s)
    file.close()

if __name__ == '__main__':
    main(sys.argv)