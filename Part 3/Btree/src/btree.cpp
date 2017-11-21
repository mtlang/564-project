/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	// Get name of index file
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();
	// Return the name
	outIndexName = indexName;
	
	// If the index file exists...
	if (file->exists(indexName)) {
		// Open the file
		file->openIfNeeded(false);
	}
	// Otherwise create the index file
	else {
		file = BlobFile.create(indexName);
	}

	// Initialize fields
	bufMgr = bufMgrIn;
	attributeType = attrType;
	this->attrByteOffest = attrByteOffset;

	// Scan given relation
	FileScan scanner = new FileScan(relationName, bufMgrIn);
	RecordId outRid;
	// Scan until end of file is reached
	try {
		while (true) {
			// Get record ID
			scanner.scanNext(outRid);
			// Find key value for the record
			std::string sRecord = scanner.getRecord();
			char * rec = sRecord.c_str();
			void * key = (void *)(rec + attrByteOffest);
			// Insert the entry
			insertEntry(key, outRid);
		}
	} catch (EndOfFileException eof) {
	// Intentionally empty, exception is caught when all records have been inserted
	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// Clear state variables
	scanExecuting = false;
	nextEntry = 0;
	currentPageNum = 0;
	// Unpin all pinned tree pages
	bufMgr->unPinPage(file,currentPageNum,true);
	// Flush index file
	bufMgr->flushFile(file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    //read in the root page; try to cast it to a leaf/non-leaf node
    NonLeafNodeInt* node = &(file->readPage(rootPageNum));   
    LeafNodeInt* leaf = null;
    bool found;
    int i, leafLength, insertAt = -1;   
    
    //start at root node and traverse to the correct leaf node
    while(leaf == null) {
        found = false;
    
        //find the correct range for the parameter key
        //when found, always check the level to see if the next page is a nonleaf or leaf node
        for (i = 0; i < INTARRAYNONLEAFSIZE && node.pageNoArray[i] != null && !found; i++) {
            
            //check if key is less than the smallest value
            if (i == 0) {
                if (*key < node.keyArray[0]) {
                    found = true;
                    if (node.level == 1) {
                        leaf = &(file->readPage(node.pageNoArray[0]));
                    }
                    else {
                        node = &(file->readPage(node.pageNoArray[0]));
                    }                    
                }
            }
            //the key must be larger than the largest value
            else if (i == INTARRAYNONLEAFSIZE - 1) {
                found = true;
                if (node.level == 1) {
                    leaf = &(file->readPage(node.pageNoArray[i + 1]));
                }
                else {
                    node = &(file->readPage(node.pageNoArray[i + 1]));
                }
            }
            else {
                //check that the key is inbetween the two given values
                if (*key >= node.keyArray[i] && *key < node.keyArray[i + 1]) {
                    found = true;
                    if (node.level == 1) {
                        leaf = &(file->readPage(node.pageNoArray[i + 1]));
                    }
                    else {
                        node = &(file->readPage(node.pageNoArray[i + 1]));
                    }
                }
            }
        }
        
    }
    
    //determine how many values are in the leaf
    for (i = 0; i < INTARRAYLEAFSIZE && leaf.keyArray[i] != null) {
        //the leaf is full
        if (i == INTARRAYLEAFSIZE - 1) {
            leafLength = INTARRAYLEAFSIZE;
        }
        //check if the next value is null
        else if (leaf.keyArray[i + 1] == null) {
            leafLength = i + 1;
        }
    }
    
    //find the spot in the leaf node to insert into
	//check beginning of leaf
	if (*key < leaf.keyArray[0]) {
        insertAt = i;
    }
	//check between existing entries
	else {
		for (i = 0; i < leafLength-1 && insertAt == -1; i++) {
			else if (*key >= leaf.keyArray[i] && *key < leaf.keyArray[i + 1]) {
				insertAt = i + 1;
			}
		}
    }
	//if insertAt still isn't set, entry belongs at the end
    if (insertAt == -1) {
		insertAt = leafLength;
	}
	
	
    //if the leaf is full, split
    if (leafLength == INTARRAYLEAFSIZE) {
        
    }
    //leaf is not full; insert at the correct index
    //must shift previous array entries
    else {       
        for (i = leafLength; i > insertAt; i--) {
            leaf.keyArray[i] = leaf.keyArray[i - 1];
            leaf.ridArray[i] = leaf.ridArray[i - 1];
        }
        leaf.keyArray[insertAt] = *key;
        leaf.ridArray[i] = rid;
    }

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    //check for in-progress scan
    if (scanExecuting) {
        endScan();
    }
    
    //check for exceptions
    if (*lowValParm > *highValParm) {
        throw BadScanrangeException();
    }
    if (lowOpParm != "GT" && lowOpParm != "GTE") {
        throw BadOpcodesException();
    }
    if (highOpParm != "LT" && highOpParm != "LTE") {
        throw BadOpcodesException();
    }
    
    //set scanning params
    scanExecuting = true;
    lowOp = lowOpParm;
    highOp = highOpParm;
    
    //check the type of the low/high parms
    
    
    //starting at root, traverse index tree; throw exception if no matches found
    
    
}
	
// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
