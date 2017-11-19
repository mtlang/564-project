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
		file = new File(indexName,true);
	}

	// Initialize fields
	bufMgr = bufMgrIn;
	attributeType = attrType;
	attrByteOffest = attrByteOffset;

	// Scan given relation
	FileScan scanner = new FileScan(relationName, bufMgrIn);
	RecordId outRid;
	// Scan until end of file is reached
	try {
		while (true) {
			scanNext(outRid);
			// Insert found record
			//TODO: Find key value for the record
			void * key = 
			insertEntry(key, outRid);
		}
	} catch (EndOfFileException eof) {
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
	//TODO: Find pinned tree pages
	// bufMgr->unPinPage(file,pageno,dirty);
	// Flush index file
	bufMgr->flushFile(file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

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
