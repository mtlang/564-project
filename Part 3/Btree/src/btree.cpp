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
            *file = BlobFile::open(indexName);
            std::cout << "Done opening existing\n";
        }
        // Otherwise create the index file
        else {
            file = new BlobFile(indexName, true);
            std::cout << "Done creating nonexisting\n";
        }
    
        // Initialize fields
        bufMgr = bufMgrIn;
        attributeType = attrType;
        this->attrByteOffset = attrByteOffset;
        nodeOccupancy = INTARRAYNONLEAFSIZE;
        leafOccupancy = INTARRAYLEAFSIZE;
        scanExecuting = false;
        nextEntry = 0;
        
        std::cout << "leaf occupancy: " << leafOccupancy << " node occupancy: " << nodeOccupancy << "\n";
        
        //Create meta data page and root page; initialize their values       
        Page* headerPage;
        Page* rootPage;
        bufMgr->allocPage(file, headerPageNum, headerPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);
        IndexMetaInfo* header = (IndexMetaInfo*)headerPage;
        NonLeafNodeInt* root = (NonLeafNodeInt*)rootPage;
        
        strcpy(header->relationName, relationName.c_str());
        header->attrByteOffset = attrByteOffset;
        header->attrType = attrType;
        header->rootPageNo = rootPageNum;
        
        root->occupancy = 0;
        root->level = 1;
        
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);

        // Scan given relation
        FileScan* scanner = new FileScan(relationName, bufMgrIn);
        RecordId outRid;
        // Scan until end of file is reached
        try {
            std::cout << "Starting scanning\n";
            while (true) {
                // Get record ID
                scanner->scanNext(outRid);
                // Find key value for the record
                std::string sRecord = scanner->getRecord();
                const char * rec = sRecord.c_str();
                void * key = (void *)(rec + attrByteOffset);
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
		bufMgr->unPinPage(file, currentPageNum, true);
		// Flush index file
		bufMgr->flushFile(file);
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
	{
        std::cout << "Insert Entry: " << *(int*)key << " " << rid.page_number << "\n";
		//read in the root page; try to cast it to a leaf/non-leaf node
        Page* nodePage;
		bufMgr->readPage(file, rootPageNum, nodePage);
        NonLeafNodeInt* node = (NonLeafNodeInt*)nodePage;
		LeafNodeInt* leaf = NULL;
        Page* leafPage;
		bool found;
		int i, insertAt = -1;
        PageId pageNum1, pageNum2;
        std::list<Page*> nodes;
        int* keyInt = (int*)key;
        PageId leafPageNo;

        //check that root isn't empty
        if (node->occupancy == 0) {
            std::cout << "Empty root\n";
            bufMgr->allocPage(file, pageNum1, leafPage);
            leaf = (LeafNodeInt*)leafPage;
            Page* leafPage2;
            bufMgr->allocPage(file, pageNum2, leafPage2);
            LeafNodeInt* leaf2 = (LeafNodeInt*)leafPage2;
            
            //set root node's array values
            node->keyArray[0] = *keyInt;
            node->occupancy++;
            node->pageNoArray[0] = pageNum1;
            node->pageNoArray[1] = pageNum2;
            
            //set first leaf node's values
            leaf->occupancy = 0;
            leaf->rightSibPageNo = pageNum2;
            
            //set second leaf node's values
            leaf2->keyArray[0] = *keyInt;
            leaf2->occupancy = 1;
            leaf2->ridArray[0] = rid;
            leaf2->rightSibPageNo = 0;
            
            bufMgr->unPinPage(file, pageNum1, true);
            bufMgr->unPinPage(file, pageNum2, true);
            bufMgr->unPinPage(file, rootPageNum, true);
            
            return;
        }
        else {
            bufMgr->unPinPage(file, rootPageNum, false);
        }
        
		//start at root node and traverse to the correct leaf node
		while (leaf == NULL) {
			found = false;
            nodes.push_front(nodePage);
            //previousPageNum = currentPageNum;
            
            //while (*keyInt > 1005) {
                
            //}
            
			//find the correct range for the parameter key
			//when found, always check the level to see if the next page is a nonleaf or leaf node
			for (i = 0; i < INTARRAYNONLEAFSIZE && i < node->occupancy && !found; i++) {
                std::cout << "Searching for key " << *keyInt << " at " << i << " with node " << node->keyArray[i] << "\n";
                std::cout << "Rid: " << rid.page_number << " " << rid.slot_number << "\n";
                
				//check if key is less than the smallest value
				if (i == 0) {
					if (*keyInt < node->keyArray[0]) {
                        std::cout << "found!!!!\n";
						found = true;
                        leafPageNo = node->pageNoArray[0];
						if (node->level == 1) {
							bufMgr->readPage(file, leafPageNo, leafPage);
						}
						else {
							bufMgr->readPage(file, node->pageNoArray[0], nodePage);
                            bufMgr->unPinPage(file, node->pageNoArray[0], false);
						}
					}
                    //case of one entry in the node
                    else if (node->occupancy == 1) {
                        if (leaf == NULL || (leaf != NULL && leaf->occupancy < INTARRAYLEAFSIZE)) {
                            std::cout << "one entry in node; >=\n";
                            found = true;
                            leafPageNo = node->pageNoArray[1];
                            if (node->level == 1) {
                                bufMgr->readPage(file, leafPageNo, leafPage);
                            }
                            else {
                                bufMgr->readPage(file, node->pageNoArray[1], nodePage);
                                bufMgr->unPinPage(file, node->pageNoArray[1], false);
                            }
                        }                        
                    }
				}
				//the key must be larger than the largest value
				else if (i == (node->occupancy - 1) && *keyInt > node->keyArray[i]) {
                    std::cout << "Larger than largest value\n";
					found = true;
                    leafPageNo = node->pageNoArray[i + 1];
					if (node->level == 1) {
						bufMgr->readPage(file, leafPageNo, leafPage);
					}
					else {
						bufMgr->readPage(file, node->pageNoArray[i + 1], nodePage);
                        bufMgr->unPinPage(file, node->pageNoArray[i + 1], false);
					}
				}
				else {
					//check that the key is inbetween the two given values
					if (*keyInt >= node->keyArray[i] && *keyInt < node->keyArray[i + 1]) {
						found = true;
                        leafPageNo = node->pageNoArray[i + 1];
						if (node->level == 1) {
							bufMgr->readPage(file, leafPageNo, leafPage);
						}
						else {
							bufMgr->readPage(file, node->pageNoArray[i + 1], nodePage);
                            bufMgr->unPinPage(file, node->pageNoArray[i + 1], false);
						}
					}
				}
                if (leafPage == NULL) {
                    node = (NonLeafNodeInt*)nodePage;
                    nodes.push_front(nodePage);
                }
                else {
                    leaf = (LeafNodeInt*)leafPage;
                }
			}

		}

        std::cout << "Leaf found\n";
        //find the spot in the leaf node to insert into
		for (i = 0; i < leaf->occupancy && insertAt == -1; i++) {
			if (i == 0) {
				if (*keyInt < leaf->keyArray[0]) {
					insertAt = i;
				}
                else if (leaf->occupancy == 1) {
                    insertAt = 1;
                }
			}
			else if (i == leaf->occupancy - 1) {
				if (*keyInt > leaf->keyArray[leaf->occupancy - 1]) {
					insertAt = leaf->occupancy;
				}
			}
			else if (*keyInt >= leaf->keyArray[i] && *keyInt < leaf->keyArray[i + 1]) {
				insertAt = i + 1;
			}
        }
        std::cout << "insertAt: " << insertAt << "\n";
		//if the leaf is full, split
		if (leaf->occupancy == INTARRAYLEAFSIZE) {
            std::cout << "Insert: SPLIT LEAF\n";
            int count = 0;
            PageId splitPageNo;
            Page* splitPage;
            bufMgr->allocPage(file, splitPageNo, splitPage);
            LeafNodeInt* split = (LeafNodeInt*)splitPage;
            
            if (insertAt > INTARRAYLEAFSIZE / 2) {
                //insert the new entry into the new leaf
                for (i = (INTARRAYLEAFSIZE / 2) + 1; i < insertAt; i++) {
                    split->keyArray[count] = leaf->keyArray[i];
                    split->ridArray[count] = leaf->ridArray[i];
                    count++;
                }
                split->keyArray[count] = leaf->keyArray[insertAt];
                split->ridArray[count] = leaf->ridArray[insertAt];
                count++;
                for (i = insertAt + 1; i < INTARRAYLEAFSIZE; i++) {
                    split->keyArray[count] = leaf->keyArray[i];
                    split->ridArray[count] = leaf->ridArray[i];
                    count++;
                }
                split->occupancy = count;
                leaf->occupancy = (leaf->occupancy + 1) - split->occupancy;
            }
            else {
                //copy the second half of the array to the new leaf
                for (i = INTARRAYLEAFSIZE / 2; i < INTARRAYLEAFSIZE; i++) {
                    split->keyArray[count] = leaf->keyArray[i];
                    split->ridArray[count] = leaf->ridArray[i];
                    count++;
                }
                split->occupancy = count;
                //insert the new entry into the original leaf
                leaf->occupancy = (leaf->occupancy + 1) - split->occupancy;
                for (i = INTARRAYLEAFSIZE / 2; i > insertAt; i--) {
                    leaf->keyArray[i] = leaf->keyArray[i - 1];
                    leaf->ridArray[i] = leaf->ridArray[i - 1];
                }                
                leaf->keyArray[i] = *keyInt;
                leaf->ridArray[i] = rid;                
            }
                        
            node = (NonLeafNodeInt*)nodes.front();
            node->keyArray[node->occupancy] = split->keyArray[0];
            node->pageNoArray[node->occupancy + 1] = splitPageNo;
            
            if (leaf->rightSibPageNo != 0) {
                split->rightSibPageNo = leaf->rightSibPageNo;
            }
            else {
                split->rightSibPageNo = 0;
            }
            leaf->rightSibPageNo = splitPageNo;
            node->occupancy++;
            
            while(nodes.front()) {
                node = (NonLeafNodeInt*)nodes.front();
                
                //insertNodeEntry(split->keyArray[], splitPageNo);
                nodes.pop_front();
            }
            bufMgr->unPinPage(file, splitPageNo, true);
            
		}
		else {			
			//insert at the correct index; must shift previous entries
            std::cout << "Insert: NO SPLIT\n";
			for (i = leaf->occupancy; i > insertAt; i--) {
				leaf->keyArray[i] = leaf->keyArray[i - 1];
				leaf->ridArray[i] = leaf->ridArray[i - 1];
			}
			leaf->keyArray[insertAt] = *keyInt;
			leaf->ridArray[insertAt] = rid;
            leaf->occupancy++;
            bufMgr->unPinPage(file, leafPageNo, true);
            std::cout << "leaf occupancy: " << leaf->occupancy << "\n";
		}

        //for (i = 0; i < node->occupancy; i++) {
          //  for (int j = 0; j < ) {
            //    bufMgr->readPage(file, node->pageNoArray[1], nodePage);
            //}
        //}
        
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
        std::cout << "In startScan\n";
        if (scanExecuting) {
            endScan();
        }
        std::cout << "Checked executing\n";
        //check for exceptions
        if (lowOpParm != GT && lowOpParm != GTE) {
            std::cout << "BadOps\n";
            throw BadOpcodesException();
        }
        if (highOpParm != LT && highOpParm != LTE) {
            std::cout << "BadOps\n";
            throw BadOpcodesException();
        }
    
        //set scanning params
        scanExecuting = true;
        lowOp = lowOpParm;
        highOp = highOpParm;
    
        //check that the low/high parms are ints
        try {
            lowValInt = *((int*)lowValParm);
            highValInt = *((int*)highValParm);
        }
        catch (std::exception e) {
            lowValInt = std::stoi(*((std::string*)lowValParm));
            highValInt = std::stoi(*((std::string*)highValParm));
        }
    
        if (lowValInt > highValInt) {
            throw BadScanrangeException();
        }
        
        std::cout << "initialization complete\n";
        
        //traverse index tree; throw exception if no matches found
        //read in the root page; try to cast it to a leaf/non-leaf node
        Page* nodePg;
        bufMgr->readPage(file, rootPageNum, nodePg);
        bufMgr->unPinPage(file, rootPageNum, false);
        NonLeafNodeInt* node = (NonLeafNodeInt*)nodePg;
		LeafNodeInt* leaf = NULL;
        Page* leafPg;
		bool leafFound, entryFound, edge;
		int i, k;
        currentPageNum = rootPageNum;

		//start at root node and traverse to the correct leaf node
		while (leaf == NULL) {
			leafFound = false;
            edge = false;
            std::cout << "traverse tree\n";
			//find the correct range for the parameter key
			//when found, always check the level to see if the next page is a nonleaf or leaf node
			for (i = 0; i < INTARRAYNONLEAFSIZE && i < node->occupancy && !leafFound; i++) {
                std::cout << "" << node->keyArray[i] << "\n";
                //check edge cases
                if (i == 0) {
                    if (highOp == LT) {
                        if (highValInt < node->keyArray[0]) {
                            leafFound = true;
                            edge = true;
                        }
                        if (node->keyArray[0] < highValInt) {
                            if (lowOp == GT) { 
                                if (node->keyArray[0] > lowValInt) { leafFound = true; edge = true; }
                            }
                            else {
                                if (node->keyArray[0] >= lowValInt) { leafFound = true; edge = true; }
                            }
                        }
                    }
                    else {
                        if (highValInt <= node->keyArray[0]) {
                            leafFound = true;
                            edge = true;
                        }
                        if (node->keyArray[0] <= highValInt) {
                            if (lowOp == GT) { 
                                if (node->keyArray[0] > lowValInt) { leafFound = true; edge = true; }
                            }
                            else {
                                if (node->keyArray[0] >= lowValInt) { leafFound = true; edge = true; }
                            }
                        }
                    }
                }
                else if (i == (node->occupancy - 1)) {
                    if (lowOp == GT) {
                        if (lowValInt > node->keyArray[node->occupancy]) {
                            leafFound = true;
                            edge = true;
                        }
                    }
                    else {
                        if (lowValInt >= node->keyArray[node->occupancy]) {
                            leafFound = true;
                            edge = true;
                        }
                    }
                }
                
                //if entry is matches the lower bound condition, set found to true
                if (!edge) {                  
                    if (lowOp == GT) {                   
                        if (lowValInt > node->keyArray[i]) {
                            leafFound = true;
                        }
                    }
                    else {                 
                        if (lowValInt >= node->keyArray[i]) {
                            leafFound = true;
                        }
                    }
                
                    //if the entry doesn't match the upper bound condition and found is true, set found to false
                    if (highOp == LT) {
                        if (!(highValInt < node->keyArray[i + 1])) {
                            if (leafFound) {
                                leafFound = false;
                            }
                        }
                    }
                    else {
                        if (!(highValInt <= node->keyArray[i + 1])) {
                            if (leafFound) {
                                leafFound = false;
                            }
                        }
                    }
                }
                
                std::cout << "found: " << leafFound << "\n";
                if (leafFound) {
                    entryFound = false;
                    std::cout << "level: " << node->level << "\n";
                    if (node->level == 1) {
                        //if the low value is equal to the entry, must go to next entry,
                        //since next entry will be >= the matched value
                        if (lowValInt == node->keyArray[i]) {
                            currentPageNum = node->pageNoArray[i + 2];
                            bufMgr->readPage(file, currentPageNum, leafPg);
                            bufMgr->unPinPage(file, currentPageNum, false);
                        }
                        else {
                            currentPageNum = node->pageNoArray[i + 1];
                            bufMgr->readPage(file, currentPageNum, leafPg);
                            bufMgr->unPinPage(file, currentPageNum, false);
                        }
                        leaf = (LeafNodeInt*)leafPg;
                        currentPageData = (Page*)leaf;
                        //find the next entry that satisfies the conditions
                        for (k = 0; k < leaf->occupancy && !entryFound; k++) {
                            if (lowOp == GT) {
                                if (leaf->keyArray[k] > lowValInt) { entryFound = true; }
                            }
                            else {
                                if (leaf->keyArray[k] >= lowValInt) { entryFound = true; }
                            }
                            std::cout << "entryFound after lowOP: " << entryFound << "\n";
                            if (highOp == LT) {
                                if (!(node->keyArray[k] < highValInt)) {
                                    if (entryFound) { entryFound = false; }
                                }
                            }
                            else {
                                if (!(node->keyArray[k] <= highValInt)) {
                                    if (entryFound) { entryFound = false; }
                                }
                            }
                            std::cout << "entryFound after highOp: " << entryFound << "\n";
                            if (entryFound) {
                                nextEntry = leaf->keyArray[k];
                                std::cout << "nextEntry found: " << nextEntry << "\n";
                            }
                        }                       
                    }
                    else {
                        if (lowValInt == node->keyArray[i]) {
                            bufMgr->readPage(file, node->pageNoArray[i + 1], nodePg);
                            bufMgr->unPinPage(file, node->pageNoArray[i + 1], false);
                        }
                        else {
                            bufMgr->readPage(file, node->pageNoArray[i], nodePg);
                            bufMgr->unPinPage(file, node->pageNoArray[i], false);
                        }
                    }                 
                }
                
			}
            //if iteration at a level completes without finding a match, throw an exception
            if (!leafFound) {
                throw NoSuchKeyFoundException();
            }

		}
        //must have found a matching leaf to get here        
    
    }

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId& outRid)
	{
        std::cout << "" << outRid.page_number << " " << outRid.slot_number << "\n";
		//check to make sure the current page is valid 
		if (currentPageNum == 0) throw IndexScanCompletedException();

        std::cout << "nextEntry: " << nextEntry << "\n";
		LeafNodeInt* curr = (LeafNodeInt*)currentPageData;
		int nextKey = curr->keyArray[nextEntry];

		//if the high operator is "less than" the scan is complete when the next key is >= to the high int
		if (highOp == LT && nextKey >= highValInt) {
			throw IndexScanCompletedException();
		}
		//if the high operator is LTE the scan is complete when the next key exceeds the high int
		if (highOp == LTE && nextKey > highValInt) {
			throw IndexScanCompletedException();
		}

		nextEntry++;
		outRid = curr->ridArray[nextEntry];
        std::cout << "" << outRid.page_number << " " << outRid.slot_number << "\n";
        std::cout << "key: " << curr->keyArray[nextEntry] << "\n";
        
        std::cout << "in scanNext\n";

		//if you reach the end of the page change the current page to the next
		if (curr->ridArray[nextEntry].page_number == 0 || nextEntry == leafOccupancy) {
            
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = curr->rightSibPageNo;
			outRid = ((LeafNodeInt*)currentPageData)->ridArray[0];
            
            std::cout << "PageNum: " << currentPageNum << "\n";

			//if a right sibling exists read the current page and unpin it
			//and reset nextEntry
			if (curr->rightSibPageNo != 0) {
				bufMgr->readPage(file, currentPageNum, currentPageData);
				nextEntry = 0;
			}
			
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	const void BTreeIndex::endScan()
	{
		//check to see if scan is executing
		if (!scanExecuting) throw ScanNotInitializedException();

		//stop the scan and reset state
		scanExecuting = false;
		nextEntry = 0;
		currentPageNum = 0;

		//unpin current page
		if (currentPageNum != 0) bufMgr->unPinPage(file, currentPageNum, false);

	}

}
