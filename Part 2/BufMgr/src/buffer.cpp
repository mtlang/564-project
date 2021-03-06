/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	// Flush dirty pages
	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		if (bufDescTable[i].dirty)
		{
			bufDescTable[i].file->writePage(bufPool[i]);
			bufDescTable[i].dirty = false;
		}
	}

	// Deallocate bufPool and bufDescTable
	delete bufPool;
	delete bufDescTable;
}

//Increments clockhand clockwise. If clockhand points to the last page in the pool, set to zero
void BufMgr::advanceClock()
{
	if (clockHand < numBufs - 1) {
		clockHand++;
	}
	else {
		clockHand = 0;
	}
}
//evicts page in buffer pool using clock replacement policy
//clears frame at FrameID passed to allocBuf
void BufMgr::allocBuf(FrameId & frame) 
{
		advanceClock();
		bool validSet = bufDescTable[clockHand].valid;
		bool refBitSet = bufDescTable[clockHand].refbit;
		bool pinned = bufDescTable[clockHand].pinCnt > 0;
		bool dirty = bufDescTable[clockHand].dirty;
		FrameId clockStart = clockHand;

		//check if all frames are pinned
		while (bufDescTable[clockHand].pinCnt > 0) {
			advanceClock();
			//loop through the buffer pool until an unpinned page is found
			//if the hand makes it all around they're all pinned
			if (clockHand == clockStart)
				throw new BufferExceededException;
		}

		//reset clockHand to where it started and begin searching for a page to evict
		clockHand = clockStart;
		while (validSet && (refBitSet || pinned)) {
			if (refBitSet)
				bufDescTable[clockHand].refbit = false;
			advanceClock();
			validSet = bufDescTable[clockHand].valid;
			refBitSet = bufDescTable[clockHand].refbit;
			pinned = bufDescTable[clockHand].pinCnt > 0;
			dirty = bufDescTable[clockHand].dirty;

		}
		if (dirty && validSet)
			flushFile(bufDescTable[clockHand].file);

		//once victim page is found, the page at the requested FrameID (frame) is moved to the victim pages slot
		//the requested frame is then cleared 
        frame = bufDescTable[clockHand].frameNo;
        return;
		//bufDescTable[clockHand].Set(bufDescTable[frame].file, bufDescTable[frame].pageNo);
		//bufDescTable[frame].Clear();


}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frameNo;
    try {
        hashTable->lookup(file, pageNo, frameNo);
        bufDescTable[frameNo].refbit = 1;
        bufDescTable[frameNo].pinCnt++;
	page = &bufPool[frameNo];
        return;
    }
    catch (const HashNotFoundException ex) {
        allocBuf(frameNo);
        bufPool[frameNo] = (*file).readPage(pageNo);
        hashTable->insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file, pageNo);
        page = &bufPool[frameNo];
        return;
    }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	//check if page exists in bufferpool
	FrameId clockStart = clockHand;
	bool notFound;
	while (bufDescTable[clockHand].pageNo != pageNo || bufDescTable[clockHand].file != file) {
		advanceClock();
		//loop through the buffer pool until desired page is found
		//if the hand makes it all around it's not in the buffer pool
		if (clockHand == clockStart)
			notFound = true;
	}

	if (!notFound) {
		//std::cout << "bufDescTable[" << clockHand << "] = " << bufDescTable[clockHand].pinCnt << "\n";
		if (bufDescTable[clockHand].pinCnt == 0) {
			throw new PageNotPinnedException("Page not pinned", pageNo, clockHand);
		}

		else {
			if (dirty) {
				bufDescTable[clockHand].dirty = true;
			}
			bufDescTable[clockHand].pinCnt--;
		}

	}
	clockHand = clockStart;
}

void BufMgr::flushFile(const File* file) 
{
    for (unsigned int i = 0; i < numBufs; i++) {
        
        if (bufDescTable[i].file == file) {
            
            if (bufDescTable[i].pinCnt > 0) {
                throw new PagePinnedException("Page pinned", bufDescTable[i].pageNo, bufDescTable[i].frameNo);
            }
            if (!bufDescTable[i].valid) {
                throw new BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, 
                  bufDescTable[i].refbit);
            }
            
            if (bufDescTable[i].dirty) {
                FrameId frameNo = bufDescTable[i].frameNo;
                bufDescTable[i].file->writePage(bufPool[frameNo]);
                bufDescTable[i].dirty = 0;
            }
            hashTable->remove(file, bufDescTable[i].pageNo);
            bufDescTable[i].Clear();
        }
        
    }
    
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
	// Allocate an empty page
	Page newPage = file->allocatePage();
	pageNo = newPage.page_number();

	// Obtain a buffer pool frame
	FrameId bufFrame;
	allocBuf(bufFrame);

	// Insert entry into hash table
	hashTable->insert(file, pageNo, bufFrame);

	// Set up frame
	bufDescTable[bufFrame].Set(file, pageNo);
	bufPool[bufFrame] = newPage;
	page = &bufPool[bufFrame];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	Page toDispose = file->readPage(PageNo);

	// If page is in buffer pool...
	int indexInPool = -1;
	for (std::uint32_t i = 0; i < numBufs; i++) {
		if (toDispose.getFreeSpace() == bufPool[i].getFreeSpace()
			&& toDispose.page_number() == bufPool[i].page_number()
			&& toDispose.next_page_number() == bufPool[i].next_page_number()) {
		indexInPool = i;
		}
	}
	if (indexInPool != -1)
	{
		// Free page from pool
		bufPool[indexInPool] = Page();
		// Remove page from hash table
		hashTable->remove(file, PageNo);
	}
	// Delete page from file
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
