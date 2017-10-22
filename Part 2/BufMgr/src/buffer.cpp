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
	for (int i = 0; i < numBufs; i++)
	{
		if (bufDescTable[i].dirty) 
		{
			File->writePage(&bufPool[i]);
			bufDescTable[i].dirty = false;
		}
	}
	
	// Deallocate bufPool and bufDescTable
	delete bufPool;
	delete bufDescTable;
	
}

void BufMgr::advanceClock()
{

}

void BufMgr::allocBuf(FrameId & frame) 
{
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	// Allocate an empty page
	Page newPage = file->allocatePage();
	
	// Obtain a buffer pool frame
	FrameId bufFrame;
	allocBuf(&bufFrame);
	
	// Insert entry into hash table
	hashTable.insert(file, *pageNo, bufFrame);
	
	// Set up frame
	bufDescTable[bufFrame].Set(file, *pageNo);
	
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	Page * toDispose = file->readPage(PageNo, false);
    // If page is in buffer pool...
	Page * inPool = std::find(std::begin(bufPool), std::end(bufPool), *toDispose);
	if (inPool != std::end(bufPool)) 
	{
		// Free page from pool
		*inPool = null;
		// Remove page from hash table
		hashTable.remove(file,PageNo);
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
