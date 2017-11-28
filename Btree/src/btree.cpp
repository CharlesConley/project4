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
scanExecuting = true;

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
LeafNodeInt* cnode = 0;
int nextPage = 0;

//check if this is called before a startScan call
if(scanExecuting == false){throw ScanNotInitializedException();}
//check if page num is 0. throw exception
if(currentPageNum == 0){throw IndexScanCompletedException();}
//get node from currentPageData
cnode = (LeafNodeInt*)currentPageData;
//check if its at the end of a page. throw exception
if(cnode->rightSibPageNo == 0){throw IndexScanCompletedException();}
nextPage = cnode->ridArray[nextEntry].page_number;
//chekc if end of page. if yes unpin then read
if((nextEntry == INTARRAYLEAFSIZE) || (nextPage == 0)){
//unpin page
bufMgr->unPinPage(file, currentPageNum, false);
//goto next page. read it.
bufMgr->readPage(file,cnode->rightSibPageNo,currentPageData);
nextEntry =0;
}
int k = cnode->keyArray[nextEntry]; 
//now check if key/rid works
while(compK(lowValInt,lowOp,highValInt, highOp, k) == false){
if(outRid == cnode->ridArray[nextEntry]){
//found a match. throw finish exception
throw  IndexScanCompletedException();
}
nextEntry++;

}

}

bool BTreeIndex::compK(int lowValInt,const Operator lowOp,int highValInt,const Operator highOp, int key){

int lVal = (lowValInt);
int hVal = (highValInt);
bool retVal;
if(lowOp == GTE){
	if(highOp == LTE)
		retVal =(key >= lVal && key <=hVal);
		return retVal; 
	if(highOp == LT)
		retVal = (key < hVal && key >= lVal);
		return retVal;
}
else(lowOp == GT);{
	if(highOp == LTE)
		retVal = (key <= hVal && key > lVal);
		return retVal;
	if(highOp == LT)
		retVal = (key < hVal && key > lVal);
		return retVal;
}

}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
//check if this is called before a startScan call
if(scanExecuting == false){throw ScanNotInitializedException();}
//end the scan
scanExecuting = false;
//unpin page
bufMgr->unPinPage(file, currentPageNum, false);
}
//reset scan spefific variables

}
