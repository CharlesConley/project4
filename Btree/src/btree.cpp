/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"

#include "exceptions/file_exists_exception.h"
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
    this->attrByteOffset = attrByteOffset;
    this->attributeType = attrType;

    ///Description indicates that only Integer Datatypes will be used
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    ///define bufMgr for Btree
    bufMgr = bufMgrIn;
    
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str();
    outIndexName = indexName;
    ///instance of IndexMeta
    Page * metaHeaderPage;
    Page * rootPage;
    struct IndexMetaInfo * metaInfo;
    PageId rPageNum;
    headerPageNum = 1;
    
    ///try to create a new blobfile, catch FileExistsException
    try{
        
        file = new BlobFile(indexName, true);
        ///file didn't exist and it now needs to be created.
        
        ///write the constructor arguments into IndexMetaInfo, then write to file.
        
        ///allocate space for Meta Info page
        bufMgr->allocPage(file, headerPageNum, metaHeaderPage);
        bufMgr->allocPage(file, rPageNum, rootPage);
        
        rootPageNum = rPageNum;
        
        metaInfo = (IndexMetaInfo*)metaHeaderPage;
        
        metaInfo->rootPageNo = rPageNum;
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->attrType = attrType;
        strcpy((char*)&(metaInfo->relationName), (const char*)&relationName);
        std::cout << metaInfo->relationName << " " << relationName.c_str() << std::endl;
        
        ///Insert the entries related to the relation into the index file.
        FileScan fScan = FileScan(relationName, bufMgr);
        NonLeafNodeInt *rootNode;
        rootNode = (NonLeafNodeInt*)rootPage;
        ///Initialize rootNode as nonLeafNodeInt, set all pages to 0
        rootNode->level = 1;
        
        rootNode->pageNoArray[INTARRAYNONLEAFSIZE] = {};
        
        
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
        
        ///read all records through filescan->scanNext, prepare entry, call insertEntry
        RecordId tmpRec;
        std::string tmpStr;
        int* key;
        try{
            while(1){
                fScan.scanNext(tmpRec);
                tmpStr = fScan.getRecord();
                key = (int*)&tmpStr + attrByteOffset;
                insertEntry(key, tmpRec);
            }
            
        }catch(EndOfFileException e){
    
            std::cout << "Finished creating index file" << std::endl;
            
        }
        bufMgr->flushFile(file);
        //fScan.~FileScan();
    }catch(FileExistsException e){ ///file already exists. Check meta file and load entries
        
        file = new BlobFile(indexName, false);
        
        ///read the first page of the file. This will conatin the IndexMetaInfo
        bufMgr->readPage(file, 1, metaHeaderPage);
        
        metaInfo = (IndexMetaInfo*)metaHeaderPage;
        std::cout << metaInfo->relationName << " " << relationName << std::endl;
        ///Datatype will always be int. Check attrByteOffset and relation name to make sure this is the correct index
        if(strcmp((char *)&(metaInfo->relationName),(const char *)&relationName) != 0){
            
            
            throw BadIndexInfoException("The Relation in the indexFile is not the same as the tree");
        }
        rootPageNum = metaInfo->rootPageNo;
        bufMgr->unPinPage(file, 1, false);
    }
    
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{ ///call destructor of blobfile and flush the indexfile from buffer
    
    file->~File();
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
	// check low and high operators are correct, if not, throw BadOpCodeException error
	if (lowOpParm != GT && lowOpParm != GTE) {
        scanExecuting = false;
		throw BadOpcodesException();
	}
    else if (highOpParm != LT && highOpParm != LTE) {
        scanExecuting = false;
        throw BadOpcodesException();
    }

	// check value search range is valid, ie low value < high value
    if (lowValParm > highValParm) {
        scanExecuting = false;
        throw BadScanrangeException();
    }

    // If another scan is already executing, that needs to be ended here
    if (scanExecuting) {
        endScan();
    }
    

    scanExecuting = true;
    lowOp = lowOpParm;
    highOp = highOpParm;

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
