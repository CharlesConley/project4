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
    scanExecuting = true;

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
        
        metaInfo = (IndexMetaInfo*)metaHeaderPage;
        
        rootPageNum = rPageNum;
        
        metaInfo->rootPageNo = rPageNum;
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->attrType = attrType;
        strcpy((char*)&(metaInfo->relationName), (const char*)&relationName);
        std::cout << metaInfo->relationName << " " << relationName.c_str() << std::endl;
        
        ///Insert the entries related to the relation into the index file.
        FileScan fScan = FileScan(relationName, bufMgr);
        LeafNodeInt *rootNode;
        rootNode = (LeafNodeInt*)rootPage;
        ///Initialize rootNode as nonLeafNodeInt, set all pages to 0
        
        
        metaInfo->isRootALeaf = true;
        isRootALeaf = true;
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
        
        ///read all records through filescan->scanNext, prepare entry, call insertEntry
        RecordId tmpRec;
        std::string tmpStr;
        int* key;
        
        try{
            int tracker = 0;
            while(1){
                std::cout << tracker << std::endl;
                tracker++;
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
        isRootALeaf = metaInfo->isRootALeaf;
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

///newNodeInfo will contain the pageId and key, level will indicate whether or not the new root node is just above the leafs
void BTreeIndex::rootSplit(PageKeyPair<int> newNodeInfo, int level)
{
 
    PageId newPageNum;
    Page * tmpPage;
    bufMgr->allocPage(file, newPageNum, tmpPage);
    
    NonLeafNodeInt* newRootNode = (NonLeafNodeInt*)tmpPage;
    newRootNode->level = level;
    
    ///set NonLeafNode info for RootNode
    newRootNode->keyArray[0] = newNodeInfo.key;
    ///left of key is the root, right is the newNode
    newRootNode->pageNoArray[0] = rootPageNum;
    newRootNode->pageNoArray[1] = newNodeInfo.pageNo;
    
    ///update rootPageNum
    rootPageNum = newNodeInfo.pageNo;
    
    ///read and update MetaPage
    Page* tmpMetaPage;
    bufMgr->readPage(file, 1, tmpMetaPage);
    IndexMetaInfo* metaInfo = (IndexMetaInfo*)tmpMetaPage;
    
    metaInfo->rootPageNo = rootPageNum;
    metaInfo->isRootALeaf = false;
    bufMgr->unPinPage(file, newPageNum, true);
    
}
    
void BTreeIndex::nonLeafSplit(NonLeafNodeInt* nonLeafNode, PageKeyPair<int>& newNonLeafPage, PageKeyPair<int> pageEntry)
{
    ///create a new nonLeafNode, reassign half the values to the new node, pass the middle key/pageNo combo back up
    PageId newPageNum;
    Page * tmpPage;
    bufMgr->allocPage(file, newPageNum, tmpPage);
    
    NonLeafNodeInt* newNonLeafNode = (NonLeafNodeInt*)tmpPage;
    int i;
    for(i = 0; i < nodeOccupancy/2; i++){
        newNonLeafNode->keyArray[i] = nonLeafNode->keyArray[nodeOccupancy/2 + i];
        nonLeafNode->keyArray[nodeOccupancy/2 + i] = 0;
        newNonLeafNode->pageNoArray[i] = nonLeafNode->pageNoArray[nodeOccupancy/2 + i];
        nonLeafNode->pageNoArray[nodeOccupancy/2 + i] = 0;
    }
    ///since pageNoArray is one large, manually change the last values
    newNonLeafNode->pageNoArray[nodeOccupancy/2] = nonLeafNode->pageNoArray[nodeOccupancy];
    nonLeafNode->pageNoArray[nodeOccupancy] = 0;
    
    if(nonLeafNode->keyArray[nodeOccupancy/2 - 1] > pageEntry.key){
        nonLeafInsert(nonLeafNode, pageEntry);
    }else nonLeafInsert(newNonLeafNode, pageEntry);
    
    newNonLeafPage.set(newPageNum, newNonLeafNode->keyArray[0]);
    bufMgr->unPinPage(file, newPageNum, true);
    
}
    
void BTreeIndex::leafSplit(LeafNodeInt* leafNode, PageKeyPair<int>& newLeafPage, RIDKeyPair<int> dataEntry)
{
    ///create a new nonLeafNode, reassign half the values to the new node, pass the middle key/pageNo combo back up
    PageId newPageNum;
    Page * tmpPage;
    bufMgr->allocPage(file, newPageNum, tmpPage);
    
    LeafNodeInt* newLeafNode = (LeafNodeInt*)tmpPage;
    int i;
    for(i = 0; i < leafOccupancy/2; i++){
        newLeafNode->keyArray[i] = leafNode->keyArray[leafOccupancy/2 + i];
        leafNode->keyArray[leafOccupancy/2 + i] = 0;
        newLeafNode->ridArray[i] = leafNode->ridArray[leafOccupancy/2 + i];
        leafNode->ridArray[leafOccupancy/2 + i].page_number = 0;
    }

    newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
    leafNode->rightSibPageNo = newPageNum;
    
    if(leafNode->keyArray[nodeOccupancy/2 - 1] > dataEntry.key){
        leafInsert(leafNode, dataEntry);
    }else leafInsert(newLeafNode, dataEntry);
    
    newLeafPage.set(newPageNum, newLeafNode->keyArray[0]);
    bufMgr->unPinPage(file, newPageNum, true);
    
}
    
    
// -----------------------------------------------------------------------------
// leafInsert
// -----------------------------------------------------------------------------
    

    
void BTreeIndex::leafInsert(LeafNodeInt * leafNode, RIDKeyPair<int> dataEntry)
{
    
    ///traverse leafNode for insert index, stop at first key that is greater than insert key
    int i;
    for(int i = 0; i < leafOccupancy && leafNode->ridArray[i].page_number != 0; i++){
        
        if(dataEntry.key < leafNode->keyArray[i])break; ///found insert location
        
    }
    
    ///shift all entries one to the right. Work from right to left to avoid overwriting entries
    int n;
    for(n = leafOccupancy - 1; n > i; n--){
        ///set [n-1] to [n]
        
        leafNode->keyArray[n] = leafNode->keyArray[n-1];
        leafNode->ridArray[n] = leafNode->ridArray[n-1];
    }
    
    leafNode->keyArray[i] = dataEntry.key;
    leafNode->ridArray[i] = dataEntry.rid;
    
}
    
void BTreeIndex::nonLeafInsert(NonLeafNodeInt * nonLeafNode, PageKeyPair<int> pageEntry)
{
    ///node has been split and we need to now insert the pageEntry
    
    int i;
    for(int i = 0; i < nodeOccupancy && nonLeafNode->pageNoArray[i] != 0; i++){
        
        if(pageEntry.key < nonLeafNode->keyArray[i] )break; ///found insert location
    }
    
    ///shift all entries one to the right
    int n;
    for(n = nodeOccupancy -1; n > i; n--){
        nonLeafNode->keyArray[n] = nonLeafNode->keyArray[n-1];
        nonLeafNode->pageNoArray[n+1] = nonLeafNode->pageNoArray[n];
    }

    nonLeafNode->keyArray[i] = pageEntry.key;
    nonLeafNode->pageNoArray[i+1] = pageEntry.pageNo;


}
        
    
void BTreeIndex::rootLeafInsert(LeafNodeInt * rootNode, RIDKeyPair<int> dataEntry, bool split)
{
    
    if(!split){ ///don't need to split, treat like any other leaf
        leafInsert(rootNode, dataEntry);
    }else{
        ///split the node and make a new root node to take its place
        
        PageKeyPair<int> newRootPage;
        
        leafSplit(rootNode, newRootPage, dataEntry);
        PageId curRootPage = rootPageNum;
        
        rootSplit(newRootPage, 1);
        bufMgr->unPinPage(file, curRootPage, true);
    }
        
}
    
// -----------------------------------------------------------------------------
// FindLeaf
// Arguments:  RIDKeypair to be inserted, current page number, and Placeholder PageKeyPair entry as overflow tracker
// -----------------------------------------------------------------------------
///Recursively visit each node on the way down to the leaf node that contains the correct location for the key. Start at root
///Base Case, the next node is a leaf node and the insertion can be attempted.
void BTreeIndex::findandInsert(RIDKeyPair<int> dataEntry, PageId curPageNum, PageKeyPair<int>& splitEntry)
{
    ///read current page from bufferManager
    Page* tmpPage;
    bufMgr->readPage(file, curPageNum, tmpPage);
    
    ///this will always be a nonLeaf page
    NonLeafNodeInt* curPage = (NonLeafNodeInt*)tmpPage;
    
    ///find dataEntry's key position relative to curPage keyArray
    int i;
    PageId nextPageNum;
    for(i = 0; i < nodeOccupancy && curPage->pageNoArray[i] != 0; i++){
        
        if(dataEntry.key < curPage->keyArray[i]){
            break;
        }
        
    }
    
    nextPageNum = curPage->pageNoArray[i];
    
    if(curPage->level == 1){ ///directly above leaf. Next page is leafNode
        Page* nextPage;
        
        bufMgr->readPage(file, nextPageNum, nextPage);
        
        LeafNodeInt * leafNode = (LeafNodeInt*)nextPage;
        
        if(leafNode->ridArray[leafOccupancy-1].page_number != 0){///will need to split
            
            PageKeyPair<int> newLeafPage;
            leafSplit(leafNode, newLeafPage, dataEntry);
            
            ///can the new key fit on currpage, if not split again.
            if(curPage->pageNoArray[nodeOccupancy] != 0){
                PageKeyPair<int> newNonLeafPage;
                nonLeafSplit(curPage, newNonLeafPage, newLeafPage);
                splitEntry = newNonLeafPage;
            }else nonLeafInsert(curPage, newLeafPage);
            
        }else{///can be inserted no problem.
            leafInsert(leafNode, dataEntry);
        }
        ///clear bufMgr of pinned files
        bufMgr->unPinPage(file, curPageNum, true);
        bufMgr->unPinPage(file, nextPageNum, true);
        return;
    }
    
    ///not low enough yer, traverse to next node
    PageKeyPair<int> newSplitPage;
    newSplitPage.set(0,0);
    bufMgr->unPinPage(file, curPageNum, false);
    
    ///call the next level of search
    findandInsert(dataEntry, nextPageNum, newSplitPage);
    
    ///on the way back up, check to see if newSplitPage has been modified, if so insert
    if(newSplitPage.pageNo != 0) {
        PageKeyPair<int> nonLeafPage;
        nonLeafPage.set(newSplitPage.pageNo, newSplitPage.key);
       if(curPage->pageNoArray[nodeOccupancy] != 0){
           PageKeyPair<int> newNonLeafPage;
           nonLeafSplit(curPage, newNonLeafPage, nonLeafPage);
           splitEntry = newNonLeafPage;
       }else nonLeafInsert(curPage, nonLeafPage);
        
    }
}
    
    
    
// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    Page* tmpPage;
    bufMgr->readPage(file, rootPageNum, tmpPage);
    
    RIDKeyPair<int> dataEntry;
    dataEntry.set(rid, *(int*)key);
    
    ///if root is a leaf, then manually insert until it needs to split
    if(isRootALeaf){
        
        LeafNodeInt * rootLeaf = (LeafNodeInt*)tmpPage;
        ///check to see if last key/page pair is taken
        if(rootLeaf->ridArray[leafOccupancy - 1].page_number != 0){
            ///full and needs to split
            isRootALeaf = false;
            rootLeafInsert(rootLeaf, dataEntry, true);
            
        }else{
            rootLeafInsert(rootLeaf, dataEntry, false);
        }
        
    }else{
        ///set up call to FindLeaf, let it handle the insert.
        ///pass in root page number, dataEntry, and PageKeyPair entry
        
        PageKeyPair<int> splitEntry;
        ///set page number to zero as this will mark whether a page is allocated
        splitEntry.set(0, *(int*)key);
        
        findandInsert(dataEntry, rootPageNum, splitEntry);
        
        ///if splitEntry has a valid page, that means root needs to split
        
    }
    
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
	//try the next one
	nextEntry++;
	k=cnode->keyArray[nextEntry];
	outRid = cnode->ridArray[nextEntry];
    }

}

bool BTreeIndex::compK(int lowValInt,const Operator lowOp,int highValInt,const Operator highOp, int key){
    int lVal = (lowValInt);
    int hVal = (highValInt);
    bool retVal = false;
    
    if(lowOp == GTE){
        if(highOp == LTE) retVal = (key >= lVal && key <= hVal);
        else if(highOp == LT) retVal = (key < hVal && key >= lVal);
    }if(lowOp == GT){
        if(highOp == LTE) retVal = (key <= hVal && key > lVal);
        else if(highOp == LT) retVal = (key < hVal && key > lVal);
    }
    return retVal;
    
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
