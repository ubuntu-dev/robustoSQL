
#include "ix.h"
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>
#include <cstdio>
#include <algorithm>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	pfm=PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	RC retval=pfm->createFile(fileName);
		if(retval==0){
			return 0;
		}
	return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC retval=pfm->destroyFile(fileName);
	if(retval==0){
			return 0;
	}
	return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{

	RC retVal = pfm->openFile(fileName, ixfileHandle.fileHandle);
	if(retVal == 0){
		return 0;
	}
	return -1;

}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	RC retVal = pfm->closeFile(ixfileHandle.fileHandle);
	if(retVal == 0){
			return 0;
		}
	return -1;
}

RC IndexManager::populateFirstRootData(void* rootData, int leftLeaf, int rightLeaf, const Attribute &attribute, const void* key){
	short int freeSpacePointer = 0;
	if(attribute.type == TypeInt || attribute.type == TypeReal){
		memcpy((char*) rootData, &leftLeaf, sizeof(int));
		memcpy((char*) rootData + sizeof(int), (char*) key, sizeof(int));
		memcpy((char*) rootData + 2*sizeof(int), &rightLeaf, sizeof(int));
		freeSpacePointer = 3*sizeof(int);
	}
	else{//varchar
		int offset = 0;
		memcpy((char*) rootData + offset, &leftLeaf, sizeof(int));
		offset += sizeof(int);
		int len;
		memcpy(&len, (char*) key, sizeof(int));
		memcpy((char*) rootData + offset, (char*) key, sizeof(int) + len);
		offset += sizeof(int) + len;
		memcpy((char*) rootData + offset, &rightLeaf, sizeof(int));
		offset += sizeof(int);
		freeSpacePointer = offset;
	}
	bool isLeaf = false;
	memcpy((char*) rootData + PAGE_SIZE - sizeof(bool), &isLeaf, sizeof(bool));
	memcpy((char*) rootData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
	short int numOfEntries = 1;
	memcpy((char*) rootData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));
}

bool IndexManager::keyRIDPairPresent(void* nodeData, const Attribute &attribute, const void* key, const RID &rid){
	//checks if a key rid pair is already present in a leaf
	short int numOfEntries;
	memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	if(attribute.type == TypeInt){
		int keySearch;
		memcpy(&keySearch, (char*) key, sizeof(int));
		for(int i=0;i<numOfEntries;i++){
			int offset = i*(3*sizeof(int));
			int keyInLeaf,pg,sl;
			memcpy(&keyInLeaf, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&pg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&sl, (char*) nodeData + offset, sizeof(int));
			if(keySearch == keyInLeaf && rid.pageNum == pg && rid.slotNum == sl){
				return true;
			}
		}
	}
	else if(attribute.type == TypeReal){
		float keySearch;
		memcpy(&keySearch, (char*) key, sizeof(float));
		for(int i=0;i<numOfEntries;i++){
			int offset = i*(sizeof(float) + 2*sizeof(int));
			int pg,sl;
			float keyInLeaf;
			memcpy(&keyInLeaf, (char*) nodeData + offset, sizeof(float));
			offset += sizeof(float);
			memcpy(&pg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&sl, (char*) nodeData + offset, sizeof(int));
			if(keySearch == keyInLeaf && rid.pageNum == pg && rid.slotNum == sl){
				return true;
			}
		}
	}
	else{
		char* str = (char*) malloc(PAGE_SIZE);
		int len;
		memcpy(&len, (char*) key, sizeof(int));
		memcpy(str, (char*) key + sizeof(int), len);
		str[len]='\0';
		string keySearch(str);
		free(str);
		int offset = 0;
		for(int i=0;i<numOfEntries;i++){
			char* strInLeaf = (char*) malloc(PAGE_SIZE);
			int lenInLeaf, pg, sl;
			memcpy(&lenInLeaf, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(strInLeaf, (char*) nodeData + offset, lenInLeaf);
			offset += lenInLeaf;
			strInLeaf[lenInLeaf]='\0';
			memcpy(&pg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&sl, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			string keyInLeaf(strInLeaf);
			free(strInLeaf);
			if(keySearch == keyInLeaf && rid.pageNum == pg && rid.slotNum == sl){
				return true;
			}
		}
	}
	return false;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int numPages = ixfileHandle.getNumberOfPages();

	int keyLength = 0;
	if(attribute.type == TypeReal || attribute.type == TypeInt){
		keyLength = sizeof(int);
	}else{
		int stringLength = 0;
		memcpy(&stringLength, key, sizeof(int));
		keyLength = stringLength + sizeof(int);
	}
	if(numPages == 0){
		void* nodeData = malloc(PAGE_SIZE);
		createBlankLeafPage(nodeData, -1, -1);
		insertEntryIntoLeaf(ixfileHandle, nodeData, 0, attribute, key, rid, keyLength);
		free(nodeData);
		return 0;
//		createRoot(ixfileHandle, attribute, key, keyLength);
	}
	if(numPages == 1){//only one leaf. no root yet
		void* nodeData = malloc(PAGE_SIZE);
		ixfileHandle.readPage(INIT_LEAF_PAGE, nodeData);
		bool keyRidPresent = keyRIDPairPresent(nodeData, attribute, key, rid);
		if(keyRidPresent){
			free(nodeData);
			return -1;
		}
		bool isLeaf;
		memcpy(&isLeaf, (char*) nodeData + PAGE_SIZE - sizeof(bool), sizeof(bool));
		if(!isLeaf){
			free(nodeData);
			return -1;
		}
		bool hasSpace;
		checkIfNodeHasSpace(hasSpace, nodeData, keyLength, isLeaf);
		if(hasSpace){
			insertEntryIntoLeaf(ixfileHandle, nodeData, INIT_LEAF_PAGE, attribute, key, rid, keyLength);
		}
		else{ // now have to create root
			void* newChildKey = malloc(PAGE_SIZE);
			int newChildPageId = -1;
			splitLeafNode(ixfileHandle, nodeData, INIT_LEAF_PAGE, attribute, key, rid, keyLength, newChildKey, newChildPageId);
			void* rootData = malloc(PAGE_SIZE);
			int leftLeaf = ixfileHandle.getNumberOfPages(), rightLeaf = newChildPageId;
			populateFirstRootData(rootData, leftLeaf, rightLeaf, attribute, newChildKey);
			void *backupOfInitialLeaf = malloc(PAGE_SIZE);
			ixfileHandle.readPage(INIT_LEAF_PAGE, backupOfInitialLeaf);
			ixfileHandle.writePage(INIT_LEAF_PAGE, rootData);
			ixfileHandle.appendPage(backupOfInitialLeaf);
			free(rootData);
			free(backupOfInitialLeaf);
			free(newChildKey);
		}
		free(nodeData);
		return 0;
	}
	void* newChildKey = malloc(PAGE_SIZE);
	int newChildPageId = -1;
	RC retval = insert(ixfileHandle, ROOT_PAGE_NUM, attribute, key, rid, keyLength, newChildKey, newChildPageId);
	free(newChildKey);
    return retval;
}

RC IndexManager::insert(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute,
		const void *key, const RID &rid, int keyLength, void *newChildKey, int &newChildPageId){
	void *nodeData = malloc(PAGE_SIZE);
	ixfileHandle.readPage(nodePageNum, nodeData);
	bool isLeaf;
	memcpy(&isLeaf,(char*) nodeData + PAGE_SIZE - sizeof(bool), sizeof(bool));

	if(!isLeaf){ // not a leaf
		int keyPointer;
		searchKeyInNonLeaf(attribute, nodeData, key, keyPointer);
		insert(ixfileHandle, keyPointer, attribute, key, rid, keyLength, newChildKey, newChildPageId);
		if(newChildPageId == -1){ //usual case, didn't split the child
			free(nodeData);
			return 0;
		}else{ //we split the child, must insert <childKey, childPageId> in this node
			bool hasSpace;
			checkIfNodeHasSpace(hasSpace, nodeData, keyLength, isLeaf);
			if(hasSpace){
				insertEntryIntoNonLeaf(ixfileHandle, nodeData, nodePageNum, attribute, newChildKey, newChildPageId);
				newChildPageId=-1;
			}else{
				splitNonLeafNode(ixfileHandle, nodeData, nodePageNum, attribute, newChildKey, newChildPageId);
				if(nodePageNum == ROOT_PAGE_NUM){
					//split already done. create a new root node and swap that contents with page 0

					createNewRootAndSwap(ixfileHandle, nodeData, nodePageNum, attribute, newChildKey, newChildPageId);
				}
			}
		}
	}
	else{//node is a leaf
		bool hasSpace = false;
		checkIfNodeHasSpace(hasSpace, nodeData, keyLength, isLeaf);
		bool keyRidPresent = keyRIDPairPresent(nodeData, attribute, key, rid);
		if(keyRidPresent){
			free(nodeData);
			return -1;
		}
		if(hasSpace){
			insertEntryIntoLeaf(ixfileHandle, nodeData, nodePageNum, attribute, key, rid, keyLength);
		}else{
			splitLeafNode(ixfileHandle, nodeData, nodePageNum, attribute, key, rid, keyLength, newChildKey, newChildPageId);
		}
	}
	free(nodeData);
	return 0;
}

RC IndexManager::createNewRootAndSwap(IXFileHandle &ixfileHandle, void* oldRootData, int oldRootPageNum,
		const Attribute &attribute, void *newChildKey, int &newChildPageId){
	int numPages = ixfileHandle.getNumberOfPages();
	int newNodePage = numPages;

	void * newRootData = malloc(PAGE_SIZE);
	int leftPointer = newNodePage;
	int rightPointer = newChildPageId;
	short offset = 0;
	//write leftpointer
	memcpy(newRootData + offset, &leftPointer, PAGE_NUM_SIZE);
	offset += PAGE_NUM_SIZE;
	//write the key
	if(attribute.type == TypeReal || attribute.type == TypeInt){
		memcpy(newRootData + offset, newChildKey, sizeof(int));
		offset += sizeof(int);
	}else{
		int keyLength;
		memcpy(&keyLength, newChildKey, sizeof(int));
		memcpy(newRootData + PAGE_NUM_SIZE, newChildKey, sizeof(int) + keyLength);
		offset += sizeof(int) + keyLength;
	}
	//write the right pointer
	memcpy(newRootData + offset, &rightPointer, PAGE_NUM_SIZE);
	offset += PAGE_NUM_SIZE; //added
	//write is Leaf
	bool isLeaf = false;
	memcpy(newRootData + PAGE_SIZE - sizeof(bool), &isLeaf, sizeof(bool));
	//write freespacePointer
	memcpy(newRootData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &offset, sizeof(short int));
	//write num entries
	short numEntries = 1;
	memcpy(newRootData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), &numEntries, sizeof(short int));

	ixfileHandle.writePage(oldRootPageNum, newRootData);
	ixfileHandle.appendPage(oldRootData);

	free(newRootData);


}

RC IndexManager::insertEntryIntoNonLeaf(IXFileHandle &ixfileHandle, void * nodeData, int nodePageNum, const Attribute &attribute, void* newChildKey, int &newChildPageId){
	// insert newChildKey, newChildPageId into the nodePageNum page whose data is present in nodeData
	short int freeSpacePointer;
	memcpy(&freeSpacePointer, (char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));
	short int numOfEntries;
	int newPage = newChildPageId;
	memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	if(attribute.type == TypeInt){
		int searchKey;
		int insertAtIndex = numOfEntries;
		memcpy(&searchKey, (char*)newChildKey, sizeof(int));
		for(int i=0;i<numOfEntries;i++){
			int offset = sizeof(int) + i*2*sizeof(int);
			int keyVal;
			memcpy(&keyVal, (char*) nodeData + offset, sizeof(int));
			if(searchKey > keyVal){
				continue;
			}
			else{
				insertAtIndex = i;
				break;
			}
		}
		if(insertAtIndex >= numOfEntries){
			memcpy((char*) nodeData + freeSpacePointer, &searchKey, sizeof(int));
			memcpy((char*) nodeData + freeSpacePointer + sizeof(int), &newPage, sizeof(int));
		}
		else{
			short int offsetToStore = sizeof(int) + insertAtIndex * 2 * sizeof(int);
			short int shiftBegin = offsetToStore + 2*sizeof(int);
			short int lengthToShift = freeSpacePointer - offsetToStore;
			memmove((char*) nodeData + shiftBegin, (char*) nodeData + offsetToStore, lengthToShift);
			// now write the entry
			memcpy((char*) nodeData + offsetToStore, &searchKey, sizeof(int));
			offsetToStore += sizeof(int);
			memcpy((char*) nodeData + offsetToStore, &newPage, sizeof(int));
		}
		//update freespace pointer and num of entries
		freeSpacePointer += 2*sizeof(int);
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
		numOfEntries += 1;
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));
	}
	else if(attribute.type == TypeReal){
		float floatSearchKey;
		int insertAtIndex = numOfEntries;
		memcpy(&floatSearchKey, (char*) newChildKey, sizeof(float));
		for(int i=0;i<numOfEntries;i++){
			int offset = sizeof(int) + i*(sizeof(float) + sizeof(int));
			float floatKeyVal;
			memcpy(&floatKeyVal, (char*) nodeData + offset, sizeof(float));
			if(floatSearchKey > floatKeyVal){
				continue;
			}
			else{
				insertAtIndex = i;
				break;
			}
		}
		if(insertAtIndex >= numOfEntries){
			memcpy((char*) nodeData + freeSpacePointer, &floatSearchKey, sizeof(float));
			memcpy((char*) nodeData + freeSpacePointer + sizeof(float), &newPage, sizeof(int));
		}
		else{
			short int offsetToStore = sizeof(int) + insertAtIndex*(sizeof(float)+sizeof(int));
			short int shiftBegin = offsetToStore + sizeof(float)+sizeof(int);
			short int lengthToShift = freeSpacePointer - offsetToStore;
			memmove((char*) nodeData + shiftBegin, (char*) nodeData + offsetToStore, lengthToShift);
			memcpy((char*) nodeData + offsetToStore, &floatSearchKey, sizeof(float));
			offsetToStore += sizeof(float);
			memcpy((char*) nodeData + offsetToStore, &newPage, sizeof(int));
		}
		//update freespacepointer and num of entries
		freeSpacePointer += sizeof(float) + sizeof(int);
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
		numOfEntries += 1;
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));
	}
	else{ //varchar
		int lenOfStr;
		memcpy(&lenOfStr, (char*) newChildKey, sizeof(int));
		char* searchKey = (char*) malloc(PAGE_SIZE);
		memcpy(searchKey, (char*) newChildKey + sizeof(int), lenOfStr);
		searchKey[lenOfStr]='\0';
		string searchKeyString(searchKey);
		int offset=sizeof(int);
		int offsetToInsertAt = freeSpacePointer;
		short int shiftBegin,lengthToShift;
		for(int i=0;i<numOfEntries;i++){
			int length;
			memcpy(&length, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			char* keyVal = (char*) malloc(PAGE_SIZE);
			memcpy(keyVal, (char*) nodeData + offset, length);
			offset += length;
			offset += sizeof(int);
			keyVal[length]='\0';
			string keyValString(keyVal);
			free(keyVal);
			if(searchKeyString > keyValString){
				continue;
			}
			else{
				offsetToInsertAt = offset - sizeof(int) - length - sizeof(int);
				shiftBegin = offsetToInsertAt + sizeof(int) + lenOfStr + sizeof(int);
				lengthToShift = freeSpacePointer - offsetToInsertAt;
				break;
			}
		}
		if(offsetToInsertAt == freeSpacePointer){
			memcpy((char*) nodeData + freeSpacePointer, &lenOfStr, sizeof(int));
			memcpy((char*) nodeData + freeSpacePointer + sizeof(int), searchKey, lenOfStr);
			memcpy((char*) nodeData + freeSpacePointer + sizeof(int) + lenOfStr, &newPage, sizeof(int));
		}
		else{
			memmove((char*) nodeData + shiftBegin, (char*) nodeData + offsetToInsertAt, lengthToShift);
			memcpy((char*) nodeData + offsetToInsertAt, &lenOfStr, sizeof(int));
			offsetToInsertAt += sizeof(int);
			memcpy((char*) nodeData + offsetToInsertAt, searchKey, lenOfStr);
			offsetToInsertAt += lenOfStr;
			memcpy((char*) nodeData + offsetToInsertAt, &newPage, sizeof(int));
		}
		//now update the freespace pointers
		freeSpacePointer += sizeof(int) + lenOfStr + sizeof(int);
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
		numOfEntries += 1;
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));
		free(searchKey);
	}
	ixfileHandle.writePage(nodePageNum, nodeData);
	return 0;
}

RC IndexManager::splitNonLeafNode(IXFileHandle &ixfileHandle, void * nodeData, int nodePageNum, const Attribute &attribute,
		void* newChildKey, int &newChildPageId){
	unsigned numPages = ixfileHandle.getNumberOfPages();
	int newPageDataPageNum = numPages;

	//read freeSpacePointer
	short freeSpacePointer;
	memcpy(&freeSpacePointer, nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));
	//read numEntries
	short numEntries;
	memcpy(&numEntries, nodeData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), sizeof(short int));
	short numEntriesToKeep = numEntries / 2;

	short newFreeSpacePointer = getOffsetEndForMiddleEntry(numEntriesToKeep, attribute, nodeData, false);
	//update new free space pointer
	memcpy(nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &newFreeSpacePointer, sizeof(short int));
	//update new numEntries
	memcpy(nodeData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), &numEntriesToKeep, sizeof(short int));
	ixfileHandle.writePage(nodePageNum, nodeData);

	void *newPageData = malloc(PAGE_SIZE);
	//write the 2nd half entries from the original page
	memcpy(newPageData + PAGE_NUM_SIZE, nodeData + newFreeSpacePointer, (freeSpacePointer - newFreeSpacePointer));
	//write the left most pointer which is not pointing anywhere now
	int voidPage = -1;
	memcpy(newPageData, &voidPage, PAGE_NUM_SIZE);
	//write isLeaf
	bool isLeaf = false;
	memcpy(newPageData + PAGE_SIZE - sizeof(bool), &isLeaf, sizeof(bool));
	//write freeSpacePointer;
	short newPageFreeSpacePointer = freeSpacePointer + PAGE_NUM_SIZE- newFreeSpacePointer;
	memcpy(newPageData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &newPageFreeSpacePointer, sizeof(short int));
	//write numEntries
	short newPageNumEntries = numEntries - numEntriesToKeep;
	memcpy(newPageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), &newPageNumEntries, sizeof(short int));
	ixfileHandle.appendPage(newPageData);

	//insert the entry into the oldpage or newpage
	bool insertInNewPage;
	findPageToInsert(insertInNewPage, newPageData, attribute, newChildKey, false);
	if(insertInNewPage){
		insertEntryIntoNonLeaf(ixfileHandle, newPageData, newPageDataPageNum, attribute, newChildKey, newChildPageId);
	}else{
		insertEntryIntoNonLeaf(ixfileHandle, nodeData, nodePageNum, attribute, newChildKey,newChildPageId);
	}

	populateNewChildKey(newChildKey, newPageData, attribute, false);
	newChildPageId = newPageDataPageNum;
	deleteFirstEntryFromNonLeafNode(ixfileHandle, newPageData, newPageDataPageNum, attribute);
	free(newPageData);

}

RC IndexManager::deleteFirstEntryFromNonLeafNode(IXFileHandle &ixfileHandle, void *pageData, int pageNum, const Attribute &attribute){
	short freeSpacePointer;
	memcpy(&freeSpacePointer, pageData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));

	if(attribute.type == TypeInt || attribute.type == TypeReal){
		int moveFrom = PAGE_NUM_SIZE + sizeof(int);
		memmove(pageData, pageData + moveFrom, freeSpacePointer - moveFrom);
		freeSpacePointer -= moveFrom;
	}else{
		int keyLength;
		memcpy(&keyLength, pageData + PAGE_NUM_SIZE, sizeof(int));
		int moveFrom = PAGE_NUM_SIZE + sizeof(int) + keyLength;
		memmove(pageData, pageData + moveFrom, freeSpacePointer - moveFrom);
		freeSpacePointer -= moveFrom;
	}
	//Update free space Pointer
	memcpy(pageData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
	//Update num entries
	short numEntries;
	memcpy(&numEntries, pageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), sizeof(short int));
	numEntries -= 1;
	memcpy(pageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), &numEntries, sizeof(short int));
	ixfileHandle.writePage(pageNum, pageData);
}

RC IndexManager::splitLeafNode(IXFileHandle &ixfileHandle, void *nodeData, int nodePageNum, const Attribute &attribute,
		const void *key,const RID &rid, int keyLength, void* newChildKey, int &newChildPageId){
	unsigned numPages = ixfileHandle.getNumberOfPages();
	int newPageDataPageNum = numPages;

	//read freeSpacePointer
	short freeSpacePointer;
	memcpy(&freeSpacePointer, nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));
	//read numEntries
	short numEntries;
	memcpy(&numEntries, nodeData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), sizeof(short int));
	short numEntriesToKeep = numEntries / 2;
	//read nextPage
	int oldNextPage;
	memcpy(&oldNextPage, nodeData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int) - sizeof(int), sizeof(int));


	short newfreeSpacePointer = getOffsetEndForMiddleEntry(numEntriesToKeep, attribute, nodeData, true);
	//check for the same keys and updte the new free space pointer and the numEntries to keep accordingly
	checkForDuplicateKeys(numEntries, attribute, nodeData, newfreeSpacePointer, numEntriesToKeep);
	//update new free space pointer
	memcpy(nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &newfreeSpacePointer, sizeof(short int));
	//update new numEntries
	memcpy(nodeData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), &numEntriesToKeep, sizeof(short int));
	//update next Page Pointer
	memcpy(nodeData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int) - sizeof(int), &newPageDataPageNum, sizeof(int));
	ixfileHandle.writePage(nodePageNum, nodeData);


	void *newPageData = malloc(PAGE_SIZE);
	//write the 2nd half entries from the original page
	memcpy(newPageData, nodeData + newfreeSpacePointer, (freeSpacePointer - newfreeSpacePointer));
	//write isLeaf
	bool isLeaf = true;
	memcpy(newPageData + PAGE_SIZE - sizeof(bool), &isLeaf, sizeof(bool));
	//write freeSpacePointer;
	short newPageFreeSpacePointer = freeSpacePointer - newfreeSpacePointer;
	memcpy(newPageData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &newPageFreeSpacePointer, sizeof(short int));
	//write numEntries
	short newPageNumEntries = numEntries - numEntriesToKeep;
	memcpy(newPageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), &newPageNumEntries, sizeof(short int));
	//write nextPagePointer
	memcpy(newPageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int) - sizeof(int), &oldNextPage, sizeof(int));
	//write previousPagePointer
	memcpy(newPageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int) - 2 * sizeof(int), &nodePageNum, sizeof(int));
	ixfileHandle.appendPage(newPageData);

	//Update previous PageNumber of the initial next page
	void * nextPageData = malloc(PAGE_SIZE); // write old next iff oldNextpage > 0
	if(oldNextPage >= 0){
		ixfileHandle.readPage(oldNextPage, nextPageData);
		memcpy(nextPageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int) - 2 * sizeof(int), &newPageDataPageNum, sizeof(int));
		ixfileHandle.writePage(oldNextPage, nextPageData);
	}

	//insert the entry into the oldpage or newpage
	bool insertInNewPage;
	if(newPageNumEntries == 0){
		insertInNewPage = true;
	}else{
		findPageToInsert(insertInNewPage, newPageData, attribute, key, true);
	}
	if(insertInNewPage){
		insertEntryIntoLeaf(ixfileHandle, newPageData, newPageDataPageNum, attribute, key, rid, keyLength);
	}else{
		insertEntryIntoLeaf(ixfileHandle, nodeData, nodePageNum, attribute, key, rid, keyLength);
	}
	populateNewChildKey(newChildKey, newPageData, attribute, true);
	newChildPageId = newPageDataPageNum;
//	ixfileHandle.appendPage();
	free(newPageData);
	free(nextPageData);
	return 0;

}


RC IndexManager::populateNewChildKey(void* key, void*pageData, const Attribute &attribute, bool isLeaf){
	if(attribute.type == TypeInt || attribute.type == TypeReal){
		if(isLeaf){
			memcpy(key, pageData, sizeof(int));
		}else{
			memcpy(key, pageData + PAGE_NUM_SIZE, sizeof(int));
		}
	}else{
		int keyLength;
		if(isLeaf){
			memcpy(&keyLength, pageData, sizeof(int));
			memcpy(key, pageData, sizeof(int)+keyLength);
		}else{
			memcpy(&keyLength, pageData + PAGE_NUM_SIZE, sizeof(int));
			memcpy(key, pageData + PAGE_NUM_SIZE, sizeof(int)+keyLength);
		}

	}
}

RC IndexManager::findPageToInsert(bool &insertInNewPage, void* pageData, const Attribute &attribute, const void * key, bool isLeaf){
	if(attribute.type == TypeInt){
		int keyFromPage;
		if(isLeaf){
			memcpy(&keyFromPage, pageData, sizeof(int));
		}else{
			memcpy(&keyFromPage, pageData + PAGE_NUM_SIZE, sizeof(int));
		}
		int keyToInsert;
		memcpy(&keyToInsert, key, sizeof(int));

		if(keyToInsert >= keyFromPage){
			insertInNewPage = true;
		}else{
			insertInNewPage = false;
		}
	}else if(attribute.type == TypeReal){
		float keyFromPage;
		if(isLeaf){
			memcpy(&keyFromPage, pageData, sizeof(float));
		}else{
			memcpy(&keyFromPage, pageData + PAGE_NUM_SIZE, sizeof(float));
		}
		float keyToInsert;
		memcpy(&keyToInsert, key, sizeof(float));
		if(keyToInsert >= keyFromPage){
			insertInNewPage = true;
		}else{
			insertInNewPage = false;
		}
	}else{
		int keyLength;
		char* keyFrompage = (char*) malloc(PAGE_SIZE);
		if(isLeaf){
			memcpy(&keyLength, pageData, sizeof(int));
			memcpy(keyFrompage, pageData+sizeof(int), keyLength);
		}else{
			memcpy(&keyLength, pageData + PAGE_NUM_SIZE, sizeof(int));
			memcpy(keyFrompage, pageData + PAGE_NUM_SIZE + sizeof(int), keyLength);
		}
		keyFrompage[keyLength]='\0';

		int insertKeyLength;
		memcpy(&insertKeyLength, key, sizeof(int));
		char* keyToInsert = (char*) malloc(PAGE_SIZE);
		memcpy(keyToInsert, key+sizeof(int), insertKeyLength);
		keyToInsert[insertKeyLength]='\0';

		string page_string = string(keyFrompage);
		string insert_string = string(keyToInsert);
		if(insert_string >= page_string){
			insertInNewPage = true;
		}else{
			insertInNewPage = false;
		}
		free(keyFrompage);
		free(keyToInsert);
	}
}

short IndexManager::getOffsetEndForMiddleEntry(short numEntries, const Attribute &attribute, void *nodeData, bool isLeaf){
	if(attribute.type == TypeInt || attribute.type == TypeReal){
		if(isLeaf){
			return numEntries * LEAF_INT_ENTRY_SIZE;
		}else{
			return  PAGE_NUM_SIZE + ( numEntries * NON_LEAF_INT_ENTRY_SIZE );
		}

	}else{ //if it is varchar do linear search
		int offset = 0;
		if(!isLeaf){
			offset = PAGE_NUM_SIZE;
		}
		for (int var = 0; var < numEntries; ++var) {
			int stringLength;
			memcpy(&stringLength, nodeData + offset, sizeof(int));
			if(isLeaf){
				offset += sizeof(int) + stringLength + RECORD_ID_SIZE;
			}else{
				offset += sizeof(int) + stringLength + PAGE_NUM_SIZE;
			}
		}
		return offset;
	}
}

void IndexManager::checkForDuplicateKeys(short numEntries, const Attribute &attribute,
		void *nodeData, short &splitOffset, short &numEntriesToKeep){
	if(attribute.type == TypeInt){
		int currentKey;
		memcpy(&currentKey, nodeData + splitOffset , sizeof(int));
		int leftKey;
		memcpy(&leftKey, nodeData + splitOffset - LEAF_INT_ENTRY_SIZE, sizeof(int));
//		int rightKey;
//		memcpy(&leftKey, nodeData + splitOffset + LEAF_INT_ENTRY_SIZE, sizeof(int));

		if(currentKey == leftKey){
			int leftDuplicateKeys = 1;
			short leftOffset = splitOffset - LEAF_INT_ENTRY_SIZE;
			int leftNumEntries = numEntries / 2;
			for (int var = 1; var < leftNumEntries; ++var) {
				int key;
				memcpy(&key, nodeData + splitOffset - ( var + 1 ) * LEAF_INT_ENTRY_SIZE, sizeof(int));
				if(key == currentKey){
					leftDuplicateKeys ++;
					leftOffset -= LEAF_INT_ENTRY_SIZE;
				}else{
					break;
				}
			}
			int rightDuplicateKeys = 1;
			short rightOffset = splitOffset +  LEAF_INT_ENTRY_SIZE;
			int rightNumEntries = numEntries - leftNumEntries;
			for (int var = 1; var < rightNumEntries; ++var) {
				int key;
				memcpy(&key, nodeData + splitOffset + var * LEAF_INT_ENTRY_SIZE, sizeof(int));
				if(key == currentKey){
					rightDuplicateKeys ++;
					rightOffset += LEAF_INT_ENTRY_SIZE;
				}else{
					break;
				}
			}

			if(leftDuplicateKeys < rightDuplicateKeys){
				splitOffset = leftOffset;
				numEntriesToKeep = numEntriesToKeep - leftDuplicateKeys;
			}else{
				splitOffset = rightOffset;
				numEntriesToKeep = numEntriesToKeep + rightDuplicateKeys;
			}
		}

	}else if(attribute.type == TypeReal){
		float currentKey;
		memcpy(&currentKey, nodeData + splitOffset , sizeof(int));
		float leftKey;
		memcpy(&leftKey, nodeData + splitOffset - LEAF_INT_ENTRY_SIZE, sizeof(int));
		float rightKey;
		memcpy(&leftKey, nodeData + splitOffset + LEAF_INT_ENTRY_SIZE, sizeof(int));

		if(currentKey == leftKey){
			int leftDuplicateKeys = 1;
			short leftOffset = splitOffset - LEAF_INT_ENTRY_SIZE;
			int leftNumEntries = numEntries / 2;
			for (int var = 1; var < leftNumEntries; ++var) {
				float key;
				memcpy(&key, nodeData + splitOffset - ( var + 1 ) * LEAF_INT_ENTRY_SIZE, sizeof(int));
				if(key == currentKey){
					leftDuplicateKeys ++;
					leftOffset -= LEAF_INT_ENTRY_SIZE;
				}else{
					break;
				}
			}
			int rightDuplicateKeys = 1;
			short rightOffset = splitOffset + LEAF_INT_ENTRY_SIZE;
			int rightNumEntries = numEntries - leftNumEntries;
			for (int var = 1; var < rightNumEntries; ++var) {
				float key;
				memcpy(&key, nodeData + splitOffset + var * LEAF_INT_ENTRY_SIZE, sizeof(int));
				if(key == currentKey){
					rightDuplicateKeys ++;
					rightOffset += LEAF_INT_ENTRY_SIZE;
				}else{
					break;
				}
			}
			if(leftDuplicateKeys < rightDuplicateKeys){
				splitOffset = leftOffset;
				numEntriesToKeep = numEntriesToKeep - leftDuplicateKeys;
			}else{
				splitOffset = rightOffset;
				numEntriesToKeep = numEntriesToKeep + rightDuplicateKeys;
			}
		}
	}else{
		char* currentKey = (char*)malloc(PAGE_SIZE);
		int currentKeyLength;
		memcpy(&currentKeyLength, nodeData + splitOffset, sizeof(int));
		memcpy(currentKey, nodeData + splitOffset + sizeof(int) , currentKeyLength);
		currentKey[currentKeyLength] = '\0';

		int leftDuplicateKeys = 0;
		int offset = 0;
		bool leftDuplicateFound = false;
		int leftOffset;
		while(offset < splitOffset){
			char *leftKey = (char*)malloc(PAGE_SIZE);
			int leftKeyLength;
			memcpy(&leftKeyLength, nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(leftKey, nodeData + offset, leftKeyLength);
			offset += leftKeyLength;
			leftKey[leftKeyLength] = '\0';
			if(string(currentKey) == string(leftKey)){
				if(!leftDuplicateFound){
					//We need to save the start of the left duplicates
					leftDuplicateFound = true;
					leftOffset = offset - sizeof(int) - leftKeyLength;
				}
				leftDuplicateKeys ++;
			}
			offset += RECORD_ID_SIZE;
			free(leftKey);
		}

		if(leftDuplicateKeys > 0){

			int rightDuplicateKeys = 1;
			short rightOffset = splitOffset +  (sizeof(int) + currentKeyLength) + RECORD_ID_SIZE;
			int rightNumEntries = numEntries - numEntries / 2;
			for (int var = 1; var < rightNumEntries; ++var) {
				int offset = splitOffset + var * (sizeof(int) + currentKeyLength + RECORD_ID_SIZE) + sizeof(int);
				char* key = (char*)malloc(PAGE_SIZE);
				memcpy(key, nodeData + offset, currentKeyLength);
				key[currentKeyLength] = '\0';
				if(string(key) == string(currentKey)){
					rightDuplicateKeys ++;
					rightOffset += (sizeof(int) + currentKeyLength) + RECORD_ID_SIZE;
					free(key);
				}else{
					free(key);
					break;
				}
			}
			if(leftDuplicateKeys < rightDuplicateKeys){
				splitOffset = leftOffset;
				numEntriesToKeep = numEntriesToKeep - leftDuplicateKeys;
			}else{
				splitOffset = rightOffset;
				numEntriesToKeep = numEntriesToKeep + rightDuplicateKeys;
			}
		}
		free(currentKey);
	}
}


RC IndexManager::insertEntryIntoLeaf(IXFileHandle &ixfileHandle, void* nodeData, int nodePageNum, const Attribute attribute,
		const void* key, const RID &rid, int keyLength){ //maintain sorted order
	//leaf has [key,rid],[key,rid]...
	short freeSpacePointer;
	memcpy(&freeSpacePointer, nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));
	short int numOfEntriesInLeaf;
	memcpy(&numOfEntriesInLeaf, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	//find the correct spot for key
	if(attribute.type==TypeInt){
		int toInsert;
		int toStoreAtIndex=numOfEntriesInLeaf; // in case the incoming key is greater than all keys already in the page
		memcpy(&toInsert, (char*) key, sizeof(int));
		for(int i=0;i<numOfEntriesInLeaf;i++){
			int offset=i*(sizeof(int) + 2*sizeof(int));
			int keyVal;
			memcpy(&keyVal, (char*) nodeData + offset, sizeof(int));
			if(toInsert > keyVal){
				continue;
			}
			else{
				toStoreAtIndex=i;
				break;
			}
		}
		//insert at the end
		if(toStoreAtIndex >= numOfEntriesInLeaf){
			short int writeAt = freeSpacePointer;
			memcpy((char*) nodeData + writeAt, &toInsert, sizeof(int));
			writeAt += sizeof(int);
			memcpy((char*) nodeData + writeAt, &rid.pageNum, sizeof(int));
			writeAt += sizeof(int);
			memcpy((char*) nodeData + writeAt, &rid.slotNum, sizeof(int));
		}
		//shift all subsequent data
		else{
			short int offsetToStore = toStoreAtIndex*(sizeof(int) + 2*sizeof(int));
			short int shiftBegin = offsetToStore + sizeof(int) + 2*(sizeof(int));
			short int lengthToShift = freeSpacePointer - offsetToStore;
			memmove((char*) nodeData + shiftBegin, (char*) nodeData + offsetToStore, lengthToShift);
			//now write the new key,rid pair
			memcpy((char*) nodeData + offsetToStore, &toInsert, sizeof(int));
			offsetToStore += sizeof(int);
			memcpy((char*) nodeData + offsetToStore, &rid.pageNum, sizeof(int));
			offsetToStore += sizeof(int);
			memcpy((char*) nodeData + offsetToStore, &rid.slotNum, sizeof(int));
		}
		//update freespacepointer
		freeSpacePointer += sizeof(int) + 2*sizeof(int);
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
		numOfEntriesInLeaf += 1;
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntriesInLeaf, sizeof(short int));
	}
	else if(attribute.type==TypeReal){
		float toInsert;
		int toStoreAtIndex=numOfEntriesInLeaf;
		memcpy(&toInsert, (char*) key, sizeof(float));
		for(int i=0;i<numOfEntriesInLeaf;i++){
			int offset = i*(sizeof(float) + 2*sizeof(int));
			float keyVal;
			memcpy(&keyVal, (char*) nodeData + offset, sizeof(float));
			if(toInsert > keyVal){
				continue;
			}
			else{
				toStoreAtIndex = i;
				break;
			}
		}
		if(toStoreAtIndex >= numOfEntriesInLeaf){
			short int writeAt = freeSpacePointer;
			memcpy((char*) nodeData + writeAt, &toInsert, sizeof(float));
			writeAt += sizeof(float);
			memcpy((char*) nodeData + writeAt, &rid.pageNum, sizeof(int));
			writeAt += sizeof(int);
			memcpy((char*) nodeData + writeAt, &rid.slotNum, sizeof(int));
		}
		//now shift data
		else{
			short int offsetToStore = toStoreAtIndex * (sizeof(float) + 2*sizeof(int));
			short int shiftBegin = offsetToStore + (sizeof(float) + 2*sizeof(int));
			short int lengthToShift = freeSpacePointer - offsetToStore;
			memmove((char*) nodeData + shiftBegin, (char*) nodeData + offsetToStore, lengthToShift);
			//write the key,rid pair
			memcpy((char*) nodeData + offsetToStore, &toInsert, sizeof(float));
			offsetToStore += sizeof(float);
			memcpy((char*) nodeData + offsetToStore, &rid.pageNum, sizeof(int));
			offsetToStore += sizeof(int);
			memcpy((char*) nodeData + offsetToStore, &rid.slotNum, sizeof(int));
		}
		freeSpacePointer += sizeof(float) + 2*(sizeof(int));
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
		numOfEntriesInLeaf += 1;
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntriesInLeaf, sizeof(short int));
	}
	else{ //varchar
		int lenOfStr;
		memcpy(&lenOfStr, (char*) key, sizeof(int));
		char* str=(char*) malloc(PAGE_SIZE);
		memcpy(str, (char*) key + sizeof(int), lenOfStr);
		str[lenOfStr]='\0';
		string toInsertString(str);
		int offset=0;
		short int offsetToInsertAt=freeSpacePointer;
		short int shiftBegin, lengthToShift;
		for(int i=0;i<numOfEntriesInLeaf;i++){
			int lenOfStringInKey;
			memcpy(&lenOfStringInKey, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			char* keyString = (char*) malloc(PAGE_SIZE);
			memcpy(keyString, (char*) nodeData + offset, lenOfStringInKey);
			offset += lenOfStringInKey;
			keyString[lenOfStringInKey]='\0';
			string keyValString(keyString);
			free(keyString);
			//move offset to next string
			offset += (2*sizeof(int));
			if(toInsertString > keyValString){
				continue;
			}
			else{
				offsetToInsertAt = offset - (2*sizeof(int)) - lenOfStringInKey - sizeof(int);
				shiftBegin = offsetToInsertAt + sizeof(int) + lenOfStr + 2 * sizeof(int);
				lengthToShift = freeSpacePointer - offsetToInsertAt;
				break;
			}
		}
		if(offsetToInsertAt == freeSpacePointer){
			short int offset=freeSpacePointer;
			memcpy((char*) nodeData + offset, &lenOfStr, sizeof(int));
			offset += sizeof(int);
			memcpy((char*) nodeData + offset, str, lenOfStr);
			offset += lenOfStr;
			memcpy((char*) nodeData + offset, &rid.pageNum, sizeof(int));
			offset += sizeof(int);
			memcpy((char*) nodeData + offset, &rid.slotNum, sizeof(int));
			offset += sizeof(int);
		}
		else{
			memmove((char*) nodeData + shiftBegin, (char*) nodeData + offsetToInsertAt, lengthToShift);//shift the subsequent data
			memcpy((char*) nodeData + offsetToInsertAt, &lenOfStr, sizeof(int));
			offsetToInsertAt += sizeof(int);
			memcpy((char*) nodeData + offsetToInsertAt, str, lenOfStr);
			offsetToInsertAt += lenOfStr;
			memcpy((char*) nodeData + offsetToInsertAt, &rid.pageNum, sizeof(int));
			offsetToInsertAt += sizeof(int);
			memcpy((char*) nodeData + offsetToInsertAt, &rid.slotNum, sizeof(int));
		}
		freeSpacePointer += (sizeof(int) + lenOfStr + 2*sizeof(int));
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
		numOfEntriesInLeaf += 1;
		memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntriesInLeaf, sizeof(short int));
		//now shift the data
		free(str);
	}
	ixfileHandle.writePage(nodePageNum, nodeData);
	return 0;
}

bool IndexManager::areAllKeysSame(void* nodeData, const Attribute &attribute){
	bool isAllSame = true;
	short int numOfEntries;
	memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	if(attribute.type == TypeInt){
		int key;
		isAllSame = true;
		memcpy(&key, (char*) nodeData, sizeof(int));
		for(int i=1;i<numOfEntries;i++){
			int offset = i*3*sizeof(int);
			int keyInLeaf;
			memcpy(&keyInLeaf, (char*) nodeData + offset, sizeof(int));
			if(key != keyInLeaf){
				isAllSame = false;
				break;
			}
		}
	}
	else if(attribute.type == TypeReal){
		float key;
		isAllSame = true;
		memcpy(&key, (char*) nodeData, sizeof(float));
		for(int i=0;i<numOfEntries;i++){
			int offset = i*(sizeof(float) + 2*sizeof(int));
			float keyInLeaf;
			memcpy(&keyInLeaf, (char*) nodeData + offset, sizeof(float));
			if(key != keyInLeaf){
				isAllSame = false;
				break;
			}
		}
	}
	else{
		char* str = (char*) malloc(PAGE_SIZE);
		int lenOfStr;
		isAllSame = true;
		memcpy(&lenOfStr, (char*) nodeData, sizeof(int));
		memcpy(str, (char*) nodeData + sizeof(int), lenOfStr);
		str[lenOfStr]='\0';
		string key(str);
		free(str);
		int offset = sizeof(int) + lenOfStr + 2*sizeof(int);
		for(int i=0;i<numOfEntries;i++){
			int len;
			memcpy(&len, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			char* strInLeaf = (char*) malloc(PAGE_SIZE);
			memcpy(strInLeaf, (char*) nodeData + offset, len);
			strInLeaf[len] = '\0';
			string keyInLeaf(strInLeaf);
			free(strInLeaf);
			if(key != keyInLeaf){
				isAllSame = false;
				break;
			}
		}
	}
	return isAllSame;
}

RC IndexManager::checkIfNodeHasSpace(bool &hasSpace, void* nodeData, int keyLength, bool isLeaf){
	short freeSpacePointer;
	memcpy(&freeSpacePointer, nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));
	short availableSpace;
	short requiredSpace;

	if(!isLeaf){
		availableSpace =  PAGE_SIZE - freeSpacePointer - NON_LEAF_DIR_SIZE;
		requiredSpace = keyLength + PAGE_NUM_SIZE;
	}else{
		availableSpace = PAGE_SIZE - freeSpacePointer - LEAF_DIR_SIZE;
		requiredSpace = keyLength + RECORD_ID_SIZE;
	}

	if(availableSpace >= requiredSpace){
		hasSpace = true;
	}else{
		hasSpace = false;
	}
	return 0;
}

RC IndexManager::searchKeyInNonLeaf(const Attribute &attribute, const void *nodeData, const void *key, int &keyPointer){
	short int numOfEntries;
	memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	if(attribute.type == TypeInt){
		int searchKey;
		memcpy(&searchKey, (char*) key, sizeof(int));
//		int childPageNo;
		for(int i=0;i<numOfEntries;i++){
			int keyValue;
			memcpy(&keyValue,(char*)nodeData + sizeof(int) + i*2*sizeof(int),sizeof(int));
			//We will continue searching if there are more entries and the search key is greater than the current key value
			if(searchKey > keyValue && ( i + 1 )< numOfEntries){
				continue;
			}
			else{
				if(searchKey >= keyValue){
					int pgNo;
					//get the right pointer
					memcpy(&pgNo, (char*) nodeData + sizeof(int) + i*2*sizeof(int) + sizeof(int),sizeof(int));
					keyPointer = pgNo;
				}
				else{
					int pgNo;
					//get the left pointer
					memcpy(&pgNo, (char*) nodeData + sizeof(int) + i*2*sizeof(int) - sizeof(int),sizeof(int));
					keyPointer = pgNo;
				}
				break;
			}
		}
	}
	else if(attribute.type == TypeReal){
		float floatSearchKey;
		memcpy(&floatSearchKey, (char*)key, sizeof(float));
		for(int i=0;i<numOfEntries;i++){
			float floatKeyValue;
			memcpy(&floatKeyValue,(char*)nodeData + sizeof(int) + i*2*sizeof(int),sizeof(float));
			if(floatSearchKey > floatKeyValue && (i+1) < numOfEntries){
				continue;
			}
			else{
				if(floatSearchKey >= floatKeyValue){
					int pgNo;
					memcpy(&pgNo, (char*)nodeData + sizeof(int) + i*2*sizeof(int) + sizeof(int), sizeof(int));
					keyPointer = pgNo;
				}
				else{
					int pgNo;
					memcpy(&pgNo, (char*)nodeData + sizeof(int) + i*2*sizeof(int) - sizeof(int), sizeof(int));
					keyPointer = pgNo;
				}
				break;
			}
		}
	}
	else{ //varchar
		char* searchKey= (char*)malloc(PAGE_SIZE);
		int strLength;
		memcpy(&strLength, (char*) key, sizeof(int));
		memcpy(searchKey, (char*) key + sizeof(int), strLength);
		searchKey[strLength]='\0';
		string searchString(searchKey);
		free(searchKey);
		int currentOffsetOnNode=sizeof(int);
		for(int i=0;i<numOfEntries;i++){
			char* keyValueString= (char*)malloc(PAGE_SIZE);
			int length;
			memcpy(&length, (char*) nodeData + currentOffsetOnNode, sizeof(int));
			currentOffsetOnNode += sizeof(int);
			memcpy(keyValueString, (char*) nodeData + currentOffsetOnNode, length);
			currentOffsetOnNode += length;
			keyValueString[length]='\0';
			string keyString(keyValueString);
			free(keyValueString);
			if(searchString > keyString && (i+1) < numOfEntries){
				currentOffsetOnNode+=sizeof(int);
				continue;
			}
			else{
				if(searchString >= keyString){
					int pgNo;
					memcpy(&pgNo, (char*) nodeData + currentOffsetOnNode, sizeof(int));
					keyPointer=pgNo;
				}
				else{
					int pgNo;
					memcpy(&pgNo, (char*) nodeData + currentOffsetOnNode - length - 2*sizeof(int), sizeof(int));
					keyPointer=pgNo;
				}
				break;
			}
		}
	}
	return 0;
}

RC IndexManager::createRoot(IXFileHandle &ixfileHandle, const Attribute &attribute, const void * key, int keyLength){
	void *rootData = malloc(PAGE_SIZE);
	bool isLeaf = false;
	memcpy(rootData+PAGE_SIZE-sizeof(bool), &isLeaf, sizeof(bool)); //Write boolean value isLeaf
	short freeSpace = 2*sizeof(int) + keyLength;
	short numOfEntries = 1;
	memcpy(rootData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpace, sizeof(short int)); //write freespace pointer
	memcpy(rootData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));//write num of Entries
	memcpy(rootData+sizeof(int), key, keyLength); //write the key

	//Write New blank leaf pages
	void *leafPage1 = malloc(PAGE_SIZE);
	void *leafPage2 = malloc(PAGE_SIZE);
	int leaf1PageNum = ROOT_PAGE_NUM + 1;
	int leaf2PageNum = ROOT_PAGE_NUM + 2;
	createBlankLeafPage(leafPage1, leaf2PageNum, -1);
	createBlankLeafPage(leafPage2, -1, leaf1PageNum);
	memcpy((char*) rootData, &leaf1PageNum, sizeof(int));
	memcpy((char*) rootData + sizeof(int) + keyLength, &leaf2PageNum, sizeof(int));
	ixfileHandle.writePage(ROOT_PAGE_NUM, rootData); //Write the root page into the file
	ixfileHandle.writePage(leaf1PageNum, leafPage1); //write leaf page 1
	ixfileHandle.writePage(leaf2PageNum, leafPage2); //write leaf page 2

	free(rootData);
	free(leafPage1);
	free(leafPage2);
	return 0;

//	ixfileHandle.appendPage()
}

RC IndexManager::createBlankLeafPage(void* data, int nextLeafPage, int prevLeafPage){
	bool isLeaf = true;
	memcpy(data+PAGE_SIZE -sizeof(bool) , &isLeaf, sizeof(bool)); //write isLeaf at the end of the page
	short freeSpacePointer = 0;
	memcpy(data + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int)); //write freespace pointer
	short numEntries = 0;
	memcpy(data + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int), &numEntries, sizeof(short int)); //write num Entries
	//write next page pointer
	memcpy(data + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int) - sizeof(int), &nextLeafPage, sizeof(int));
	//write previous page pointer
	memcpy(data + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short int) - 2 * sizeof(int), &prevLeafPage, sizeof(int));
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC retVal = deleteEntry(ixfileHandle, ROOT_PAGE_NUM, attribute, key, rid);
	return retVal;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute,
		const void *key, const RID &rid){
	void *nodeData = malloc(PAGE_SIZE);
	ixfileHandle.readPage(nodePageNum, nodeData);
	bool isLeaf;
	memcpy(&isLeaf,(char*) nodeData + PAGE_SIZE - sizeof(bool), sizeof(bool));
	if(!isLeaf){
		int childPointer;
		searchKeyInNonLeaf(attribute, nodeData, key, childPointer);
		RC retVal = deleteEntry(ixfileHandle, childPointer, attribute, key, rid);
		free(nodeData);
		return retVal;

	}else{
		RC retVal = deleteEntryFromLeaf(ixfileHandle, nodeData, nodePageNum, attribute, key, rid);
		free(nodeData);
		return retVal;
	}

}

RC IndexManager::deleteEntryFromLeaf(IXFileHandle &ixfileHandle, void *nodeData, int nodePageNum,
		const Attribute &attibute, const void *key, const RID &rid){
	//return -1 if the entry is not there
	short int freeSpacePointer;
	memcpy(&freeSpacePointer, (char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));
	short int numOfEntries;
	memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	bool deleted=false;
	if(attibute.type == TypeInt){
		int keyToDelete;
		int offsetToDeleteAt = -1;
		memcpy(&keyToDelete, (char*) key, sizeof(int));
		for(int i=0;i<numOfEntries;i++){
			int offset = i*3*sizeof(int);
			int keyInLeaf, leafRidPg, leafRidSl;
			memcpy(&keyInLeaf, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&leafRidPg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&leafRidSl, (char*) nodeData + offset, sizeof(int));
			if(keyInLeaf == keyToDelete && leafRidPg == rid.pageNum && leafRidSl == rid.slotNum){
				//found entry
				offsetToDeleteAt = i*3*sizeof(int);
				break;
			}
		}
		if(offsetToDeleteAt == -1){
			//not found
			deleted = false;
		}
		else{
			int shiftBegin = offsetToDeleteAt + 3*sizeof(int);
			int lengthToShift = freeSpacePointer - shiftBegin;
			//shift the data
			memmove((char*) nodeData + offsetToDeleteAt, (char*) nodeData + shiftBegin, lengthToShift);
			freeSpacePointer -= 3*sizeof(int);
			memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
			numOfEntries -= 1;
			memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));
			deleted = true;
		}
	}
	else if(attibute.type == TypeReal){
		float keyToDelete;
		memcpy(&keyToDelete, (char*) key, sizeof(float));
		int offsetToDeleteAt = -1;
		for(int i=0;i<numOfEntries;i++){
			int offset = i*(sizeof(float) + 2*sizeof(int));
			float keyInLeaf;
			int leafRidPg,leafRidSl;
			memcpy(&keyInLeaf, (char*) nodeData + offset, sizeof(float));
			offset += sizeof(float);
			memcpy(&leafRidPg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&leafRidSl, (char*) nodeData + offset, sizeof(int));
			if(keyInLeaf == keyToDelete && leafRidPg == rid.pageNum && leafRidSl == rid.slotNum){
				offsetToDeleteAt = i*(sizeof(float) + 2*sizeof(int));
				break;
			}
		}
		if(offsetToDeleteAt == -1){
			//not found
			deleted = false;
		}
		else{
			int shiftBegin = offsetToDeleteAt + sizeof(float) + 2*sizeof(int);
			int lengthToShift = freeSpacePointer - shiftBegin;
			memmove((char*) nodeData + offsetToDeleteAt, (char*) nodeData + shiftBegin, lengthToShift);
			freeSpacePointer -= sizeof(float) + 2*sizeof(int);
			memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
			numOfEntries -= 1;
			memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));
			deleted = true;
		}
	}
	else{//varchar
		int lenOfStr, lenOfStringToDelete;
		memcpy(&lenOfStr, (char*) key, sizeof(int));
		char* str = (char*) malloc(PAGE_SIZE);
		memcpy(str, (char*) key + sizeof(int), lenOfStr);
		str[lenOfStr]='\0';
		string keyToDelete(str);
		free(str);
		int offset = 0, offsetToDeleteAt = -1;
		for(int i=0;i<numOfEntries;i++){
			int len, leafRidPg, leafRidSl;
			memcpy(&len, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			char* strInLeaf = (char*) malloc(PAGE_SIZE);
			memcpy(strInLeaf, (char*) nodeData + offset, len);
			offset += len;
			strInLeaf[len]='\0';
			string keyInLeaf(strInLeaf);
			memcpy(&leafRidPg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&leafRidSl, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			free(strInLeaf);
			if(keyToDelete == keyInLeaf && leafRidPg == rid.pageNum && leafRidSl == rid.slotNum){
				offsetToDeleteAt = offset - 3*sizeof(int) - len;
				lenOfStringToDelete = len;
				break;
			}
		}
		if(offsetToDeleteAt == -1){
			//not found
			deleted = false;
		}
		else{
			int shiftBegin = offsetToDeleteAt + sizeof(int) + lenOfStringToDelete + 2*sizeof(int);
			int lenToShift = freeSpacePointer - shiftBegin;
			memmove((char*) nodeData + offsetToDeleteAt, (char*) nodeData + shiftBegin, lenToShift);
			freeSpacePointer -= (sizeof(int) + lenOfStringToDelete + 2*sizeof(int));
			memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), &freeSpacePointer, sizeof(short int));
			numOfEntries -= 1;
			memcpy((char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), &numOfEntries, sizeof(short int));
			deleted = true;
		}
	}
	if(!deleted){ //not found
		return -1;
	}
	ixfileHandle.writePage(nodePageNum, nodeData);
	return 0;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	if(ixfileHandle.fileHandle.fp == NULL){ //if no file is open
		return -1;
	}
	ix_ScanIterator.ixfileHandle = ixfileHandle;
	ix_ScanIterator.attribute = attribute;
	ix_ScanIterator.highKey = highKey;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;
	ix_ScanIterator.pageData = malloc(PAGE_SIZE);
	int leafPageNum;
	findLeafPage(ixfileHandle, attribute, lowKey, leafPageNum);
	ix_ScanIterator.currentPageNum = leafPageNum;
	ixfileHandle.readPage(ix_ScanIterator.currentPageNum, ix_ScanIterator.pageData);
	updateOffset(lowKey, lowKeyInclusive, ix_ScanIterator);
    return 0;
}

void IndexManager::findLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, int &leafPageNum){
	leafPageNum = getLeafPageNum(ixfileHandle, ROOT_PAGE_NUM, attribute, key);
}

int IndexManager::getLeafPageNum(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute, const void*key){
	void* nodeData = malloc(PAGE_SIZE);
	ixfileHandle.readPage(nodePageNum, nodeData);
	bool isLeaf;
	memcpy(&isLeaf, nodeData + PAGE_SIZE - sizeof(bool), sizeof(bool));
	short int freeSpacePointer;
	memcpy(&freeSpacePointer, (char*) nodeData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(int));
	if(!isLeaf){
		int keyPointer;
		if(key == NULL){
			//read the first left pointer if the key is null
			memcpy(&keyPointer, nodeData, PAGE_NUM_SIZE);
		}else{
			searchKeyInNonLeaf(attribute, nodeData, key, keyPointer);
		}
		int retVal = getLeafPageNum(ixfileHandle, keyPointer, attribute, key);
		free(nodeData);
		return retVal;
	}else{
		if(freeSpacePointer == 0){//leaf page is empty
			int nextPage;
			memcpy(&nextPage, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int) - sizeof(int), sizeof(int));
			int ret = -1;
			if(nextPage != -1){
				ret = getLeafPageNum(ixfileHandle, nextPage, attribute, key);
			}
			free(nodeData);
			return ret;
		}
		free(nodeData);
		return nodePageNum;
	}
}

void IndexManager::updateOffset(const void* lowKey, bool lowKeyInclusive, IX_ScanIterator &ix_ScanIterator){
	//update offset, freeSpacePointer, IS_EOF
	//stop the offset when the key is greater than lowKey
	//set IS_EOF to true id the key is not lesser than highKey
	if(ix_ScanIterator.currentPageNum == -1){
		ix_ScanIterator.isEOF = true;
		return;
	}
	void *leafPageData = malloc(PAGE_SIZE);
	ix_ScanIterator.ixfileHandle.readPage(ix_ScanIterator.currentPageNum, leafPageData);
	short freeSpacePointer;
	memcpy(&freeSpacePointer, leafPageData + PAGE_SIZE - sizeof(bool) - sizeof(short), sizeof(short));
	ix_ScanIterator.freeSpacePointer = freeSpacePointer;
	short numEntries;
	memcpy(&numEntries, leafPageData + PAGE_SIZE - sizeof(bool) - 2 * sizeof(short), sizeof(short));
	bool lowerBoundFound = false;
	bool lowerBoundNULL = false;
	if(lowKey == NULL){
		lowerBoundFound = true;
		lowerBoundNULL = true;
		ix_ScanIterator.offset = 0;
	}
	bool isUpperBoundPresent = false;
	bool upperBoundNULL = false;
	if(ix_ScanIterator.highKey == NULL){
		isUpperBoundPresent = true;
		upperBoundNULL = true;
		ix_ScanIterator.isEOF = false;
	}
	if(lowerBoundNULL && upperBoundNULL){
		free(leafPageData);
		return; //already updated the offset, free space pointer and isEOF
	}
	if(ix_ScanIterator.attribute.type == TypeInt){
		int lowKeyValue;
		if(!lowerBoundNULL){
			memcpy(&lowKeyValue, lowKey, sizeof(int));
		}
		int highKeyValue;
		if(!upperBoundNULL){
			memcpy(&highKeyValue, ix_ScanIterator.highKey, sizeof(int));
		}

		for (int var = 0; var < numEntries; ++var) {

			int key;
			memcpy(&key, leafPageData + var * LEAF_INT_ENTRY_SIZE, sizeof(int));

			if (!lowerBoundNULL) {
				if(lowKeyInclusive){
					lowerBoundFound = lowKeyValue <= key;
				}else{
					lowerBoundFound = lowKeyValue < key;
				}
			}

			if(lowerBoundFound){
				if(!lowerBoundNULL){//if lower bound is null the offset is already updated
					ix_ScanIterator.offset = var * LEAF_INT_ENTRY_SIZE;
				}
				if (!upperBoundNULL) {
					if(ix_ScanIterator.highKeyInclusive){
						isUpperBoundPresent = key <= highKeyValue;
					}else{
						isUpperBoundPresent = key < highKeyValue;
					}
					if(isUpperBoundPresent){
						ix_ScanIterator.isEOF = false;
					}else{
						ix_ScanIterator.isEOF = true;
					}
				}
				break;
			}else{
				continue;
			}
		}
		if(!lowerBoundFound){
			ix_ScanIterator.isEOF=true;
		}
		if(numEntries == 0){
			ix_ScanIterator.isEOF = false;
		}

	}else if(ix_ScanIterator.attribute.type == TypeReal){
		float lowKeyValue;
		if(!lowerBoundNULL){
			memcpy(&lowKeyValue, lowKey, sizeof(int));
		}
		float highKeyValue;
		if(!upperBoundNULL){
			memcpy(&highKeyValue, ix_ScanIterator.highKey, sizeof(int));
		}
		for (int var = 0; var < numEntries; ++var) {
			float key;
			memcpy(&key, leafPageData + var * LEAF_INT_ENTRY_SIZE, sizeof(int));
			if(!lowerBoundNULL){
				if(lowKeyInclusive){
					lowerBoundFound = lowKeyValue <= key;
				}else{
					lowerBoundFound = lowKeyValue < key;
				}
			}
			if(lowerBoundFound){
				if(!lowerBoundNULL){
					ix_ScanIterator.offset = var * LEAF_INT_ENTRY_SIZE;
				}
				if (!upperBoundNULL) {
					if(ix_ScanIterator.highKeyInclusive){
						isUpperBoundPresent = key <= highKeyValue;
					}else{
						isUpperBoundPresent = key < highKeyValue;
					}
					if(isUpperBoundPresent){
						ix_ScanIterator.isEOF = false;
					}else{
						ix_ScanIterator.isEOF = true;
					}
				}
				break;
			}else{
				continue;
			}
		}
		if(!lowerBoundFound){
			ix_ScanIterator.isEOF=true;
		}
		if(numEntries == 0){
			ix_ScanIterator.isEOF = false;
		}
	}else if(ix_ScanIterator.attribute.type == TypeVarChar){
		char* lowKeyValue = (char*)malloc(PAGE_SIZE);
		if (!lowerBoundNULL) {
			int lowkey_length;
			memcpy(&lowkey_length, lowKey, sizeof(int));
			memcpy(lowKeyValue, lowKey + sizeof(int), lowkey_length);
			lowKeyValue[lowkey_length] = '\0';
		}

		char* highKeyValue = (char*)malloc(PAGE_SIZE);
		if (!upperBoundNULL) {
			int highkey_length;
			memcpy(&highkey_length, ix_ScanIterator.highKey, sizeof(int));
			memcpy(highKeyValue, ix_ScanIterator.highKey + sizeof(int), highkey_length);
			highKeyValue[highkey_length] = '\0';
		}

		int offset = 0;
		for (int var = 0; var < numEntries; ++var) {
			char* key = (char*)malloc(PAGE_SIZE);
			int key_length;
			memcpy(&key_length, leafPageData + offset, sizeof(int));
			memcpy(key, leafPageData + offset + sizeof(int), key_length);
			key[key_length] = '\0';

			if (!lowerBoundNULL) {
				if(lowKeyInclusive){
					lowerBoundFound = string(lowKeyValue) <= string(key);
				}else{
					lowerBoundFound = string(lowKeyValue) < string(key);
				}
			}

			if(lowerBoundFound){
				if (!lowerBoundNULL) {
					ix_ScanIterator.offset = offset;
				}
				if (!upperBoundNULL) {
					if(ix_ScanIterator.highKeyInclusive){
						isUpperBoundPresent = string(key) <= string(highKeyValue);
					}else{
						isUpperBoundPresent = string(key) < string(highKeyValue);
					}
					if(isUpperBoundPresent){
						ix_ScanIterator.isEOF = false;
					}else{
						ix_ScanIterator.isEOF = true;
					}
				}
				free(key);
				break;
			}else{
				offset += sizeof(int) + key_length + 2*sizeof(int);
				free(key);
				continue;
			}
		}

		if(!lowerBoundFound){
			ix_ScanIterator.isEOF=true;
		}
		if(numEntries == 0){
			ix_ScanIterator.isEOF = false;
		}

		free(lowKeyValue);
		free(highKeyValue);
	}
	free(leafPageData);
}

bool sort_int(const pair<int,int> &a, const pair<int,int> &b){
	if(a.first < b.first) return true;
	if(a.first == b.first){
		if(a.second <= b.second) return true;
		return false;
	}
	return false;
}


RC IndexManager::printLeafNode(void* nodeData, const Attribute &attribute, int numTabs){
	for(int i=0;i<numTabs;i++) cout<<"\t";
	int numOfEntries;
	memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	cout<<"{\"keys\": [";
	if(attribute.type == TypeInt){
		vector< pair <int, pair <int,int> > > output;
		for(int i=0;i<numOfEntries;i++){
			int offset = i*(sizeof(int) + 2*sizeof(int));
			int key,pg,sl;
			memcpy(&key, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&pg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&sl, (char*) nodeData + offset, sizeof(int));
			output.push_back(make_pair(key,make_pair(pg,sl)));
		}
		int len=output.size(),i=0;
		while(i<len){
			vector<pair<int,int> > ridList;
			int keyUnderConsideration = output[i].first;
			ridList.push_back(make_pair(output[i].second.first,output[i].second.second));
			i++;
			while(i<len && output[i].first == keyUnderConsideration){
				ridList.push_back(make_pair(output[i].second.first,output[i].second.second));
				i++;
			}
			sort(ridList.begin(), ridList.end(), sort_int);
			int j=1;
			cout<<"\""<<keyUnderConsideration<<":[";
			cout<<"("<<ridList[0].first<<","<<ridList[0].second<<")";
			while(j<ridList.size()){
				cout<<",("<<ridList[j].first<<","<<ridList[j].second<<")";
				j++;
			}
			cout<<"]\"";
			if(i<len) cout<<",";
//			cout<<"\""<<keyUnderConsideration<<":[";
//			cout<<"("<<output[i].second.first<<","<<output[i].second.second<<")";
//			i++;
//			while(i<len && output[i].first == keyUnderConsideration){
//
//				cout<<",("<<output[i].second.first<<","<<output[i].second.second<<")";
//				i++;
//			}
//			cout<<"]\"";
//			if(i<len) cout<<",";
		}
		cout<<"]}";
	}
	else if(attribute.type == TypeReal){
		vector<pair<float, pair<int,int> > > output;
		for(int i=0;i<numOfEntries;i++){
			int offset = i*(sizeof(float) + 2*sizeof(int));
			float key;
			int pg,sl;
			memcpy(&key, (char*) nodeData + offset, sizeof(float));
			offset += sizeof(float);
			memcpy(&pg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&sl, (char*) nodeData + offset, sizeof(int));
			output.push_back(make_pair(key,make_pair(pg,sl)));
			//cout<<"\""<<key<<":[("<<pg<<","<<sl<<")]\"";
			//if(i!=numOfEntries-1) cout<<",";
		}
		int len=output.size(),i=0;
		while(i<len){
			vector<pair<int,int> > ridList;
			float keyUnderConsideration = output[i].first;
			ridList.push_back(make_pair(output[i].second.first,output[i].second.second));
			i++;
			while(i<len && output[i].first == keyUnderConsideration){
				ridList.push_back(make_pair(output[i].second.first,output[i].second.second));
				i++;
			}
			sort(ridList.begin(), ridList.end(), sort_int);
			int j=1;
			cout<<"\""<<keyUnderConsideration<<":[";
			cout<<"("<<ridList[0].first<<","<<ridList[0].second<<")";
			while(j<ridList.size()){
				cout<<",("<<ridList[j].first<<","<<ridList[j].second<<")";
				j++;
			}
			cout<<"]\"";
			if(i<len) cout<<",";
//			cout<<"\""<<keyUnderConsideration<<":[";
//			cout<<"("<<output[i].second.first<<","<<output[i].second.second<<")";
//			i++;
//			while(i<len && output[i].first == keyUnderConsideration){
//				cout<<",("<<output[i].second.first<<","<<output[i].second.second<<")";
//				i++;
//			}
//			cout<<"]\"";
//			if(i<len) cout<<",";
		}
		cout<<"]}";
	}
	else{
		int offset=0;
		vector< pair<string, pair<int,int> > > output;
		for(int i=0;i<numOfEntries;i++){
			int len;
			memcpy(&len, (char*) nodeData + offset, sizeof(int));
			offset+=sizeof(int);
			char* str = (char*) malloc(PAGE_SIZE);
			memcpy(str, (char*) nodeData + offset, len);
			str[len]='\0';
			string key(str);
			offset+=len;
			int pg,sl;
			memcpy(&pg, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			memcpy(&sl, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			output.push_back(make_pair(key,make_pair(pg,sl)));
//			cout<<"\""<<key<<":[("<<pg<<","<<sl<<")]\"";
//			if(i!=numOfEntries-1) cout<<",";
			free(str);
		}
		int len=output.size(),i=0;
		while(i<len){
			vector<pair<int,int> >ridList;
			string keyUnderConsideration = output[i].first;
			ridList.push_back(make_pair(output[i].second.first,output[i].second.second));
			i++;
			while(i<len && output[i].first == keyUnderConsideration){
				ridList.push_back(make_pair(output[i].second.first,output[i].second.second));
				i++;
			}
			sort(ridList.begin(), ridList.end(), sort_int);
			int j=1;
			cout<<"\""<<keyUnderConsideration<<":[";
			cout<<"("<<ridList[0].first<<","<<ridList[0].second<<")";
			while(j<ridList.size()){
				cout<<",("<<ridList[j].first<<","<<ridList[j].second<<")";
				j++;
			}
			cout<<"]\"";
			if(i<len) cout<<",";
//			cout<<"\""<<keyUnderConsideration<<":[";
//			cout<<"("<<output[i].second.first<<","<<output[i].second.second<<")";
//			i++;
//			while(i<len && output[i].first == keyUnderConsideration){
//				cout<<",("<<output[i].second.first<<","<<output[i].second.first<<")";
//				i++;
//			}
//			cout<<"]\"";
//			if(i<len) cout<<",";
		}
		cout<<"]}";
	}
	return 0;
}

RC IndexManager::printNonLeafNode(void* nodeData, const Attribute &attribute, int numTabs){
	for(int i=0;i<numTabs;i++) cout<<"\t";
	cout<<"{"<<endl;
	for(int i=0;i<numTabs;i++) cout<<"\t";
	int numOfEntries;
	memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
	cout<<"\"keys\":[";
	if(attribute.type == TypeInt){
		for(int i=0;i<numOfEntries;i++){
			int offset = sizeof(int) + i*2*sizeof(int);
			int key;
			memcpy(&key, (char*) nodeData + offset, sizeof(int));
			cout<<"\""<<key<<"\"";
			if(i!=numOfEntries-1){
				cout<<",";
			}
		}
		cout<<"],"<<endl;
		for(int i=0;i<numTabs;i++) cout<<"\t";
		cout<<"\"children\": ["<<endl;
	}
	else if(attribute.type == TypeReal){
		for(int i=0;i<numOfEntries;i++){
			int offset = sizeof(int)+i*(sizeof(float) + sizeof(int));
			float key;
			memcpy(&key, (char*) nodeData + offset, sizeof(float));
			cout<<"\""<<key<<"\"";
			if(i!=numOfEntries-1){
				cout<<",";
			}
		}
		cout<<"],"<<endl;
		for(int i=0;i<numTabs;i++) cout<<"\t";
		cout<<" \"children\": ["<<endl;
	}
	else{//varchar
		int offset = sizeof(int);
		for(int i=0;i<numOfEntries;i++){
			int len;
			memcpy(&len, (char*) nodeData + offset, sizeof(int));
			offset += sizeof(int);
			char* str = (char*) malloc(PAGE_SIZE);
			memcpy(str, (char*) nodeData + offset, len);
			offset += len;
			offset += sizeof(int);
			str[len]='\0';
			string key(str);
			cout<<"\""<<key<<"\"";
			if(i!=numOfEntries-1){
				cout<<",";
			}
			free(str);
		}
		cout<<"],"<<endl;
		for(int i=0;i<numTabs;i++) cout<<"\t";
		cout<<" \"children\": ["<<endl;
	}
	return 0;
}
RC IndexManager::printBTreeUtil(IXFileHandle &ixfileHandle, int pageNum, const Attribute &attribute, int numTabs){
	//first check if its a leaf
	bool isLeaf;
	void* nodeData = malloc(PAGE_SIZE);
	ixfileHandle.readPage(pageNum, nodeData);
	memcpy(&isLeaf, (char*) nodeData + PAGE_SIZE - sizeof(bool), sizeof(bool));
	if(isLeaf){
		if(pageNum == INIT_LEAF_PAGE){
			printLeafNode(nodeData, attribute, numTabs);
		}
		else{
			printLeafNode(nodeData, attribute, numTabs+1);
		}
	}
	else{
		printNonLeafNode(nodeData, attribute, numTabs);
		short int numOfEntries;
		memcpy(&numOfEntries, (char*) nodeData + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int), sizeof(short int));
		if(attribute.type == TypeInt || attribute.type == TypeReal){
			for(int i=0;i<=numOfEntries;i++){
				int offset=i*2*sizeof(int);
				int nextPage;
				memcpy(&nextPage, (char*) nodeData + offset, sizeof(int));
				if(nextPage < 0){
					break;
				}
				printBTreeUtil(ixfileHandle,nextPage,attribute,numTabs+1);
				if(i<numOfEntries){
					cout<<","<<endl;
				}
				else{
					cout<<endl;
				}
			}
		}
		else{
			int offset=0;
			for(int i=0;i<=numOfEntries;i++){
				int nextPage;
				memcpy(&nextPage, (char*) nodeData + offset, sizeof(int));
				if(i!=numOfEntries){
					offset += sizeof(int);
					int length;
					memcpy(&length, (char*) nodeData + offset, sizeof(int));
					offset += sizeof(int) + length;
				}
				if(nextPage < 0){
					break;
				}
				printBTreeUtil(ixfileHandle, nextPage, attribute, numTabs+1);
				if(i<numOfEntries){
					cout<<","<<endl;
				}
			}
		}
		for(int i=0;i<numTabs;i++) cout<<"\t";
		cout<<"]}"<<endl;
	}
	free(nodeData);
	return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	if(ixfileHandle.getNumberOfPages() == 0){//no index exists yet
		cout<<"{\"keys\": []}"<<endl;
		return;
	}
	printBTreeUtil(ixfileHandle, ROOT_PAGE_NUM, attribute, 0);
	cout<<endl;
}

IX_ScanIterator::IX_ScanIterator()
{}

IX_ScanIterator::~IX_ScanIterator()
{}

int IX_ScanIterator::getNextLeafPage(int pageNum){
	void* data = malloc(PAGE_SIZE);
	ixfileHandle.readPage(pageNum, data);
	int nextPage;
	memcpy(&nextPage, (char*) data + PAGE_SIZE - sizeof(bool) - 2*sizeof(short int) - sizeof(int), sizeof(int));
	free(data);
	return nextPage;
}
RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	int numPages = ixfileHandle.getNumberOfPages();
	if(isEOF){
		return IX_EOF;
	}

	while(offset >= freeSpacePointer){
		//currentPageNum ++; //shouldn't we follow the next page number present in leaf - yes!
		currentPageNum = getNextLeafPage(currentPageNum);
		if(currentPageNum < 0){
			isEOF=true;
			return IX_EOF;
		}
		if(currentPageNum >= numPages){
			isEOF = true;
			return IX_EOF;
		}
		offset = 0;

		ixfileHandle.readPage(currentPageNum, pageData);
		short fp;
		memcpy(&fp, pageData + PAGE_SIZE - sizeof(bool) - sizeof(short int), sizeof(short int));
		freeSpacePointer = fp;
		//update free space pointer
	}

	bool condition_satisfied = false;
	bool highKeyNULL = false;
	if(highKey == NULL){
		condition_satisfied = true;
		highKeyNULL = true;
	}
	if(attribute.type == TypeReal || attribute.type == TypeInt){
		if (!highKeyNULL) {
			if(attribute.type == TypeInt){
				int keyValue;
				memcpy(&keyValue, pageData + offset, sizeof(int));

				int highKeyValue;
				memcpy(&highKeyValue, highKey, sizeof(int));

				if(highKeyInclusive){
					condition_satisfied = keyValue <= highKeyValue;
				}else{
					condition_satisfied = keyValue < highKeyValue;
				}
			}else if(attribute.type == TypeReal){
				float keyValue;
				memcpy(&keyValue, pageData + offset, sizeof(int));

				float highKeyValue;
				memcpy(&highKeyValue, highKey, sizeof(int));

				if(highKeyInclusive){
					condition_satisfied = keyValue <= highKeyValue;
				}else{
					condition_satisfied = keyValue < highKeyValue;
				}
			}
		}

		if(!condition_satisfied){
			isEOF = true;
			return IX_EOF;
		}

		memcpy(key, pageData + offset, sizeof(int));
		offset += sizeof(int);

		int pageNum;
		memcpy(&pageNum, pageData + offset, sizeof(int));
		offset += sizeof(int);

		int slotNum;
		memcpy(&slotNum, pageData + offset, sizeof(int));
		offset += sizeof(int);

		rid.pageNum = pageNum;
		rid.slotNum = slotNum;
	}else{

		int keyLength;
		memcpy(&keyLength, pageData + offset, sizeof(int));
		if (!highKeyNULL) {
			char* highKeyValue = (char*)malloc(PAGE_SIZE);
			int highkey_length;
			memcpy(&highkey_length, highKey, sizeof(int));
			memcpy(highKeyValue, highKey + sizeof(int), highkey_length);
			highKeyValue[highkey_length] = '\0';

			char *keyValue = (char*)malloc(PAGE_SIZE);
			memcpy(keyValue, pageData + offset + sizeof(int), keyLength);
			keyValue[keyLength] = '\0';

			if(highKeyInclusive){
				condition_satisfied = string(keyValue) <= string(highKeyValue);
			}else{
				condition_satisfied = string(keyValue) < string(highKeyValue);
			}

			free(highKeyValue);
			free(keyValue);
		}
		if(!condition_satisfied){
			isEOF = true;
			return IX_EOF;
		}
		memcpy(key, pageData + offset, sizeof(int) + keyLength);
		offset += sizeof(int) + keyLength;

		int pageNum;
		memcpy(&pageNum, pageData + offset, sizeof(int));
		offset += sizeof(int);

		int slotNum;
		memcpy(&slotNum, pageData + offset, sizeof(int));
		offset += sizeof(int);

		rid.pageNum = pageNum;
		rid.slotNum = slotNum;

	}


    return 0;
}

RC IX_ScanIterator::close()
{
//	fclose(ixfileHandle.fileHandle.fp);//??should we?
	free(pageData);
	isEOF = true;
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::readPage(PageNum pageNum, void* data){
	if(pageNum < 0) return -1;
	RC retVal = fileHandle.readPage(pageNum, data);
	if(retVal == 0){
		return 0;
	}
	return -1;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
	if(pageNum < 0) return -1;
	RC retVal = fileHandle.writePage(pageNum, data);
	if(retVal == 0){
		return 0;
	}
	return -1;
}
RC IXFileHandle::appendPage(const void *data){
	RC retVal = fileHandle.appendPage(data);
	if(retVal == 0){
		return 0;
	}
	return -1;
}
unsigned IXFileHandle::getNumberOfPages(){
	return fileHandle.getNumberOfPages();
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount=fileHandle.readPageCounter;
	writePageCount=fileHandle.writePageCounter;
	appendPageCount=fileHandle.appendPageCounter;
	return 0;
}

