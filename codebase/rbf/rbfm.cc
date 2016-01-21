#include "rbfm.h"
#include <cstring>
#include <iostream>
#include <cstdlib>
#include <cmath>
#include <cstdio>
#include <algorithm>
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	//create a PFM instance to use openFile, closeFile, createFile, destroyFile functions
	pfm=PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	RC retval=pfm->createFile(fileName);
	if(retval==0){
		return 0;
	}
    return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	RC retval=pfm->destroyFile(fileName);
	if(retval==0){
		return 0;
	}
    return -1;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	RC retval=pfm->openFile(fileName,fileHandle);
	if(retval==0){
		return 0;
	}
    return -1;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	RC retval=pfm->closeFile(fileHandle);
	if(retval==0){
		return 0;
	}
    return -1;
}

RC RecordBasedFileManager::getAvailableFreeSpaceInAPage(FileHandle &fileHandle,int pageNum,short int *freeSpaceSize,short int *offsetOfFreeSpace,short int *freeSpaceEnding,short int *numOfRecordsInPage){
	/*****************************************************************************************************************************************
		 * Available space is calculated by finding the offset of the free space block which can be obtained from the last portion of the page   *
		 * and the end of the free space block is calculated by subtracting the directory entry sizes from the Page Size (4096 B)                *
	******************************************************************************************************************************************/
	void *pageData=malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,pageData); //numOfPages changed to pageNum
	//check if there is enough free space to fit in the record
	short int offOfFree=0;
	short int numberOfElementsInDirectory=0;
	memcpy(&offOfFree,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
	*offsetOfFreeSpace=offOfFree;
	memcpy(&numberOfElementsInDirectory,(char*)pageData+PAGE_SIZE-(2*sizeof(short int)),sizeof(short int));
	*numOfRecordsInPage=numberOfElementsInDirectory;
	*freeSpaceEnding=PAGE_SIZE-(2*sizeof(short int))-(numberOfElementsInDirectory*sizeof(short int))-1;
	*freeSpaceSize=(*freeSpaceEnding)-offOfFree+1;
	free(pageData);
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	/*******************************************************************************************************************
	 * First parse the data into record format which has,                                                              *
	 * First part - Number of Values in the record (basically recordDescriptor.size)   								   *
	 * Second part - Offset of the field endings from the beginning of the record to facilitate O(1) field access  	   *
	 * Third part- Data itself																						   *
	 * This is the format of the record we store inside a page
	 *******************************************************************************************************************/
	//parse begins
	short int numOfFields=(short int)recordDescriptor.size();
	// first calculate the record size
	short int recordSize=0;
	short int offset=ceil((double) numOfFields/CHAR_BIT);
	void *record=malloc(PAGE_SIZE);
	short int offsetOfRecord=0;
	//first copy the number of fields of the record. This points (offsets)
	//to the first data entry in the record.
	memcpy((char *)record+offsetOfRecord,&numOfFields,sizeof(short int));
	offsetOfRecord+=(numOfFields+1)*sizeof(short int);//because the actual data starts after the directory which has
	   	   	   	   	   	   	   	   	   	   	   	   	   //offsets to the ends of the field.
	offset=ceil((double) numOfFields/CHAR_BIT);
	for(short int i=0;i<numOfFields;i++){
		if(((char*)data)[i/8] & (1<<(7-(i%8)))){ //this field is NULL
			//if a field is NULL we represent it with a -1 in the offset value in the corresponding directory entries
			short int len=-1;
			memcpy((char*)record+((i+1)*sizeof(short int)),&len,sizeof(short int));
			continue;
		}
		if(recordDescriptor[i].type==TypeVarChar){
			int len=0;
			memcpy(&len,(char *) data+offset,sizeof(int)); //first find the length of the string which is stored in the data
			offset+=sizeof(int);
			// store the varchar data into record
			memcpy((char *)record+offsetOfRecord,(char*)data+offset,len);//copy the actual string into the record
			offsetOfRecord+=len;
			offset+=len;
			//update the corresponding directory with offset that points to the end of the record
			short int offsetOfFieldEndFromBeginning=offsetOfRecord-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEndFromBeginning,sizeof(short int));
		}
		else if(recordDescriptor[i].type==TypeInt){
			// store the integer into record
			int num=0;
			memcpy(&num,(char *)data+offset,recordDescriptor[i].length);
			memcpy((char *)record+offsetOfRecord,&num,sizeof(int));
			offset+=recordDescriptor[i].length;
			offsetOfRecord+=sizeof(int);
			//update the corresponding directory with offset that points to the end of the record
			short int offsetOfFieldEndFromBeginning=offsetOfRecord-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEndFromBeginning,sizeof(short int));
		}
		else if(recordDescriptor[i].type==TypeReal){
			//store the float value into record
			float fl=0;
			memcpy(&fl,(char*)data+offset,recordDescriptor[i].length);
			memcpy((char *)record+offsetOfRecord,&fl,sizeof(float));
			offset+=recordDescriptor[i].length;
			offsetOfRecord+=sizeof(float);
			//update the corresponding directory with offset that points to the end of the record
			short int offsetOfFieldEndFromBeginning=offsetOfRecord-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEndFromBeginning,sizeof(short int));
		}
	}
	//minimum length of record has to be 8 to accommodate tombstones.
	if(offsetOfRecord < 10){ // added
		offsetOfRecord=10;	// added
	}						// added
	recordSize=offsetOfRecord;
	//parsing ends

	//********************INSERTING RECORD INTO PAGE**************************************
	bool writeOver=false;
	short int availableFreeSpace=0;
	int pageNum=fileHandle.getNumberOfPages()-1;
	if(pageNum==-1){//empty file so append a new page with the record
		void *pageData=malloc(PAGE_SIZE);
		memcpy((char*)pageData,(char*)record,recordSize);
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&recordSize,sizeof(short int)); // update free space pointer
		short int one=1;//there is only one record in this page now, so number of records field should contain 1.
		memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int)),&one,sizeof(short int));
		short int zero=0;
		memcpy((char*)pageData+PAGE_SIZE-(3*sizeof(short int)),&zero,sizeof(short int)); // the directory entry points to the first record that is at zeroth location in the page
		int writeStatus=fileHandle.appendPage(pageData);
		free(pageData);
		if(writeStatus==-1){
			free(record);
			return -1;
		}
		writeOver=true;
		rid.pageNum=0;
		rid.slotNum=0;
		free(record);
		return 0;
	}
	short int offsetOfFreeSpace=0;
	short int freeSpaceEnding=0;
	short int numOfRecordsInPage=0;
	//first check if last page has available free space
	getAvailableFreeSpaceInAPage(fileHandle,pageNum,&availableFreeSpace,&offsetOfFreeSpace,&freeSpaceEnding,&numOfRecordsInPage);
	if(availableFreeSpace>recordSize+(short int)sizeof(short int)){ ///last page has free space available to fit in the record
		void *pageData=malloc(PAGE_SIZE);
		int numOfPages=fileHandle.getNumberOfPages();
		fileHandle.readPage(numOfPages-1,pageData);
		memcpy((char*)pageData+offsetOfFreeSpace,(char*)record,recordSize); // writing data
		//now write the directory entry
		memcpy((char*)pageData+freeSpaceEnding-sizeof(short int)+1,&offsetOfFreeSpace,sizeof(short int));
		offsetOfFreeSpace+=recordSize;
		//now update the number of records
		numOfRecordsInPage+=1;
		memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int)),&numOfRecordsInPage,sizeof(short int));
		//now update free space pointer
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&offsetOfFreeSpace,sizeof(short int));
		rid.pageNum=numOfPages-1;			//pageNum and slotNum are 0-indexed
		rid.slotNum=numOfRecordsInPage-1;
		RC writeStatus=fileHandle.writePage(numOfPages-1,pageData);
		free(pageData);
		if(writeStatus==-1){
			free(record);
			return -1;
		}
		writeOver=true;
		free(record);
		return 0;
	}
	//if last page doesn't have enough space, search for a page from the beginning with free space
	// that can fit in the record
	for(short int i=0;writeOver==false && i<(short int)fileHandle.getNumberOfPages();i++){
		getAvailableFreeSpaceInAPage(fileHandle,i,&availableFreeSpace,&offsetOfFreeSpace,&freeSpaceEnding,&numOfRecordsInPage);
		if(availableFreeSpace>recordSize+(short int)sizeof(short int)){
			void *pageData=malloc(PAGE_SIZE);
			fileHandle.readPage(i,pageData);
			memcpy((char*)pageData+offsetOfFreeSpace,(char*)record,recordSize); // writing data
			memcpy((char*)pageData+freeSpaceEnding-sizeof(short int)+1,&offsetOfFreeSpace,sizeof(short int));
			offsetOfFreeSpace+=recordSize;
			//now update the directory entry
			numOfRecordsInPage+=1;
			memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int)),&numOfRecordsInPage,sizeof(short int));
			//now update free space pointer
			memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&offsetOfFreeSpace,sizeof(short int));
			rid.pageNum=i;				//pageNum and slotNum are 0-indexed
			rid.slotNum=numOfRecordsInPage-1;
			RC writeStatus=fileHandle.writePage(i,pageData);
			free(pageData);
			if(writeStatus==-1){
				free(record);
				return -1;
			}
			writeOver=true;
			free(record);
			return 0;
		}
	}
	if(writeOver==false){ // in case no page is free, we need to append another page
		void *pageData=malloc(PAGE_SIZE);
		memcpy((char*)pageData,(char*)record,recordSize);
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&recordSize,sizeof(short int)); // update free space pointer
		short int one=1;//there is only one record in this page now, so number of records field should contain 1.
		memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int)),&one,sizeof(short int));
		short int zero=0;
		memcpy((char*)pageData+PAGE_SIZE-(3*sizeof(short int)),&zero,sizeof(short int)); // the directory entry points to the first record that is at zeroth location in the page
		int writeStatus=fileHandle.appendPage(pageData);
		free(pageData);
		if(writeStatus==-1){
			free(record);
			return -1;
		}
		writeOver=true;
		rid.pageNum=fileHandle.getNumberOfPages()-1;
		rid.slotNum=0;
		free(record);
		return 0;
	}
	if(writeOver==false){
		free(record);
		return -1;
	}
	free(record);
    return 0;
}

RID RecordBasedFileManager::passTombstones(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	/********************************************************************************************************************************
	 * Given an RID rid, this function goes through all tombstones to get the actual RID of the record.								*
	 * If we try to delete an already deleted record, we can find this by checking the offset of that record in the page.			*
	 * If the offset is -1 then the record was deleted already and the function passes this information to the caller by making		*
	 * 		the slotNum field to be RECORD_DELETED.																					*
	 ********************************************************************************************************************************/
	int pageNum=rid.pageNum;
	int slotNum=rid.slotNum;
	void *pageData=malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,pageData);
	//first check if the record is present or its tombstone
	RID delRID;
	delRID.pageNum=-1; //pageNum -1 indicates record is deleted
	delRID.slotNum=RECORD_DELETED;//this slot number is not possible because all records are < 4K size
	short int startOfRecord;
	memcpy(&startOfRecord,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1)*sizeof(short int)),sizeof(short int));
	if(startOfRecord<0) {//trying to read a deleted record
		free(pageData);
		return delRID;
	}
	short int numOfAttr;
	memcpy(&numOfAttr,(char*)pageData+startOfRecord,sizeof(short int));
	if(numOfAttr==-1){//tombstone
		RID newRID;
		short int newPageNum,newSlotNum;
		memcpy(&newPageNum,(char*)pageData+startOfRecord+sizeof(short int),sizeof(int));
		memcpy(&newSlotNum,(char*)pageData+startOfRecord+sizeof(short int)+sizeof(int),sizeof(int));
		newRID.pageNum=newPageNum;
		newRID.slotNum=newSlotNum;
		RID retRID=passTombstones(fileHandle,recordDescriptor,newRID);
		if(retRID.slotNum==RECORD_DELETED){
			free(pageData);
			return delRID;
		}
		else{
			free(pageData);
			return retRID;
		}
	}
	else{
		free(pageData);
		return rid;
	}
}

RC RecordBasedFileManager::changeFormatF2toF1(const vector<Attribute> &recordDescriptor, void *record, void *data){//F2 is the record stored inside the page, F1 is the data that is given to us in insertRecord format
	/*
	 * The format (F2) is,
	 * < short int for number of attributes >< directory of offset values of the end of each attribute >< actual data >
	 * The F1 format is,
	 * < Null bit vector >< data >
	 * This function converts record which is in format F2 to data which is in format F1.
	 */
	int nullBitVectorSize=ceil((double)recordDescriptor.size()/CHAR_BIT);
	short int dataSize=nullBitVectorSize;
	unsigned char *nullBitVector=(unsigned char *)malloc(nullBitVectorSize);
	memset(nullBitVector,0,nullBitVectorSize); //init null bit vector to all 0
	short int numOfFields=recordDescriptor.size();
	short int start=(numOfFields+1)*sizeof(short int);
	//create the null bit vector
	for(short int i=0;i<numOfFields;i++){
		short int offset=0;
		memcpy(&offset,(char *)record+((i+1)*sizeof(short int)),sizeof(short int));
		if(offset!=-1){ //check if its null
			short int lenOfAttr=offset-start+1;
			dataSize+=lenOfAttr;
			start=offset+1;
		}
		else{
			nullBitVector[i/8]=nullBitVector[i/8] | (1<<(7-(i%8)));
		}
	}
	//we found the length of data. now parse record to data
	memcpy((char*)data,nullBitVector,nullBitVectorSize);
	short int dataOffset=nullBitVectorSize;
	//now copy the attribute values
	start=(numOfFields+1)*sizeof(short int);
	for(short int i=0;i<numOfFields;i++){
		short int offset=0;
		memcpy(&offset,(char*)record+((i+1)*sizeof(short int)),sizeof(short int));
		if(offset!=-1){ //what if offset==-1????
			if(recordDescriptor[i].type==TypeVarChar){
				int lenOfAttr=offset-start+1; //in data it has to be length of varchar followed by the varchar value
				memcpy((char*)data+dataOffset,&lenOfAttr,sizeof(int));
				dataOffset+=sizeof(int);
				memcpy((char*)data+dataOffset,(char*)record+start,lenOfAttr);
				dataOffset+=lenOfAttr;
				start+=lenOfAttr;
			}
			else if(recordDescriptor[i].type==TypeInt){
				memcpy((char*)data+dataOffset,(char*)record+start,recordDescriptor[i].length);
				start+=recordDescriptor[i].length;
				dataOffset+=recordDescriptor[i].length;
			}
			else if(recordDescriptor[i].type==TypeReal){
				memcpy((char*)data+dataOffset,(char*)record+start,recordDescriptor[i].length);
				start+=recordDescriptor[i].length;
				dataOffset+=recordDescriptor[i].length;
			}
		}
	}
	free(nullBitVector);
	return 0;
}

RC RecordBasedFileManager::printAttribute(const vector<Attribute> &recordDescriptor, const string &attributeName, void *data){
	for(int i=0;i<recordDescriptor.size();i++){
		if(recordDescriptor[i].name.compare(attributeName)==0){
			if(((char*)data)[0] & (1<<7)){
				cout<<attributeName<<": "<<"NULL"<<endl;
				return 0;
			}
			if(recordDescriptor[i].type==TypeInt){
				int val;
				memcpy(&val,(char*)data+1,sizeof(int));
				cout<<attributeName<<": "<<val<<endl;
			}
			else if(recordDescriptor[i].type==TypeReal){
				float val;
				memcpy(&val,(char*)data+1,sizeof(float));
				cout<<attributeName<<": "<<val<<endl;
			}
			else{
				int length;
				memcpy(&length,(char*)data+1,sizeof(int));
				char* str=(char*)malloc(length+1);
				memcpy(str,(char*)data+1+sizeof(int),length);
				str[length]='\0';
				printf("%s: %s\n",attributeName.c_str(),str);
				free(str);
			}
		}
	}
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	// first pass the tombstones
	RID tombstonePassedRID=passTombstones(fileHandle,recordDescriptor,rid);//isTombstone has no significance here
	// first read the respective record
	int pageNum=tombstonePassedRID.pageNum;
	int slotNum=tombstonePassedRID.slotNum;
	if(slotNum==RECORD_DELETED){//record is not present
		return -1;
	}
	void *pageData=malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,pageData);
	//get the size of the record first
	short int numOfRecordsInPage=0;
	memcpy(&numOfRecordsInPage,(char*)pageData+PAGE_SIZE-(2*sizeof(short int)),sizeof(short int));
	short int startOffset=0,endOffset=0;
	memcpy(&startOffset,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1)*sizeof(short int)),sizeof(short int));
	if(numOfRecordsInPage-1==slotNum){  //if the reading record is the last then ending of record is given by free space pointer
		memcpy(&endOffset,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
	}
	else{ //else get the end from the directory
		for(int i=1;i<=numOfRecordsInPage-slotNum-1;i++){ //pass deleted records
			memcpy(&endOffset,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+i)*sizeof(short int)),sizeof(short int));
			if(endOffset>=0) break;
		}
		if(endOffset<0){
			memcpy(&endOffset,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
		}
	}
	short int recordLen=endOffset-startOffset;
	void *record=malloc(recordLen);
	memcpy((char*)record,(char*)pageData+startOffset,recordLen);//read the record from page
	changeFormatF2toF1(recordDescriptor,record,data);
	// parse the record into data. [The format with null bit vector and the data]
	// first find the length of the data we will get by converting record format to data format

	free(record);
	free(pageData);
    return 0;
}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // get the null bit vector size
	int nullbytessize=ceil((double) recordDescriptor.size() / CHAR_BIT);
    int numofattr=recordDescriptor.size();
    int offset=nullbytessize;

    for(int i=0;i<numofattr;i++){
    	if(((char*)data)[i/8] & (1<<(7-(i%8)))){		// checks if a particular field is null or not
    		cout<<recordDescriptor[i].name<<": NULL"<<endl;
    		continue;
    	}
    	if(recordDescriptor[i].type==TypeVarChar){ // if type is varchar then 2 fields
    		int length;							   // first find the length
    											   // second is the varchar value itself
    		memcpy(&length,(char*)data+offset,sizeof(int));
    		offset+=sizeof(int);
    		char *str=new char[length+1]();
    		memcpy(str,(char*)data+offset,length);
    		offset+=length;
    		cout<<recordDescriptor[i].name<<": "<<str;
    		free(str);
    	}
    	else{
    		if(recordDescriptor[i].type==TypeInt){
    			int num;
    			memcpy(&num,(char*)data+offset,recordDescriptor[i].length);
    			offset+=recordDescriptor[i].length;
    			cout<<recordDescriptor[i].name<<": "<<num;
    		}
    		else if(recordDescriptor[i].type==TypeReal){
    			float fnum;
    			memcpy(&fnum,(char*) data+offset,recordDescriptor[i].length);
    			offset+=recordDescriptor[i].length;
    			cout<<recordDescriptor[i].name<<": "<<fnum;
    		}
    	}
    cout<<"\t";
    }
    cout<<endl;
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,const RID &rid, const string &attributeName, void *data){
	/*******************************************************************************************************************************
	 * This function reads the attribute and returns it in data. The data has first byte as the null bit vector with only the MSB  *
	 * signifying whether the attribute is there or not.																		   *
	 *******************************************************************************************************************************/
	void *pageData=malloc(PAGE_SIZE);
	readRecord(fileHandle,recordDescriptor,rid,pageData);
	int numOfAttr=recordDescriptor.size();
	char nullByte=0;
	int offsetOnRecord=ceil((double) recordDescriptor.size()/CHAR_BIT);
	for(int i=0;i<numOfAttr;i++){
		if(recordDescriptor[i].name.compare(attributeName)==0){
			if(( (char*) pageData )[i/8] & ( 1<< (7-(i%8) ) )){ //the field is null
				nullByte = nullByte | (1<<7);
			}
			else{
				if(recordDescriptor[i].type==TypeInt){
					int d=0;
					memcpy(&d,(char*)pageData+offsetOnRecord,recordDescriptor[i].length);
					memcpy((char*)data+1,&d,recordDescriptor[i].length); // +1 for the null bit vector which is of size 1 always
				}
				else if(recordDescriptor[i].type==TypeReal){
					float f = 0;
					memcpy(&f,(char*)pageData+offsetOnRecord,recordDescriptor[i].length);
					memcpy((char *)data+1,&f,recordDescriptor[i].length);
				}
				else if(recordDescriptor[i].type==TypeVarChar){
					int len=0;
					memcpy(&len,(char *)pageData+offsetOnRecord,sizeof(int));
					offsetOnRecord+=sizeof(int);
					memcpy((char*)data+1,&len,sizeof(int));
					memcpy((char*)data+1+sizeof(int),(char*)pageData+offsetOnRecord,len);// +1 for null bit vector, sizeofint for the length of the string
//					offsetOnRecord+=len;
				}
			}
			break;
		}
		else{
			if(( (char*) pageData )[i/8] & ( 1<< (7-(i%8) ) )){
				continue;
			}
			else if(recordDescriptor[i].type==TypeInt || recordDescriptor[i].type==TypeReal){
				offsetOnRecord+=recordDescriptor[i].length;
			}
			else if(recordDescriptor[i].type==TypeVarChar){
				int len=0;
				memcpy(&len,(char *)pageData+offsetOnRecord,sizeof(int));
				offsetOnRecord+=(len+sizeof(int));
			}
		}
	}
	memcpy((char*)data,&nullByte,sizeof(char));
	free(pageData);
	return 0;
}



RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	/**********************************************************************************************************************************
	 * This function deletes a given record. If this record was already deleted then we return -1.									  *
	 * The function first passes through the tombstones in case there are any.														  *
	 * If there are no tombstones, we delete the record as such. If there are tombstones, we delete both the data and the tombstone.  *
	 **********************************************************************************************************************************/
	RID newRID=passTombstones(fileHandle,recordDescriptor,rid);
	if(newRID.slotNum==RECORD_DELETED){//trying to delete an existing record
		return -1;
	}
	//now delete the data
	RC retval=deleteRecordUtil(fileHandle,recordDescriptor,newRID);
	if(retval!=0) return -1;
	if(newRID.pageNum!=rid.pageNum && newRID.slotNum!=rid.slotNum){// then delete the first tombstone
		retval=deleteRecordUtil(fileHandle,recordDescriptor,rid);
		if(retval!=0) return -1;
	}
	return 0;
}

RC RecordBasedFileManager::deleteRecordUtil(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	/********************************************************
	 * This function deletes a record identified by rid.	*
	 ********************************************************/
	//rid's of records that are shifted during compaction will not change
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	//get the record starting and ending first
	void *pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,pageData);
	short int numOfRecordsInPage=0;
	memcpy(&numOfRecordsInPage,(char*)pageData+PAGE_SIZE-(2*sizeof(short int)),sizeof(short int));
	short int startOfRecord,endOfRecord;
	memcpy(&startOfRecord,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1)*sizeof(short int)),sizeof(short int));
	if(numOfRecordsInPage-1 == slotNum){
		memcpy(&endOfRecord,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
	}
	else{//endOfRecord points to the start of the next record
		for(int count=1;count<=numOfRecordsInPage-slotNum-1;count++){
			memcpy(&endOfRecord,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
			if(endOfRecord>=0){
				break;
			}
		}
		if(endOfRecord<0){
			memcpy(&endOfRecord,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
		}
	}
	short int lengthOfRecord = endOfRecord - startOfRecord;
	//now to delete the record overwrite the record with the next record and so on in blocks of lengthOfRecord sizes
	int startCopy=endOfRecord;
	int startOverwrite=startOfRecord;
	short int startOfFreeSpace;
	memcpy(&startOfFreeSpace,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
	short int newStartOfFreeSpace=startOfFreeSpace-lengthOfRecord;
	short int lengthToShift=startOfFreeSpace - startCopy;
	//now shift the record data
	memmove((char*)pageData+startOverwrite,(char*)pageData+startCopy,lengthToShift);
	//now shifting is done. update freepsacepointer and directory entries.
	memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&newStartOfFreeSpace,sizeof(short int));
	//update the directory entry for the deleted node as -1
	short int minusOne=-1;
	memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1)*sizeof(short int)),&minusOne,sizeof(short int));
	//now decrement the directory entries of records after the deleted records by lengthOfRecord
	for(short int count=1;count<=numOfRecordsInPage-slotNum-1;count++){
		short int currentOffset;
		//get the existing offset
		memcpy(&currentOffset,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
		if(currentOffset!=-1){//for a record that was already deleted we shouldn't modify the offset
			currentOffset-=lengthOfRecord;
			//decrement by the appropriate length and restore
			memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),&currentOffset,sizeof(short int));
		}
	}
	fileHandle.writePage(pageNum,pageData);
	free(pageData);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
	/*****************************************************************************************************************************************
	 * This function updates the record identified by rid with data.																		 *
	 * First pass through the tombstones to get to the actual location of the record. It is enough to update this record.					 *
	 * While updating, if the new record size is same as the old record size, we just overwrite.											 *
	 * If the new record size is less than the old record size, we overwrite the old record and do compaction to coalesce the free space     *
	 * If the new record size is more than the old record size and if the current page can hold it, we shift data to the right		         *
	 * and write the record into its place. If the current page cannot hold, then delete the record and insert it into a page with available *
	 * memory. Then insert a tombstone in the original place and do compaction.																 *
	 *****************************************************************************************************************************************/
	//first pass the tombstones
	RID tombstonePassedRID=passTombstones(fileHandle,recordDescriptor,rid);
	int pageNum=tombstonePassedRID.pageNum;
	int slotNum=tombstonePassedRID.slotNum;
	if(slotNum==RECORD_DELETED){//trying to update a deleted record
		return -1;
	}
	void *pageData=malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,pageData);
	//find the current record size
	short int currentRecordSize,startOfRecord,endOfRecord;
	//find the size of the record to be updated
	memcpy(&startOfRecord,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1)*sizeof(short int)),sizeof(short int));
	short int numOfRecordsInPage;
	memcpy(&numOfRecordsInPage,(char*)pageData+PAGE_SIZE-(2*sizeof(short int)),sizeof(short int));
	if(numOfRecordsInPage-1==slotNum){
		memcpy(&endOfRecord,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
	}
	else{
		memcpy(&endOfRecord,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+2)*sizeof(short int)),sizeof(short int));
	}
	currentRecordSize=endOfRecord-startOfRecord;//size of record before updation
	//parse the given data into record format.
	short int numOfFields=(short int)recordDescriptor.size();
	// first calculate the record size
	short int recordSize=0;
	short int offset=ceil((double) numOfFields/CHAR_BIT);
	void *record=malloc(PAGE_SIZE);
	short int offsetOfRecord=0;
	//first copy the number of fields of the record. This points (offsets)
	//to the first data entry in the record.
	memcpy((char *)record+offsetOfRecord,&numOfFields,sizeof(short int));
	offsetOfRecord+=(numOfFields+1)*sizeof(short int);//because the actual data starts after the directory which has
	   	   	   	   	   	   	   	   	   	   	   	   	   //offsets to the ends of the field.
	offset=ceil((double) numOfFields/CHAR_BIT);
	for(short int i=0;i<numOfFields;i++){
		if(((char*)data)[i/8] & (1<<(7-(i%8)))){ //this field is NULL
			//if a field is NULL we represent it with a -1 in the offset value in the corresponding directory entries
			short int len=-1;
			memcpy((char*)record+((i+1)*sizeof(short int)),&len,sizeof(short int));
			continue;
		}
		if(recordDescriptor[i].type==TypeVarChar){
			int len=0;
			memcpy(&len,(char *) data+offset,sizeof(int)); //first find the length of the string which is stored in the data
			offset+=sizeof(int);
			// store the varchar data into record
			memcpy((char *)record+offsetOfRecord,(char*)data+offset,len);//copy the actual string into the record
			offsetOfRecord+=len;
			offset+=len;
			//update the corresponding directory with offset that points to the end of the record
			short int offsetOfFieldEndFromBeginning=offsetOfRecord-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEndFromBeginning,sizeof(short int));
		}
		else if(recordDescriptor[i].type==TypeInt){
			// store the integer into record
			int num=0;
			memcpy(&num,(char *)data+offset,recordDescriptor[i].length);
			memcpy((char *)record+offsetOfRecord,&num,sizeof(int));
			offset+=recordDescriptor[i].length;
			offsetOfRecord+=sizeof(int);
			//update the corresponding directory with offset that points to the end of the record
			short int offsetOfFieldEndFromBeginning=offsetOfRecord-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEndFromBeginning,sizeof(short int));
		}
		else if(recordDescriptor[i].type==TypeReal){
			//store the float value into record
			float fl=0;
			memcpy(&fl,(char*)data+offset,recordDescriptor[i].length);
			memcpy((char *)record+offsetOfRecord,&fl,sizeof(float));
			offset+=recordDescriptor[i].length;
			offsetOfRecord+=sizeof(float);
			//update the corresponding directory with offset that points to the end of the record
			short int offsetOfFieldEndFromBeginning=offsetOfRecord-1;
			memcpy((char *)record+i*sizeof(short int)+sizeof(short int),&offsetOfFieldEndFromBeginning,sizeof(short int));
		}
	}
	if(offsetOfRecord<10){
		offsetOfRecord=10;
	}
	recordSize=offsetOfRecord;//size of record after update
	//parse ends
	//update the record
	if(recordSize < currentRecordSize){//have to do compaction PROBLEM here
		//copy the new updated record into the location
		memcpy((char*)pageData+startOfRecord,(char*)record,recordSize);
		//shift the other part
		short int startOfFreeSpace;
		memcpy(&startOfFreeSpace,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
		short int lengthToShift=startOfFreeSpace-endOfRecord; //can be -1 so check
		//now do the shifting
		memmove((char*)pageData+startOfRecord+recordSize,(char*)pageData+endOfRecord,lengthToShift);
		//update the free space pointer
		short int memSaved=currentRecordSize-recordSize;
		short int newStartOfFreeSpace=startOfFreeSpace-memSaved;
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&newStartOfFreeSpace,sizeof(short int));
		//decrement offset in the directory
		for(int count=1;count<=numOfRecordsInPage-slotNum-1;count++){
			short int currentOffset;
			memcpy(&currentOffset,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
			if(currentOffset!=-1){//a record which is already deleted
				currentOffset-=memSaved;
				memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),&currentOffset,sizeof(short int));
			}
		}
	}
	else if(recordSize == currentRecordSize){
		//just overwrite the record
		memcpy((char*)pageData+startOfRecord,(char*)record,recordSize);
	}
	else if(recordSize > currentRecordSize){
		short int freeSpaceSize,freeSpaceEnding,startOfFreeSpace,numOfRecordsInPage2;
		getAvailableFreeSpaceInAPage(fileHandle,pageNum,&freeSpaceSize,&startOfFreeSpace,&freeSpaceEnding,&numOfRecordsInPage2);
		short int memNeeded=recordSize-currentRecordSize;
		short int lengthToShift=startOfFreeSpace-endOfRecord;
		if(freeSpaceSize > memNeeded){//expand in same page
			//first push the data to create room for new record
			memmove((char*)pageData+endOfRecord+memNeeded,(char*)pageData+endOfRecord,lengthToShift);
			//copy the new record
			memcpy((char*)pageData+startOfRecord,(char*)record,recordSize);
			//update free space pointer
			short int newStartOfFreeSpace=startOfFreeSpace+memNeeded;
			memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&newStartOfFreeSpace,sizeof(short int));
			//update the offsets in the directory
			for(int count=1;count<=numOfRecordsInPage-slotNum-1;count++){
				short int currentOffset;
				memcpy(&currentOffset,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
				if(currentOffset!=-1){
					currentOffset+=memNeeded;
					memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),&currentOffset,sizeof(short int));
				}
			}
		}
		else{//tombstone
			//insert the record into the appropriate place
			RID ridOfMigratedRecord;
			insertRecord(fileHandle,recordDescriptor,data,ridOfMigratedRecord);
			void* tombstone=malloc(PAGE_SIZE);
			short int minus=-1;
			//first 2 bytes denote the number of attributes. -1 indicates a tombstone
			memcpy((char*)tombstone,&minus,sizeof(short int));
			int newPageNum=ridOfMigratedRecord.pageNum;
			int newSlotNum=ridOfMigratedRecord.slotNum;
			memcpy((char*)tombstone+sizeof(short int),&newPageNum,sizeof(int));
			memcpy((char*)tombstone+sizeof(short int)+sizeof(int),&newSlotNum,sizeof(int));
			short int tombSize=sizeof(short int)+2*sizeof(int);
			//write the tombstone to the page
			memcpy((char*)pageData+startOfRecord,(char*)tombstone,tombSize);
			memcpy(&startOfFreeSpace,(char*)pageData+PAGE_SIZE-sizeof(short int),sizeof(short int));
			short int memSaved=endOfRecord-(startOfRecord+tombSize);
			short int newStartOfFreeSpace=startOfFreeSpace-memSaved;
			//shift the records
			short int lengthToShift=startOfFreeSpace-endOfRecord;
			memmove((char*)pageData+startOfRecord+tombSize,(char*)pageData+endOfRecord,lengthToShift);
			//update freespace pointer
			memcpy((char*)pageData+PAGE_SIZE-sizeof(short int),&newStartOfFreeSpace,sizeof(short int));
			//update the offsets in the directory
			for(int count=1;count<=numOfRecordsInPage-slotNum-1;count++){
				short int currentOffset;
				memcpy(&currentOffset,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),sizeof(short int));
				if(currentOffset!=-1){
					currentOffset-=memSaved;
					memcpy((char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+count)*sizeof(short int)),&currentOffset,sizeof(short int));
				}
			}
			free(tombstone);
		}
	}
	fileHandle.writePage(pageNum,pageData);
	free(pageData);
	free(record);
	return 0;
}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator){

	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.value = value;
	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.numPages = fileHandle.getNumberOfPages();
	rbfm_ScanIterator.currentPageNum = 0;
	rbfm_ScanIterator.nextSlotNum = 0;
	rbfm_ScanIterator.isEOF = false;
	return 0;

}


// Never keep the results in the memory. When getNextRecord() is called,
// a satisfying record needs to be fetched from the file.
// "data" follows the same format as RecordBasedFileManager::insertRecord().
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	/******************************************************************************************************************
	 * We iterate over all records and check if its valid. If valid, we convert it into the API format and return it. *
	 ******************************************************************************************************************/

	void * recordData = malloc(PAGE_SIZE);
	bool validRecord = true;
	do{
		getNextRecordUtil(rid, recordData);	//in format 2 i.e., how we store inside file
		if(isEOF){
			free(recordData);
			return RBFM_EOF;
		}
		validRecord = isRecordValid(recordData);

	}while(!validRecord);
	//have only the required attributes
	//getting the required attributes from the record and reconstructing the record.
	int numOfAttributes=attributeNames.size();
	int nullBitVectorSize=ceil((double)numOfAttributes/CHAR_BIT);
	memset(data,0,nullBitVectorSize);
	int offsetOnData,offsetOnRecord;
	offsetOnData=nullBitVectorSize;
	short int numOfAttributesInRecord;
	memcpy(&numOfAttributesInRecord,(char*)recordData,sizeof(short int));
	offsetOnRecord=(int)(numOfAttributesInRecord+1)*sizeof(short int);
	for(int j=0;j<attributeNames.size();j++){
		offsetOnRecord=(int)(numOfAttributesInRecord+1)*sizeof(short int);
		for(int i=0;i<recordDescriptor.size();i++){
			bool isNeeded=false;
			if(recordDescriptor[i].name==attributeNames[j]){
				isNeeded=true;
			}
			if(isNeeded){
				short int endOfAttribute;
				memcpy(&endOfAttribute,(char*)recordData+sizeof(short int)+(i*sizeof(short int)),sizeof(short int));
				if(endOfAttribute<0){//attribute is null
					((char*)data)[i/8] |= ( 1 << (7-(i%8)) );
				}
				else{
					if(recordDescriptor[i].type==TypeInt){
						int val;
						memcpy(&val,(char*)recordData+offsetOnRecord,recordDescriptor[i].length);
						offsetOnRecord+=recordDescriptor[i].length;
						memcpy((char*)data+offsetOnData,&val,sizeof(int));
						offsetOnData+=sizeof(int);
					}
					else if(recordDescriptor[i].type==TypeReal){
						float val;
						memcpy(&val,(char*)recordData+offsetOnRecord,recordDescriptor[i].length);
						offsetOnRecord+=recordDescriptor[i].length;
						memcpy((char*)data+offsetOnData,&val,sizeof(float));
						offsetOnData+=sizeof(float);
					}
					else if(recordDescriptor[i].type==TypeVarChar){
						int len=endOfAttribute-offsetOnRecord+1;
						memcpy((char*)data+offsetOnData,&len,sizeof(int));
						offsetOnData+=sizeof(int);
						memcpy((char*)data+offsetOnData,(char*)recordData+offsetOnRecord,len);
						offsetOnRecord+=len;
						offsetOnData+=len;
					}
				}
				break;
			}
			else{
				//first check if the attr is present in record
				short int endOfAttribute;
				memcpy(&endOfAttribute,(char*)recordData+sizeof(short int)+(i*sizeof(short int)),sizeof(short int));
				if(endOfAttribute>=0){
					if(recordDescriptor[i].type==TypeInt || recordDescriptor[i].type==TypeReal){
						offsetOnRecord+=recordDescriptor[i].length;
					}
					else if(recordDescriptor[i].type==TypeVarChar){
						int len=endOfAttribute-offsetOnRecord+1;
						offsetOnRecord+=len;
					}
				}
			}
		}
	}
	free(recordData);
	return 0;
}

bool RBFM_ScanIterator::isRecordValid(void * recordDataFormat2){
	/*
	 * This function checks if the record is valid. A record is valid if its not a tombstone, or if it was not deleted
	 * and satisfies the condition attribute value.
	 */
	int indexOfAttribute;
	short int checkTombstone;
	memcpy(&checkTombstone,(char*)recordDataFormat2,sizeof(short int));
	if(checkTombstone<0){ //its a tombstone or a deleted record
		return false;
	}
	if(compOp==NO_OP){
		return true;
	}
	for(int i=0;i<recordDescriptor.size();i++){
		if(recordDescriptor[i].name.compare(conditionAttribute) == 0){ //same strings
			indexOfAttribute=i;
			break;
		}
	}
	short int endOfAttribute;
	memcpy(&endOfAttribute,(char*)recordDataFormat2+((indexOfAttribute+1)*sizeof(short int)),sizeof(short int));
	if(endOfAttribute==-1) return false;//NULL values are not supported
	short int startOfAttribute=0;
	for(int i=indexOfAttribute-1;i>=0;i--){
		memcpy(&startOfAttribute,(char*)recordDataFormat2+((i+1)*sizeof(short int)),sizeof(short int));
		if(startOfAttribute>=0){
			startOfAttribute+=1;
			break;
		}
	}
	if(startOfAttribute<=0){//the attribute is first
		short int numOfAttributes;
		memcpy(&numOfAttributes,(char*)recordDataFormat2,sizeof(short int));
		startOfAttribute=(numOfAttributes+1)*sizeof(short int);
	}
	if(recordDescriptor[indexOfAttribute].type==TypeInt){
		int valueInRecord;
		memcpy(&valueInRecord,(char*)recordDataFormat2+startOfAttribute,sizeof(int));
		int valToCheck;
		memcpy(&valToCheck,(char*)value,sizeof(int));
		if(compOp==EQ_OP){
			if(valToCheck==valueInRecord){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LT_OP){
			if(valueInRecord<valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LE_OP){
			if(valueInRecord<=valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GT_OP){
			if(valueInRecord>valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GE_OP){
			if(valueInRecord >= valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NE_OP){
			if(valueInRecord!=valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NO_OP){
			return true;
		}

	}
	else if(recordDescriptor[indexOfAttribute].type==TypeReal){
		float valueInRecord;
		memcpy(&valueInRecord,(char*)recordDataFormat2+startOfAttribute,sizeof(float));
		float valToCheck;
		memcpy(&valToCheck,(char*)value,sizeof(float));
		if(compOp==EQ_OP){
			if(valToCheck==valueInRecord){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LT_OP){
			if(valueInRecord<valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LE_OP){
			if(valueInRecord<=valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GT_OP){
			if(valueInRecord>valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GE_OP){
			if(valueInRecord >= valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NE_OP){
			if(valueInRecord!=valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NO_OP){
			return true;
		}
	}
	else if(recordDescriptor[indexOfAttribute].type==TypeVarChar){
		int lenOfStr=endOfAttribute-startOfAttribute+1;
		char* str=(char*)malloc(lenOfStr+1);
		memcpy(str,(char*)recordDataFormat2+startOfAttribute,lenOfStr);
		str[lenOfStr]='\0';
		string valueInRecord(str);
		free(str);
		int length;
		memcpy(&length,(char*)value,sizeof(int));
		char * str2=(char*)malloc(length+1);
		memcpy(str2,(char*)value+sizeof(int),length);
		str2[length]='\0';
		string valToCheck(str2);
		free(str2);
		if(compOp==EQ_OP){
			if(valToCheck==valueInRecord){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LT_OP){
			if(valueInRecord<valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==LE_OP){
			if(valueInRecord<=valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GT_OP){
			if(valueInRecord>valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==GE_OP){
			if(valueInRecord >= valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NE_OP){
			if(valueInRecord!=valToCheck){
				return true;
			}
			else{
				return false;
			}
		}
		else if(compOp==NO_OP){
			return true;
		}
//		free(str);
//		free(str2);
	}
	return false;
}

RC RBFM_ScanIterator::getNextRecordUtil(RID &rid, void * recordDataFormat2){
	/*
	 * This function returns the next record and maintains the page number and slot number to be read next
	 */
	if(currentPageNum < numPages){

			void* pageData = malloc(PAGE_SIZE);
			fileHandle.readPage(currentPageNum, pageData);
			int numRecords = 0;
			memcpy(&numRecords, (char*)pageData + ( PAGE_SIZE - 2 * sizeof(short)), sizeof(short));
			if(nextSlotNum < numRecords){
				rid.pageNum=currentPageNum;
				rid.slotNum=nextSlotNum;
				readRecordFromPage(pageData, nextSlotNum, numRecords, recordDataFormat2);
				nextSlotNum ++;
			}else{ //(nextSlotNum == numRecords) is true. Already read the last record of the current page
				bool isLastPage = (currentPageNum == numPages -1);
				currentPageNum ++;
				if(!isLastPage){
					nextSlotNum = 0; //initialize the slot number to zero, since it is a new page
					fileHandle.readPage(currentPageNum, pageData);
					rid.pageNum=currentPageNum;
					rid.slotNum=nextSlotNum;
					memcpy(&numRecords, (char*)pageData + ( PAGE_SIZE - 2 * sizeof(short)), sizeof(short));

					readRecordFromPage(pageData, nextSlotNum, numRecords, recordDataFormat2);
					nextSlotNum ++;
				}else{ // it is a last page and all the records have been read.
					isEOF = true;
				}

			}

			free(pageData);
	}
	else{
		isEOF = true;
	}
	return 0;
}

RC RBFM_ScanIterator::readRecordFromPage(void* pageData, int slotNum, int numRecords, void* recordData){
	/*
	 * This function reads the record at slotNum of the pageData and returns it in recordData
	 */
	short int recordStart = 0;
	memcpy(&recordStart, (char*) pageData + (PAGE_SIZE - (2 + slotNum + 1)*sizeof(short)), sizeof(short));
	if(recordStart<0){//a deleted record. for ease, make this similar to a tombstone. tombstone records will be invalidated in future processing
		short int minusOne=-1;
		memcpy((char*)recordData,&minusOne,sizeof(short int));
		return 0;
	}
	short int recordEnd = 0; //recordEnd is the start of the next record
	if(slotNum + 1 == numRecords){//it is the last record. Use the free space pointer to get the recordEnd
		int freeSpacePointer = 0;
		memcpy(&freeSpacePointer, (char*)pageData + (PAGE_SIZE - sizeof(short)), sizeof(short));
		recordEnd = freeSpacePointer;
	}else{
		for(int i=1;i<=numRecords-slotNum-1;i++){ //get the first non deleted records beginning to find the end.
			memcpy(&recordEnd,(char*)pageData+PAGE_SIZE-(2*sizeof(short int))-((slotNum+1+i)*sizeof(short int)),sizeof(short int));

		}
		if(recordEnd<0){
			memcpy(&recordEnd, (char*)pageData + (PAGE_SIZE - sizeof(short)), sizeof(short));
		}
	}
	memcpy(recordData, (char*)pageData + recordStart, recordEnd - recordStart);
	return 0;
}


RC RBFM_ScanIterator::close() {
	fclose(fileHandle.fp);
	return 0;
}
