#include <cstdlib>
#include <iostream>
#include <cmath>
#include <cstdio>
#include "rm.h"
#include <cstring>

using namespace std;

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	indexManager = IndexManager::instance();
	populateTablesAttributes(); //create the record descriptor for tables table
	populateColumnsAttributes(); //create the record descriptor for columns table
	populateIndexAttributes(); //create the record descriptor for index table
}

RelationManager::~RelationManager()
{
	tablesAttributes.clear();
	columnsAttributes.clear();
	indexAttributes.clear();
}

RC RelationManager::createCatalog()
{

	RC retVal = rbfm->createFile(TABLES_FILE);
	if(retVal < 0){
		return retVal;
	}

	RC retVal2 = rbfm->createFile(COLUMNS_FILE);
	if(retVal2 < 0){
		return retVal2;
	}

	RC retVal3 = rbfm->createFile(INDEX_TABLE_FILENAME);
	if(retVal3 < 0){
		return retVal3;
	}
	FileHandle tablesFileHandle;
	RC retval=rbfm->openFile(TABLES_FILE, tablesFileHandle);
	if(retval!=0){
		return -1;
	}
	RID rid;
	void * tablesTableRecord = malloc(PAGE_SIZE);
	createRecordInTablesTable(tablesTableRecord, TABLES_TABLE_ID, TABLES_TABLE_NAME, TABLES_FILE, SYSTEM_TABLE);
	retval=rbfm->insertRecord(tablesFileHandle, tablesAttributes, tablesTableRecord, rid);
	if(retval!=0){
		return -1;
	}
	free(tablesTableRecord);

	void* columnsTableRecord = malloc(PAGE_SIZE);
	createRecordInTablesTable(columnsTableRecord, COLUMNS_TABLE_ID, COLUMNS_TABLE_NAME, COLUMNS_FILE, SYSTEM_TABLE);
	rbfm->insertRecord(tablesFileHandle, tablesAttributes, columnsTableRecord, rid);
	free(columnsTableRecord);

	void* indexTablesRecord = malloc(PAGE_SIZE);
	createRecordInTablesTable(indexTablesRecord, INDEX_TABLE_ID, INDEX_TABLE_NAME, INDEX_TABLE_FILENAME, SYSTEM_TABLE);
	rbfm->insertRecord(tablesFileHandle, tablesAttributes, indexTablesRecord, rid);
	free(indexTablesRecord);

	FileHandle columnsFileHandle;
	rbfm->openFile(COLUMNS_FILE, columnsFileHandle);

	void * columnsRecord = malloc(PAGE_SIZE);
	for (int var = 0; var < tablesAttributes.size(); ++var) {
		Attribute currAttr = tablesAttributes[var];
		createRecordInColumnsTable(columnsRecord, TABLES_TABLE_ID, currAttr.name, currAttr.type, currAttr.length, var+1, SYSTEM_TABLE);
		rbfm->insertRecord(columnsFileHandle, columnsAttributes, columnsRecord, rid);
	}

	for (int var = 0; var < columnsAttributes.size(); ++var) {
		Attribute currAttr = columnsAttributes[var];
		createRecordInColumnsTable(columnsRecord, COLUMNS_TABLE_ID, currAttr.name, currAttr.type, currAttr.length, var+1, SYSTEM_TABLE);
		rbfm->insertRecord(columnsFileHandle, columnsAttributes, columnsRecord, rid);
	}

	for(int var = 0; var < indexAttributes.size(); var++){
		Attribute currAttr = indexAttributes[var];
		createRecordInColumnsTable(columnsRecord, INDEX_TABLE_ID, currAttr.name, currAttr.type, currAttr.length, var+1, SYSTEM_TABLE);
		rbfm->insertRecord(columnsFileHandle, columnsAttributes, columnsRecord, rid);
	}
	rbfm->closeFile(tablesFileHandle);
	rbfm->closeFile(columnsFileHandle);
	free(columnsRecord);

    return 0;
}

RC RelationManager::deleteCatalog()
{
	unsigned retVal = rbfm->destroyFile(TABLES_FILE);
	unsigned retVal2 = rbfm->destroyFile(COLUMNS_TABLE_NAME);
	unsigned retVal3 = rbfm->destroyFile(INDEX_TABLE_FILENAME);
	if(retVal < 0){
		return retVal;
	}

	if(retVal2 < 0){
		return retVal2;
	}

	if(retVal3 < 0){
		return retVal3;
	}
    return 0;
}

RC RelationManager::getNextValidIndexID(int &nextValidIndexID){
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="index-id";
	CompOp compOp=NO_OP;
	vector<string> attributeNames;
	attributeNames.push_back("index-id");
	void* value=malloc(PAGE_SIZE);
	FileHandle indexFileHandle;
	RC retval=rbfm->openFile(INDEX_TABLE_FILENAME, indexFileHandle);
	rbfm->scan(indexFileHandle, indexAttributes, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	int maxIndexIdFound = INT_MIN;
	void* retData = malloc(PAGE_SIZE);
	RID rid;
	while(rbfmIterator.getNextRecord(rid,retData) != RBFM_EOF){
		int id;
		memcpy(&id,(char*)retData+1,sizeof(int));
		if(id > maxIndexIdFound){
			maxIndexIdFound=id;
		}
	}
	free(value);
	free(retData);
	rbfm->closeFile(indexFileHandle);
	if(maxIndexIdFound == INT_MIN){
		maxIndexIdFound = 0;
	}
	nextValidIndexID = maxIndexIdFound + 1;
	return 0;
}

RC RelationManager::getNextValidTableID(int &nextValidTableID){
	/*
	 * This function iterates over the tables table and finds the maximum table id allocated and returns the next highest value which can be used
	 * as a table id for a new table
	 */
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="table-id";
	CompOp compOp=NO_OP;
	vector<string> attributeNames;
	attributeNames.push_back("table-id");
	void *value=malloc(PAGE_SIZE);
	FileHandle tablesFileHandle;
	RC retval=rbfm->openFile(TABLES_FILE, tablesFileHandle);
	rbfm->scan(tablesFileHandle,tablesAttributes,conditionAttribute,compOp,value,attributeNames,rbfmIterator);
	int maxTableIDFound=INT_MIN;
	void *returnedData=malloc(PAGE_SIZE);
	RID rid;
	while(rbfmIterator.getNextRecord(rid,returnedData)!=RBFM_EOF){
		int id;
		memcpy(&id,(char*)returnedData+1,sizeof(int));
		if(id > maxTableIDFound){
			maxTableIDFound=id;
		}
	}
	free(value);
	free(returnedData);
	rbfm->closeFile(tablesFileHandle);
	nextValidTableID=maxTableIDFound+1;
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	int nextValidTableID;
	RC ret=getNextValidTableID(nextValidTableID);
	if(ret<0){
		return -1;
	}
	rbfm->createFile(tableName);
	void* data = malloc(PAGE_SIZE);
	unsigned tableId = nextValidTableID;

	FileHandle tableFileHandle;
	rbfm->openFile(TABLES_FILE, tableFileHandle);
	createRecordInTablesTable(data, tableId, tableName, tableName, USER_TABLE);
	RID rid;
	rbfm->insertRecord(tableFileHandle, tablesAttributes, data, rid); //insert the tables details in tables table

	FileHandle columnsFileHandle;
	rbfm->openFile(COLUMNS_FILE, columnsFileHandle);
	for (int var = 0; var < attrs.size(); ++var) { // insert the tables attribute details in columns table
		Attribute currAttr = attrs[var];
		createRecordInColumnsTable(data, tableId, currAttr.name, currAttr.type, currAttr.length, var+1, USER_TABLE);
		rbfm->insertRecord(columnsFileHandle, columnsAttributes, data, rid);
	}
	rbfm->closeFile(tableFileHandle);
	rbfm->closeFile(columnsFileHandle);
	free(data);
    return 0;
}

RC RelationManager::getIDOfTable(const string &tableName, int &tabID, RID &tabRID, bool &systemTable, char *tableFileName){
	/*
	 * Returns the id of the table with table name tableName. If there is no such table returns -1.
	 */
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="table-name";
	CompOp compOp=EQ_OP;
	void *value=malloc(PAGE_SIZE);
	int tableNameLength=tableName.length();
	char *str=(char*)malloc(PAGE_SIZE);
	for(int i=0;i<tableNameLength;i++){
		str[i]=tableName[i];
	}
	tabID=-1; //if no table exists
	str[tableNameLength]='\0';
	memcpy((char*)value,&tableNameLength,sizeof(int));
	memcpy((char*)value+sizeof(int),str,tableNameLength);
	vector<string>attributeNames;
	attributeNames.push_back("table-id");
	attributeNames.push_back("system-table");
	attributeNames.push_back("file-name");
	FileHandle tablesFileHandle;
	RC retval=rbfm->openFile(TABLES_FILE, tablesFileHandle);
	rbfm->scan(tablesFileHandle,tablesAttributes,conditionAttribute,compOp,value,attributeNames,rbfmIterator);
	RID rid;
	void *returnedData=malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(rid,returnedData)!=RBFM_EOF){
		int retID;
		memcpy(&retID,(char*)returnedData+1,sizeof(int));
		int sysTab;
		memcpy(&sysTab,(char*)returnedData+1+sizeof(int),sizeof(int));
		if(sysTab==1) systemTable=true;
		else systemTable=false;
		int lenOfFileName;
		memcpy(&lenOfFileName,(char*)returnedData+1+(2*sizeof(int)),sizeof(int));
		memcpy(tableFileName,(char*)returnedData+1+(3*sizeof(int)),lenOfFileName);
		tableFileName[lenOfFileName]='\0';
		tabID=retID;
		tabRID.pageNum=rid.pageNum;
		tabRID.slotNum=rid.slotNum;
		break;
	}
	rbfm->closeFile(tablesFileHandle);
	free(returnedData);
	free(value);
	free(str);
	return 0;
}

void RelationManager::deleteIndexEntriesForTable(int tableId){

	vector<IndexDataSchema> indexDetails;
	findIndexesForTable(tableId, indexDetails);
	RID toDeleteRID;
	for(int i=0; i < indexDetails.size(); i++){
		toDeleteRID = indexDetails[i].first.second;
		FileHandle indexFH;
		rbfm->openFile(INDEX_TABLE_FILENAME, indexFH);
		rbfm->deleteRecord(indexFH, indexAttributes, toDeleteRID);
		rbfm->closeFile(indexFH);
		indexManager->destroyFile(indexDetails[i].second.second);
	}
}

void RelationManager::findIndexesForTable(int &tabId, vector<IndexDataSchema> &indexDetails){
	//populates indexDetails with < <index-id, rid>, <attributename, filename> > that match with the tabId
	//indexDetails schema - <<index-id, rid>, <attributename, filename> >
	FileHandle indexFileHandle;
	rbfm->openFile(INDEX_TABLE_FILENAME, indexFileHandle);
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute = "table-id";
	CompOp compOp = EQ_OP;
	void* value = malloc(PAGE_SIZE);
	int tableId = tabId;
	memcpy((char*) value, &tableId, sizeof(int));
	vector<string> attributeNames;
	attributeNames.push_back("index-id");
	attributeNames.push_back("attribute-name");
	attributeNames.push_back("filename");
	int nullBitVectorSize=ceil((double)attributeNames.size()/CHAR_BIT);
	rbfm->scan(indexFileHandle, indexAttributes, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	RID rid;
	void* returnedData = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(rid, returnedData) != RBFM_EOF){//assumption - returned data has all fields and no null
		int offset = nullBitVectorSize;
		int idxId;
		memcpy(&idxId, (char*) returnedData + offset, sizeof(int));
		offset += sizeof(int);
		char* attrName = (char*) malloc(PAGE_SIZE);
		int len;
		memcpy(&len, (char*) returnedData + offset, sizeof(int));
		offset += sizeof(int);
		memcpy(attrName, (char*) returnedData + offset, len);
		offset += len;
		attrName[len] = '\0';
		string strAttrName(attrName);
		char* fileName = (char*) malloc(PAGE_SIZE);
		memcpy(&len, (char*) returnedData + offset, sizeof(int));
		offset += sizeof(int);
		memcpy(fileName, (char*) returnedData + offset, len);
		offset += len;
		fileName[len] = '\0';
		string strFileName(fileName);
		indexDetails.push_back(make_pair(make_pair(idxId, rid), make_pair(strAttrName, strFileName)));
		free(attrName);
		free(fileName);
	}
	rbfmIterator.close();//needed?
	free(value);
	free(returnedData);
}

RC RelationManager::deleteTable(const string &tableName)
{
	/*
	 * Deletes the table entry in tables table and also the table's attribute details from the columns table.
	 * The file of that table is also deleted.
	 */
	//now delete the entries in tables table and get the id of the table.
	int tabID;
	RID tabRID;
	bool systemTable;
	char *tableFileName=(char*)malloc(60); //as file-name can be at most 50 characters
	getIDOfTable(tableName,tabID,tabRID,systemTable,tableFileName);
	if(tabID<0 || systemTable){//no such table exists or its a systemTable
		return -1;
	}
	FileHandle tablesFileHandle;
	rbfm->openFile(TABLES_FILE,tablesFileHandle);
	rbfm->deleteRecord(tablesFileHandle,tablesAttributes,tabRID);
	fflush(tablesFileHandle.fp);

	//now delete the entries in the columns file
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute="table-id";
	CompOp compOp=EQ_OP;
	void *value=malloc(PAGE_SIZE);
	memcpy((char*)value,&tabID,sizeof(int));
	vector<string>attributeNames;//leaving it empty because no need to project anything
	FileHandle columnsFileHandle;
	rbfm->openFile(COLUMNS_FILE,columnsFileHandle);
	rbfm->scan(columnsFileHandle,columnsAttributes,conditionAttribute,compOp,value,attributeNames,rbfmIterator);
	RID rid;
	void *returnedData=malloc(PAGE_SIZE);
	vector<RID> toDelete; //storing rid's to be deleted because if we delete inside while loop we may read/write same page causing inconsistency
	while(rbfmIterator.getNextRecord(rid,returnedData)!=RBFM_EOF){
		toDelete.push_back(rid);
	}
	RID toDeleteRID;
	for(int i=0;i<toDelete.size();i++){
		toDeleteRID=toDelete[i];
		FileHandle columnsFH;
		rbfm->openFile(COLUMNS_FILE,columnsFH);
		rbfm->deleteRecord(columnsFH,columnsAttributes,toDeleteRID);
		rbfm->closeFile(columnsFH);
	}
	rbfm->closeFile(tablesFileHandle);
	rbfm->closeFile(columnsFileHandle);
	rbfm->destroyFile(tableFileName);
	free(tableFileName);
	free(value);
	free(returnedData);

	deleteIndexEntriesForTable(tabID);

	return 0;
}

RC RelationManager::insertTupleIntoIndexIfExists(const string &tableName, int &tabId,
		const void* data, RID &rid, vector<Attribute> &attributes){
	vector<IndexDataSchema> indexDetails; //<<index-id, rid>, <attributename, filename> >
	findIndexesForTable(tabId, indexDetails);
	int nullBitVectorSize=ceil((double)attributes.size()/CHAR_BIT);
	void* key = malloc(PAGE_SIZE);
	for(int i=0; i < indexDetails.size(); i++){
		string attr = indexDetails[i].second.first;
		string ixFileName = indexDetails[i].second.second;
		int offset = nullBitVectorSize;
		for(int j=0; j < attributes.size(); j++){
			if(attributes[j].name == attr){
				if( ((char*)data)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					break;
				}
				if(attributes[j].type == TypeInt){
					int k;
					memcpy(&k, (char*) data + offset, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) key, &k, sizeof(int));
				}
				else if(attributes[j].type == TypeReal){
					float k;
					memcpy(&k, (char*) data + offset, sizeof(float));
					offset += sizeof(float);
					memcpy((char*) key, &k, sizeof(float));
				}
				else{
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int);
					char* str = (char*) malloc(PAGE_SIZE);
					memcpy(str, (char*) data + offset, len);
					offset += len;
					str[len] = '\0';
					memcpy((char*) key, &len, sizeof(int));
					memcpy((char*) key + sizeof(int), str, len);
					free(str);
				}
				IXFileHandle ixFileHandle;
				indexManager->openFile(ixFileName, ixFileHandle);
				indexManager->insertEntry(ixFileHandle, attributes[j], key, rid);
				indexManager->closeFile(ixFileHandle);
				break;
			}
			else{
				if( ((char*)data)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					continue;
				}
				if(attributes[j].type == TypeInt){
					offset += sizeof(int);
				}
				else if(attributes[j].type == TypeReal){
					offset += sizeof(float);
				}
				else{
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				}
			}
		}
	}
	free(key);
}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	int tabID;
	RID tabRID;
	bool systemTable;
	char *tableFileName=(char*)malloc(60); //as file-name can be at most 50 characters
	getIDOfTable(tableName,tabID,tabRID,systemTable,tableFileName);
	if(tabID<0 || systemTable){
		return -1;
	}
	FileHandle fileHandle;
	int retVal = rbfm->openFile(tableFileName, fileHandle);
	if(retVal < 0){
		free(tableFileName);
		return retVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	RC rc=rbfm->insertRecord(fileHandle, attributes, data, rid);
	if(rc<0){
		free(tableFileName);
		return -1;
	}
	rc=rbfm->closeFile(fileHandle);
	free(tableFileName);
	if(rc<0){
		return -1;
	}
	//check if there is an index for this table and insert it into that too.
	RID rid2;
	rid2.pageNum = rid.pageNum;
	rid2.slotNum = rid.slotNum;
	insertTupleIntoIndexIfExists(tableName, tabID, data, rid2, attributes);

	return 0;
}

void RelationManager::deleteTupleFromIndexIfExists(int &tableId, vector<Attribute> &attributes, const void* recordData, RID &rid){
	vector<IndexDataSchema> indexDetails; //<<index-id, rid>, <attributename, filename> >
	findIndexesForTable(tableId, indexDetails);
	int nullBitVectorSize=ceil((double)attributes.size()/CHAR_BIT);
	void* key = malloc(PAGE_SIZE);
	for(int i=0; i < indexDetails.size(); i++){
		string attr = indexDetails[i].second.first;
		string ixFileName = indexDetails[i].second.second;
		int offset = nullBitVectorSize;
		for(int j=0; j < attributes.size(); j++){
			if(attributes[j].name == attr){
				if( ((char*)recordData)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					break;
				}
				if(attributes[j].type == TypeInt){
					int k;
					memcpy(&k, (char*) recordData + offset, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) key, &k, sizeof(int));
				}
				else if(attributes[j].type == TypeReal){
					float k;
					memcpy(&k, (char*) recordData + offset, sizeof(float));
					offset += sizeof(float);
					memcpy((char*) key, &k, sizeof(float));
				}
				else{
					int len;
					memcpy(&len, (char*) recordData + offset, sizeof(int));
					offset += sizeof(int);
					char* str = (char*) malloc(PAGE_SIZE);
					memcpy(str, (char*) recordData + offset, len);
					offset += len;
					str[len] = '\0';
					memcpy((char*) key, &len, sizeof(int));
					memcpy((char*) key + sizeof(int), str, len);
					free(str);
				}
				IXFileHandle ixFileHandle;
				indexManager->openFile(ixFileName, ixFileHandle);
				RC ret = indexManager->deleteEntry(ixFileHandle, attributes[j], key, rid);
				indexManager->closeFile(ixFileHandle);
				break;
			}
			else{
				if( ((char*)recordData)[j/8] & (1 << (7-(j%8))) ){// attribute is null
					continue;
				}
				if(attributes[j].type == TypeInt){
					offset += sizeof(int);
				}
				else if(attributes[j].type == TypeReal){
					offset += sizeof(float);
				}
				else{
					int len;
					memcpy(&len, (char*) recordData + offset, sizeof(int));
					offset += sizeof(int) + len;
				}
			}
		}
	}
	free(key);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	int tabID;
	RID tabRID;
	bool systemTable;
	char *tableFileName=(char*)malloc(60); //as file-name can be at most 50 characters
	getIDOfTable(tableName,tabID,tabRID,systemTable,tableFileName);
	if(tabID < 0 || systemTable){
		return -1;
	}
	FileHandle fileHandle;
	int retVal = rbfm->openFile(tableFileName, fileHandle);
	if(retVal < 0){
		return retVal;
	}
	void* recordData = malloc(PAGE_SIZE);
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	rbfm->readRecord(fileHandle, attributes, rid, recordData);
	RC ret=rbfm->deleteRecord(fileHandle, attributes, rid); //have to close file here remember
	rbfm->closeFile(fileHandle);
	free(tableFileName);

	RID rid2;
	rid2.pageNum = rid.pageNum;
	rid2.slotNum = rid.slotNum;
	deleteTupleFromIndexIfExists(tabID, attributes, recordData, rid2);

	free(recordData);
	return ret;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	int tabID;
	RID tabRID;
	bool systemTable;
	char *tableFileName=(char*)malloc(60); //as file-name can be at most 50 characters
	getIDOfTable(tableName,tabID,tabRID,systemTable,tableFileName);
	FileHandle fileHandle;
	int retVal = rbfm->openFile(tableFileName, fileHandle);
	if(retVal < 0){
		return retVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	void* recordData = malloc(PAGE_SIZE);
	rbfm->readRecord(fileHandle, attributes, rid, recordData);
	RC ret=rbfm->updateRecord(fileHandle, attributes, data, rid);
	rbfm->closeFile(fileHandle);

	RID rid2;
	rid2.pageNum = rid.pageNum;
	rid2.slotNum = rid.slotNum;
	deleteTupleFromIndexIfExists(tabID, attributes, recordData, rid2);
	insertTupleIntoIndexIfExists(tableFileName, tabID, data, rid2, attributes);

	free(tableFileName);
	free(recordData);
	return ret;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	int tabID;
	RID tabRID;
	bool systemTable;
	char *tableFileName=(char*)malloc(60); //as file-name can be at most 50 characters
	getIDOfTable(tableName,tabID,tabRID,systemTable,tableFileName);
	FileHandle fileHandle;
	int retVal = rbfm->openFile(tableFileName, fileHandle);
	if(retVal < 0){
		return retVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	RC ret=rbfm->readRecord(fileHandle, attributes, rid, data);
	rbfm->closeFile(fileHandle);
	free(tableFileName);
	return ret;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	int tabID;
	RID tabRID;
	bool systemTable;
	char *tableFileName=(char*)malloc(60); //as file-name can be at most 50 characters
	getIDOfTable(tableName,tabID,tabRID,systemTable,tableFileName);
	FileHandle fileHandle;
	int retVal = rbfm->openFile(tableFileName, fileHandle);
	if(retVal < 0){
		free(tableFileName);
		return retVal;
	}
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	RC ret=rbfm->readAttribute(fileHandle, attributes, rid, attributeName, data);
	rbfm->closeFile(fileHandle);
	free(tableFileName);
	return ret;

}
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	int tabID;
	RID tabRID;
	bool systemTable;
	char *tableFileName=(char*)malloc(60); //as file-name can be at most 50 characters
	getIDOfTable(tableName,tabID,tabRID,systemTable,tableFileName);
	vector<Attribute>recordDescriptor;
	getAttributes(tableName,recordDescriptor);
	FileHandle fileHandle;
	rbfm->openFile(tableName,fileHandle);
	RC ret=rbfm->scan(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfmScanIterator);
	free(tableFileName);
	return ret;
    //return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	if(rbfmScanIterator.getNextRecord(rid,data)!=RBFM_EOF){
		return 0;
	}
	else{
		 return RM_EOF;

	}
}

RC RM_ScanIterator::close(){
	rbfmScanIterator.close();
	return 0;
}

RC RelationManager::populateColumnsAttributes(){

	Attribute tableIdAttr;
	tableIdAttr.length = 4;
	tableIdAttr.name = "table-id";
	tableIdAttr.type = TypeInt;
	columnsAttributes.push_back(tableIdAttr);

	Attribute columnNameAttr;
	columnNameAttr.length = 50;
	columnNameAttr.name = "column-name";
	columnNameAttr.type = TypeVarChar;
	columnsAttributes.push_back(columnNameAttr);

	Attribute columnTypeAttr;
	columnTypeAttr.length = 4;
	columnTypeAttr.name = "column-type";
	columnTypeAttr.type = TypeInt;
	columnsAttributes.push_back(columnTypeAttr);

	Attribute columnLengthAttr;
	columnLengthAttr.length = 4;
	columnLengthAttr.name = "column-length";
	columnLengthAttr.type = TypeInt;
	columnsAttributes.push_back(columnLengthAttr);

	Attribute columnPositionAttr;
	columnPositionAttr.length = 4;
	columnPositionAttr.name = "column-position";
	columnPositionAttr.type = TypeInt;
	columnsAttributes.push_back(columnPositionAttr);

	Attribute systemTableAttr;
	systemTableAttr.length = 4;
	systemTableAttr.name = "system-table";
	systemTableAttr.type = TypeInt;
	columnsAttributes.push_back(systemTableAttr);

	return 0;
}

RC RelationManager::populateTablesAttributes(){
	Attribute tableIdAttr;
	tableIdAttr.length = 4;
	tableIdAttr.name = "table-id";
	tableIdAttr.type = TypeInt;
	tablesAttributes.push_back(tableIdAttr);

	Attribute tableNameAttr;
	tableNameAttr.length = 50;
	tableNameAttr.name = "table-name";
	tableNameAttr.type = TypeVarChar;
	tablesAttributes.push_back(tableNameAttr);

	Attribute fileNameAttr;
	fileNameAttr.length = 50;
	fileNameAttr.name = "file-name";
	fileNameAttr.type = TypeVarChar;
	tablesAttributes.push_back(fileNameAttr);

	Attribute systemTableAttr;
	systemTableAttr.length = 4;
	systemTableAttr.name = "system-table";
	systemTableAttr.type = TypeInt;
	tablesAttributes.push_back(systemTableAttr);

	return 0;
}

RC RelationManager::populateIndexAttributes(){
	Attribute indexIdAttr;
	indexIdAttr.length = 4;
	indexIdAttr.name = "index-id";
	indexIdAttr.type = TypeInt;
	indexAttributes.push_back(indexIdAttr);

	Attribute tableIdAttr;
	tableIdAttr.length = 4;
	tableIdAttr.name = "table-id";
	tableIdAttr.type = TypeInt;
	indexAttributes.push_back(tableIdAttr);

	Attribute tableNameAttr;
	tableNameAttr.length = 50;
	tableNameAttr.name = "table-name";
	tableNameAttr.type = TypeVarChar;
	indexAttributes.push_back(tableNameAttr);

	Attribute attributeName;
	attributeName.length = 50;
	attributeName.name = "attribute-name";
	attributeName.type = TypeVarChar;
	indexAttributes.push_back(attributeName);

	Attribute attributeType;
	attributeType.length = 4;
	attributeType.name = "attribute-type";
	attributeType.type = TypeInt;
	indexAttributes.push_back(attributeType);

	Attribute fileName;
	fileName.length = 50;
	fileName.name = "filename";
	fileName.type = TypeVarChar;
	indexAttributes.push_back(fileName);


}

RC RelationManager::createRecordInTablesTable(const void*data, int tableId, string tableName, string fileName, int systemTable){
	int nullBitVectorSize = ceil((double)tablesAttributes.size()/8);
	char *nullsIndicator = (char *) malloc(nullBitVectorSize);
	memset(nullsIndicator, 0, nullBitVectorSize);

	memcpy((char*)data, nullsIndicator, nullBitVectorSize);

	int offset = nullBitVectorSize;
	memcpy((char*)data + offset, &tableId, sizeof(int));
	offset += sizeof(int);

	int tableNameLength = tableName.length();
	memcpy((char*)data + offset, &tableNameLength, sizeof(int));
	offset += sizeof(int);

	char *str=(char*)malloc(PAGE_SIZE);
	for(int i=0;i<tableNameLength;i++){
		str[i]=tableName[i];
	}
	str[tableNameLength]='\0';
	memcpy((char*)data + offset, str, tableNameLength);
	offset += tableNameLength;

	int fileNameLength = fileName.length();
	memcpy((char*)data + offset, &fileNameLength, sizeof(int));
	offset += sizeof(int);

	for(int i=0;i<fileNameLength;i++){
		str[i]=fileName[i];
	}
	str[fileNameLength]='\0';

	memcpy((char*)data + offset, str, fileNameLength);
	offset += fileNameLength;

	free(str);

	memcpy((char*)data + offset, &systemTable, sizeof(int));
	free(nullsIndicator);
	return 0;

}

RC RelationManager::createRecordInColumnsTable(const void*data, int tableId, string columnName,
		AttrType columnType, int columnLength, int columnPosition, int systemTable){
	int nullBitVectorSize = ceil((double)columnsAttributes.size()/8);
	char *nullsIndicator = (char *) malloc(nullBitVectorSize);
	memset(nullsIndicator, 0, nullBitVectorSize);

	memcpy((char*)data, nullsIndicator, nullBitVectorSize);

	int offset = nullBitVectorSize;
	memcpy((char*)data + offset, &tableId, sizeof(int));
	offset += sizeof(int);

	int columnNameLength = columnName.length();
	memcpy((char*)data + offset, &columnNameLength, sizeof(int));
	offset += sizeof(int);

	char *str=(char*)malloc(PAGE_SIZE);
	for(int i=0;i<columnNameLength;i++){
		str[i]=columnName[i];
	}
	str[columnNameLength]='\0';

	memcpy((char*)data + offset, str, columnNameLength);
	offset += columnNameLength;

	memcpy((char*)data + offset, &columnType, sizeof(int));
	offset += sizeof(int);

	memcpy((char*)data + offset, &columnLength, sizeof(int));
	offset += sizeof(int);

	memcpy((char*)data + offset, &columnPosition, sizeof(int));
	offset += sizeof(int);

	memcpy((char*)data + offset, &systemTable, sizeof(int));
	offset += sizeof(int);
	free(str);
	free(nullsIndicator);
	return 0;

}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	int tableID;
	RID tableRID;
	bool sysTab;
	char *tableFileName=(char*)malloc(60);
	getIDOfTable(tableName, tableID, tableRID, sysTab, tableFileName);
	FileHandle columnsFileHandle;
	rbfm->openFile(COLUMNS_FILE, columnsFileHandle);
	RBFM_ScanIterator scanIterator;
	void* tableIdData = malloc(sizeof(int)+1);
	memcpy((char*)tableIdData, &tableID, sizeof(int));
	char *conditionAttribute="table-id";
	CompOp compOp=EQ_OP;
	vector<string> attributeNames;
	for (int var = 0; var < columnsAttributes.size(); ++var) {
		attributeNames.push_back(columnsAttributes[var].name);
	}
	rbfm->scan(columnsFileHandle, columnsAttributes, conditionAttribute, compOp, tableIdData, attributeNames, scanIterator);

	RID rid;
	void *data=malloc(PAGE_SIZE);
	int count = 0;
	while(scanIterator.getNextRecord(rid, data) != RBFM_EOF){
		unsigned nullBitVectorSize = ceil((double)columnsAttributes.size()/8);
		int offset = nullBitVectorSize;
		offset += sizeof(int); //Skip the tableId attribute

		int columnNameLength;
		memcpy(&columnNameLength, (char*)data+offset, sizeof(int));
		offset += sizeof(int);

		char* columnName = (char*)malloc(columnNameLength+1);
		memcpy(columnName, (char*)data+offset, columnNameLength);
		columnName[columnNameLength]='\0';
		offset += columnNameLength;

		AttrType columnType;
		memcpy(&columnType, (char*)data+offset, sizeof(int));
		offset += sizeof(4);

		int columnLength;
		memcpy(&columnLength, (char*)data+offset, sizeof(int));

		Attribute attr;
		attr.name = columnName;
		attr.length = columnLength;
		attr.type = columnType;
		attrs.push_back(attr);
		free(columnName);
	}
	rbfm->closeFile(columnsFileHandle);
	free(tableIdData);
	free(tableFileName);
	free(data);
    return 0;
}

void RelationManager::createRecordInIndexTable(void* data, int indexId, int tableId, const string &tableName,
		const string &attributeName, AttrType attributeType, const string &fileName){
	int nullBitVectorSize = ceil((double) indexAttributes.size() / CHAR_BIT);
	memset(data, 0, nullBitVectorSize);
	int offset = nullBitVectorSize;

	memcpy((char*) data + offset, &indexId, sizeof(int));
	offset += sizeof(int);

	memcpy((char*) data + offset, &tableId, sizeof(int));
	offset += sizeof(int);

	char* str = (char*) malloc(PAGE_SIZE);
	int len = tableName.size();
	for(int i = 0; i < len; i++){
		str[i] = tableName[i];
	}
	str[len] = '\0';
	memcpy((char*) data + offset, &len, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) data + offset, str, len);
	offset += len;

	len = attributeName.size();
	for(int i = 0; i < len; i++){
		str[i] = attributeName[i];
	}
	str[len] = '\0';
	memcpy((char*) data + offset, &len, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) data + offset, str, len);
	offset += len;

	memcpy((char*) data + offset, &attributeType, sizeof(int));
	offset += sizeof(int);

	len = fileName.size();
	for(int i = 0; i < len; i++){
		str[i] = fileName[i];
	}
	str[len] = '\0';
	memcpy((char*) data + offset, &len, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) data + offset, str, len);
	offset += len;

	free(str);
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName){
	int tableId;
	vector<Attribute> attributes;
	RID rid;
	bool systemTable;
	char* tableFileName = (char*) malloc(PAGE_SIZE);
	getIDOfTable(tableName, tableId, rid, systemTable, tableFileName);
	getAttributes(tableName, attributes);
	AttrType attrType;
	Attribute keyAttr;
	for(int i=0; i < attributes.size(); i++){
		if(attributes[i].name == attributeName){
			attrType = attributes[i].type;
			keyAttr.length = attributes[i].length;
			keyAttr.name = attributes[i].name;
			keyAttr.type = attributes[i].type;
			break;
		}
	}
	int nextValidIndexID;
	getNextValidIndexID(nextValidIndexID);
	void* indexTableRecord = malloc(PAGE_SIZE);
	string fileName = tableName + "_" + attributeName + ".idx";
	createRecordInIndexTable(indexTableRecord, nextValidIndexID, tableId, tableName, attributeName, attrType, fileName);
	FileHandle indexFileHandle;
	rbfm->openFile(INDEX_TABLE_FILENAME, indexFileHandle);
	rbfm->insertRecord(indexFileHandle, indexAttributes, indexTableRecord, rid);
	rbfm->closeFile(indexFileHandle);
	free(indexTableRecord);

	//now insert the entries of the table into the index
	indexManager->createFile(fileName);
	IXFileHandle ixFileHandle;
	indexManager->openFile(fileName, ixFileHandle);

	FileHandle tableFileHandle;
	rbfm->openFile(tableFileName, tableFileHandle);
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute = attributes[0].name;
	CompOp compOp = NO_OP;
	void* value = malloc(PAGE_SIZE);
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);
	rbfm->scan(tableFileHandle, attributes, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	RID retRid;
	void* returnedData = malloc(PAGE_SIZE);
	void* key = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(retRid, returnedData) != RBFM_EOF){
		if(keyAttr.type == TypeInt){
			memcpy((char*) key, (char*) returnedData + 1, sizeof(int));
		}
		else if(attrType == TypeReal){
			memcpy((char*) key, (char*) returnedData + 1, sizeof(float));
		}
		else{
			int len;
			memcpy(&len, (char*) returnedData + 1, sizeof(int));
			memcpy((char*) key, &len, sizeof(int));
			memcpy((char*) key + sizeof(int), (char*) returnedData + 1 + sizeof(int), len);
		}
		indexManager->insertEntry(ixFileHandle, keyAttr, key, retRid);
	}
	indexManager->closeFile(ixFileHandle);
	rbfm->closeFile(tableFileHandle);
	free(value);
	free(returnedData);
	free(key);
	free(tableFileName);
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
	int tableId;
	RID tableRid;
	bool systemTable;
	char* tableFileName = (char*) malloc(PAGE_SIZE);
	getIDOfTable(tableName, tableId, tableRid, systemTable, tableFileName);
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	FileHandle indexFileHandle;
	rbfm->openFile(INDEX_TABLE_FILENAME, indexFileHandle);
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute = "table-id";
	CompOp compOp = EQ_OP;
	void* value = malloc(PAGE_SIZE);
	memcpy((char*) value, &tableId, sizeof(int));
	vector<string> attributeNames;
	attributeNames.push_back("attribute-name");
	attributeNames.push_back("filename");
	rbfm->scan(indexFileHandle, indexAttributes, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	RID retRid;
	void* returnedData = malloc(PAGE_SIZE);
	RID toDeleteRid;
	toDeleteRid.pageNum = -1;
	while(rbfmIterator.getNextRecord(retRid, returnedData) != RBFM_EOF){
		int len;
		memcpy(&len, (char*) returnedData + 1, sizeof(int));
		char* attrName = (char*) malloc(100);
		memcpy(attrName, (char*) returnedData + 1 + sizeof(int), len);
		attrName[len] = '\0';
		string ixAttrName(attrName);
		free(attrName);
		if(attributeName == ixAttrName){
			int offset = 1 + sizeof(int) + len;
			int slen;
			memcpy(&slen, (char*) returnedData + offset, sizeof(int));
			offset += sizeof(int);
			char* fname = (char*) malloc(100);
			memcpy(fname, (char*) returnedData + offset, slen);
			fname[slen] = '\0';
			string ixFileName(fname);
			indexManager->destroyFile(ixFileName);
			toDeleteRid.pageNum = retRid.pageNum;
			toDeleteRid.slotNum = retRid.slotNum;
			free(fname);
			break;
		}
	}
	rbfmIterator.close();
	RC ret;
	if(toDeleteRid.pageNum != -1){
		ret = rbfm->deleteRecord(indexFileHandle, indexAttributes, toDeleteRid);
	}
	else{
		ret = -1;
	}
	rbfm->closeFile(indexFileHandle);
	return ret;
}

RC RelationManager::indexScan(const string &tableName, const string &attributeName, const void* lowKey, const void* highKey,
		bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator){
	int tabId;
	RID rid;
	bool systemTable;
	char* tableFileName = (char*) malloc(100);
	getIDOfTable(tableName, tabId, rid, systemTable, tableFileName);
	vector<Attribute> attributes;
	getAttributes(tableName, attributes);
	Attribute attr;
	for(int i = 0; i < attributes.size(); i++){
		if(attributes[i].name == attributeName){
			attr.length = attributes[i].length;
			attr.name = attributes[i].name;
			attr.type = attributes[i].type;
		}
	}
	FileHandle indexFileHandle;
	rbfm->openFile(INDEX_TABLE_FILENAME, indexFileHandle);
	RBFM_ScanIterator rbfmIterator;
	string conditionAttribute = "table-id";
	CompOp compOp = EQ_OP;
	void* value = malloc(PAGE_SIZE);
	memcpy((char*) value, &tabId, sizeof(int));
	vector<string> attributeNames;
	attributeNames.push_back("attribute-name");
	attributeNames.push_back("filename");
	rbfm->scan(indexFileHandle, indexAttributes, conditionAttribute, compOp, value, attributeNames, rbfmIterator);
	RID retRid;
	void* returnedData = malloc(PAGE_SIZE);
	string indexFileName = "";
	while(rbfmIterator.getNextRecord(retRid, returnedData) != RBFM_EOF){
		int len;
		memcpy(&len, (char*) returnedData + 1, sizeof(int));
		char* attrName = (char*) malloc(100);
		memcpy(attrName, (char*) returnedData + 1 + sizeof(int), len);
		attrName[len] = '\0';
		string ixAttrName(attrName);
		free(attrName);
		if(attributeName == ixAttrName){
			int offset = 1 + sizeof(int) + len;
			int slen;
			memcpy(&slen, (char*) returnedData + offset, sizeof(int));
			offset += sizeof(int);
			char* fname = (char*) malloc(100);
			memcpy(fname, (char*) returnedData + offset, slen);
			fname[slen] = '\0';
			string ixFileName(fname);
			indexFileName = ixFileName;
			free(fname);
			break;
		}
	}
	IXFileHandle ixFileHandle;
	indexManager->openFile(indexFileName, ixFileHandle);
	indexManager->scan(ixFileHandle, attr, lowKey, highKey, lowKeyInclusive,
			highKeyInclusive, rm_IndexScanIterator.ixScanIterator);
	rbfm->closeFile(indexFileHandle);
//	indexManager->closeFile(ixFileHandle);
	free(tableFileName);
	free(value);
	free(returnedData);
	return 0;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void* key){
	  if(ixScanIterator.getNextEntry(rid,key) != IX_EOF){
		  return 0;
	  }
	  return RM_EOF;
}

RC RM_IndexScanIterator::close(){
	  ixScanIterator.close();
	  //here close the file
	  indexManager->closeFile(ixScanIterator.ixfileHandle);
	  return 0;
}
