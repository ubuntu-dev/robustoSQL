#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <cstdio>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#include <map>

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
		Iterator* input;
		Condition condition;
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition ) {   // Selection condition
        	this->input = input;
        	this->condition = condition;

        }
        ~Filter(){
        	//Should we free input here
//        	free(input);
        };

        bool isConditionSatisfied(void * data){
        	bool condition_satisfied = false;
			vector<Attribute> attrs;
			getAttributes(attrs);
			int numAttrs = attrs.size();
			int nullBitVectorSize = ceil((double)numAttrs / CHAR_BIT);
			int offset = nullBitVectorSize;
			if(!condition.bRhsIsAttr){
				for (int var = 0; var < numAttrs; ++var) {
					Attribute attr = attrs[var];

					bool isNull = ((char*)data)[var / 8] & ( 1 << ( 7 - ( var % 8 )));
					if(attr.name == condition.lhsAttr){
						if(condition.rhsValue.data == NULL && isNull){
							condition_satisfied = true;
						}else if(condition.rhsValue.type == TypeInt){
							int value;
							memcpy(&value, data + offset, sizeof(int));
							int conditionValue;
							memcpy(&conditionValue, condition.rhsValue.data, sizeof(int));
							if(condition.op == EQ_OP){
								condition_satisfied = value == conditionValue;
							}else if(condition.op == LT_OP){
								condition_satisfied = value < conditionValue;
							}else if(condition.op == LE_OP){
								condition_satisfied = value <= conditionValue;
							}else if(condition.op == GT_OP){
								condition_satisfied = value > conditionValue;
							}else if(condition.op == GE_OP){
								condition_satisfied = value >= conditionValue;
							}else if(condition.op == NE_OP){
								condition_satisfied = value != conditionValue;
							}else if(condition.op == NO_OP){
								condition_satisfied = true;
							}
						}else if(condition.rhsValue.type == TypeReal){
							float value;
							memcpy(&value, data + offset, sizeof(float));
							float conditionValue;
							memcpy(&conditionValue, condition.rhsValue.data, sizeof(float));
							if(condition.op == EQ_OP){
								condition_satisfied = value == conditionValue;
							}else if(condition.op == LT_OP){
								condition_satisfied = value < conditionValue;
							}else if(condition.op == LE_OP){
								condition_satisfied = value <= conditionValue;
							}else if(condition.op == GT_OP){
								condition_satisfied = value > conditionValue;
							}else if(condition.op == GE_OP){
								condition_satisfied = value >= conditionValue;
							}else if(condition.op == NE_OP){
								condition_satisfied = value != conditionValue;
							}else if(condition.op == NO_OP){
								condition_satisfied = true;
							}
						}else if(condition.rhsValue.type == TypeVarChar){
							int length;
							memcpy(&length, data + offset, sizeof(int));
							char * charValue = (char*)malloc(PAGE_SIZE);
							memcpy(charValue, data + offset + sizeof(int), length);
							charValue[length] = '\0';
							string value = string(charValue);

							int conditionLength;
							memcpy(&conditionLength, condition.rhsValue.data, sizeof(int));
							char* charConditionValue = (char*) malloc(PAGE_SIZE);
							memcpy(charConditionValue, (char*) condition.rhsValue.data + sizeof(int), conditionLength);
							charConditionValue[conditionLength] = '\0';
							string conditionValue = string(charConditionValue);

							free(charValue);
							free(charConditionValue);

							if(condition.op == EQ_OP){
								condition_satisfied = value == conditionValue;
							}else if(condition.op == LT_OP){
								condition_satisfied = value < conditionValue;
							}else if(condition.op == LE_OP){
								condition_satisfied = value <= conditionValue;
							}else if(condition.op == GT_OP){
								condition_satisfied = value > conditionValue;
							}else if(condition.op == GE_OP){
								condition_satisfied = value >= conditionValue;
							}else if(condition.op == NE_OP){
								condition_satisfied = value != conditionValue;
							}else if(condition.op == NO_OP){
								condition_satisfied = true;
							}
						}
						break;
					}else{
						if(isNull){
							//don't update offset
						}else if(attr.type == TypeInt || attr.type == TypeReal){
							offset += sizeof(int);
						}else if(attr.type == TypeVarChar){
							int length;
							memcpy(&length, data + offset, sizeof(int));
							offset += sizeof(int) + length;
						}
					}
				}
			}
			else{ //if rhs is attribute
				//first get LHS value
				void* lhsValue = NULL;
				void* rhsValue = NULL;
				AttrType lhsAttrType, rhsAttrType;
				int offset2 = nullBitVectorSize;
				for(int i = 0; i < attrs.size(); i++){
					if(attrs[i].name == condition.lhsAttr){
						lhsAttrType = attrs[i].type;
						if( ((char*)data)[i/8] & ( 1 << (7 - (i%8) ) ) ){
							lhsValue = NULL;
						}
						lhsValue = malloc(PAGE_SIZE);
						if(attrs[i].type == TypeInt){
							int val;
							memcpy(&val, (char*) data + offset2, sizeof(int));
							offset2 += sizeof(int);
							memcpy((char*) lhsValue, &val, sizeof(int));
						}
						else if(attrs[i].type == TypeReal){
							float val;
							memcpy(&val, (char*) data + offset2, sizeof(float));
							offset2 += sizeof(float);
							memcpy((char*) lhsValue, &val, sizeof(float));
						}
						else if(attrs[i].type == TypeVarChar){
							int len;
							memcpy(&len,(char*) data + offset2, sizeof(int));
							memcpy((char*) lhsValue, (char*) data + offset2, sizeof(int) + len);
							offset2 += sizeof(int) + len;
						}
						break;
					}
					else{
						if( ((char*)data)[i/8] & ( (1 << (7 - (i%8) ) ) ) ){
							continue;
						}
						else if(attrs[i].type == TypeInt){
							offset2 += sizeof(int);
						}
						else if(attrs[i].type == TypeReal){
							offset2 += sizeof(float);
						}
						else{
							int len;
							memcpy(&len, (char*) data + offset2, sizeof(int));
							offset2 += sizeof(int) + len;
						}
					}
				}
				int offset3 = nullBitVectorSize;
				for(int i = 0; i < attrs.size(); i++){
					if(attrs[i].name == condition.rhsAttr){
						rhsAttrType = attrs[i].type;
						if( ((char*)data)[i/8] & ( 1 << (7 - (i%8) ) ) ){
							rhsValue = NULL;
						}
						rhsValue = malloc(PAGE_SIZE);
						if(attrs[i].type == TypeInt){
							int val;
							memcpy(&val, (char*) data + offset3, sizeof(int));
							offset3 += sizeof(int);
							memcpy((char*) rhsValue, &val, sizeof(int));
						}
						else if(attrs[i].type == TypeReal){
							float val;
							memcpy(&val, (char*) data + offset3, sizeof(float));
							offset3 += sizeof(float);
							memcpy((char*) rhsValue, &val, sizeof(float));
						}
						else if(attrs[i].type == TypeVarChar){
							int len;
							memcpy(&len,(char*) data + offset3, sizeof(int));
							memcpy((char*) rhsValue, (char*) data + offset3, sizeof(int) + len);
							offset3 += sizeof(int) + len;
						}
						break;
					}
					else{
						if( ((char*)data)[i/8] & ( (1 << (7 - (i%8) ) ) ) ){
							continue;
						}
						else if(attrs[i].type == TypeInt){
							offset3 += sizeof(int);
						}
						else if(attrs[i].type == TypeReal){
							offset3 += sizeof(float);
						}
						else{
							int len;
							memcpy(&len, (char*) data + offset2, sizeof(int));
							offset3 += sizeof(int) + len;
						}
					}
				}
				if(lhsValue == NULL){
					condition_satisfied = false;
				}
				else if(lhsAttrType != rhsAttrType){
					condition_satisfied = true;
				}
				else if(lhsAttrType == TypeInt){
					int lval,rval;
					memcpy(&lval, (char*) lhsValue, sizeof(int));
					memcpy(&rval, (char*) rhsValue, sizeof(int));
					if(condition.op == EQ_OP){
						condition_satisfied = (lval == rval);
					}
					else if(condition.op == LT_OP){
						condition_satisfied = (lval < rval);
					}
					else if(condition.op == LE_OP){
						condition_satisfied = (lval <= rval);
					}
					else if(condition.op == GT_OP){
						condition_satisfied = (lval > rval);
					}
					else if(condition.op == GE_OP){
						condition_satisfied = (lval >= rval);
					}
					else if(condition.op == NE_OP){
						condition_satisfied = (lval != rval);
					}
					else if(condition.op == NO_OP){
						condition_satisfied = true;
					}
				}
				else if(lhsAttrType == TypeReal){
					float lval,rval;
					memcpy(&lval, (char*) lhsValue, sizeof(float));
					memcpy(&rval, (char*) rhsValue, sizeof(float));
					if(condition.op == EQ_OP){
						condition_satisfied = (lval == rval);
					}
					else if(condition.op == LT_OP){
						condition_satisfied = (lval < rval);
					}
					else if(condition.op == LE_OP){
						condition_satisfied = (lval <= rval);
					}
					else if(condition.op == GT_OP){
						condition_satisfied = (lval > rval);
					}
					else if(condition.op == GE_OP){
						condition_satisfied = (lval >= rval);
					}
					else if(condition.op == NE_OP){
						condition_satisfied = (lval != rval);
					}
					else if(condition.op == NO_OP){
						condition_satisfied = true;
					}
				}
				else if(lhsAttrType == TypeVarChar){
					char* lstr = (char*) malloc(PAGE_SIZE);
					char* rstr = (char*) malloc(PAGE_SIZE);
					int lLen, rLen;
					memcpy(&lLen, (char*) lhsValue, sizeof(int));
					memcpy(&rLen, (char*) rhsValue, sizeof(int));
					memcpy(lstr, (char*) lhsValue + sizeof(int), lLen);
					memcpy(rstr, (char*) rhsValue + sizeof(int), rLen);
					lstr[lLen] = '\0';
					rstr[rLen] = '\0';
					string lval(lstr);
					string rval(rstr);
					free(lstr);
					free(rstr);
					if(condition.op == EQ_OP){
						condition_satisfied = (lval == rval);
					}
					else if(condition.op == LT_OP){
						condition_satisfied = (lval < rval);
					}
					else if(condition.op == LE_OP){
						condition_satisfied = (lval <= rval);
					}
					else if(condition.op == GT_OP){
						condition_satisfied = (lval > rval);
					}
					else if(condition.op == GE_OP){
						condition_satisfied = (lval >= rval);
					}
					else if(condition.op == NE_OP){
						condition_satisfied = (lval != rval);
					}
					else if(condition.op == NO_OP){
						condition_satisfied = true;
					}
				}
			}
			return condition_satisfied;
        }

        RC getNextTuple(void *data) {
        	if(condition.lhsAttr.size() == 0){
        		return QE_EOF;
        	}
        	bool condition_satisfied = false;
        	do{
        		RC retVal = input->getNextTuple(data);
				if(retVal == RM_EOF){
					return QE_EOF;
				}
				condition_satisfied = isConditionSatisfied(data);
        	}while(condition_satisfied == false);
        	return 0;
        }
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        	input->getAttributes(attrs);
        };
};


class Project : public Iterator {
    // Projection operator
    public:
		Iterator* input;
		vector<string> attrNames;
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames){
        	this->input = input;
        	this->attrNames = attrNames;
        };   // vector containing attribute names
        ~Project(){
        	//free(input);
        };

        RC getNextTuple(void *data) {
        	void* inputData = malloc(PAGE_SIZE);
        	RC retVal = input->getNextTuple(inputData);
        	if(retVal == QE_EOF){
        		free(inputData);
        		return QE_EOF;
        	}
        	int outputDataNullBitVectorSize = ceil((double)attrNames.size() / CHAR_BIT);
        	int output_offset = outputDataNullBitVectorSize;
        	char* outputDataNullBitVector = (char*)malloc(outputDataNullBitVectorSize);
        	memset(outputDataNullBitVector,0,outputDataNullBitVectorSize); //init null bit vector to all 0

        	vector<Attribute> attrs;
        	input->getAttributes(attrs);
        	int numAttrs = attrs.size();

        	int inputDataNullBitVectorSize = ceil((double)numAttrs / CHAR_BIT);


        	for (int var = 0; var < attrNames.size(); ++var) {
        		string attrName = attrNames[var];
        		//initialize input_offset at the start of the loop
        		int input_offset = inputDataNullBitVectorSize;
        		for (int var2 = 0; var2 < numAttrs; ++var2) {
        			Attribute attr = attrs[var2];
					bool isNull = ((char*)inputData)[var2 / 8] & ( 1 << ( 7 - ( var2 % 8 )));
					if(attr.name == attrName){
						if(isNull){
							outputDataNullBitVector[var2 / 8] = outputDataNullBitVector[var2 / 8] | (1 << (7 - (var2 % 8)));
							//don't update input_offset and output_offset
						}else{
							if(attr.type == TypeInt || attr.type == TypeReal){
								memcpy(data + output_offset, inputData + input_offset, sizeof(int));
								output_offset += sizeof(int);
								input_offset += sizeof(int);
							}else if(attr.type == TypeVarChar){
								int length;
								memcpy(&length, inputData + input_offset, sizeof(int));
								memcpy(data + output_offset, inputData + input_offset, sizeof(int) + length);
								output_offset += sizeof(int) + length;
								input_offset += sizeof(int) + length;
							}
						}
						break;
					}else{
						//Update input offset
						if(isNull){
							//Don't update
						}else{
							if(attr.type == TypeInt || attr.type == TypeReal){
								input_offset += sizeof(int);
							}else if(attr.type == TypeVarChar){
								int length;
								memcpy(&length, inputData + input_offset, sizeof(int));
								input_offset += sizeof(int) + length;
							}
						}
					}
				}
			}
        	memcpy(data, outputDataNullBitVector, outputDataNullBitVectorSize);
        	free(inputData);
        	return 0;
        };
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        	vector<Attribute> input_attrs;
        	input->getAttributes(input_attrs);
        	for (int var = 0; var < attrNames.size(); ++var) {
        		string attrName = attrNames[var];
        		for (int var2 = 0; var2 < input_attrs.size(); ++var2) {
					Attribute attr = input_attrs[var2];
					if(attr.name == attrName){
						attrs.push_back(attr);
						break;
					}
				}
			}
        };
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
		vector<Attribute> leftAttributes, rightAttributes;
		Iterator* leftIn;
		IndexScan* rightIn;
		Condition condition;
		void* leftData;
		bool isEOF;
		void* lowKey;
		Attribute leftAttr;
		bool notEqualToLeftDone;
		bool lowKeyNULL;
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin();
        RC findLowKey(void* leftData, void* lowKey, Attribute &leftAttr, bool &lowKeyNULL);
        RC setRightIterator(CompOp compOp);
        static RC joinTuples(void* leftData, void* rightData, void* joinedData,
        		vector<Attribute> leftAttributes, vector<Attribute> rightAttributes);

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

// Optional for the undergraduate solo teams: 5 extra-credit points
class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
		Iterator *leftIn;
		TableScan *rightIn;
		Condition condition;
		unsigned numPages;
		unsigned hashTableSize;
		map<string, vector<void*> > stringMap;
		map<int, vector<void*> > intMap;
		map<float, vector<void*> > floatMap;
		bool isleftTableFinished = false;

		vector<void*> leftMatchingTuples;
		int leftMatchingTuplesIndex = 0;
		void* right_data = malloc(PAGE_SIZE);

		vector<Attribute> leftAttrs;
		vector<Attribute> rightAttrs;

        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        ){
        	this->leftIn = leftIn;
        	this->rightIn = rightIn;
        	this->condition = condition;
        	this->numPages = numPages;
        	leftIn->getAttributes(leftAttrs);
        	rightIn->getAttributes(rightAttrs);
        	RC retVal = populateHashTable();

        };
        ~BNLJoin(){
        	free(right_data);
        };

        unsigned getDataSize(void * data, vector<Attribute> attrs){
        	int nullBitVectorSize = ceil((double)attrs.size() / CHAR_BIT);
        	unsigned offset = nullBitVectorSize;
        	for (int var = 0; var < attrs.size(); ++var) {
        		Attribute attr = attrs[var];
        		bool isNull = ((char*)data)[var / 8] & ( 1 << ( 7 - ( var % 8 )));
        		if(isNull){
        			//don't update offset
        		}else if(attr.type == TypeInt || attr.type == TypeReal){
        			offset += sizeof(int);
				}else if(attr.type == TypeVarChar){
					int length;
					memcpy(&length, data + offset, sizeof(int));
					offset += sizeof(int) + length;
				}
			}
        	return offset;
        }

        RC populateHashTable(){

        	//remove the previous entries in the hash table
        	for(map<int, vector<void*> >::iterator iterator = intMap.begin(); iterator != intMap.end(); ++iterator){
        		vector<void*> tuples = iterator->second;
        		for (int var = 0; var < tuples.size(); ++var) {
					free(tuples[var]);
				}
        	}
        	for(std::map<float, vector<void*> >::iterator iterator = floatMap.begin(); iterator != floatMap.end(); ++iterator){
        		vector<void*> tuples = iterator->second;
				for (int var = 0; var < tuples.size(); ++var) {
					free(tuples[var]);
				}
        	}
        	for(std::map<string, vector<void*> >::iterator iterator = stringMap.begin(); iterator != stringMap.end(); ++iterator){
        		vector<void*> tuples = iterator->second;
				for (int var = 0; var < tuples.size(); ++var) {
					free(tuples[var]);
				}
			}
        	intMap.clear();
        	floatMap.clear();
        	stringMap.clear();
        	hashTableSize = 0;
        	if(isleftTableFinished){
        		return QE_EOF;
        	}
        	bool isHashTableLimitReached = false;
        	do{
        		void* left_data = malloc(PAGE_SIZE);
				RC retVal = leftIn->getNextTuple(left_data);
				unsigned leftDataSize = getDataSize(left_data, leftAttrs);
				if(retVal == QE_EOF){
					isleftTableFinished = true;
					if(intMap.size() > 0 || floatMap.size() > 0 || stringMap.size() > 0){
						free(left_data);//free?
						return 0;
					}else{
						free(left_data);//free?
						return QE_EOF;
					}
				}


				int left_nullBitVectorSize = ceil((double)leftAttrs.size() / CHAR_BIT);
				int left_offset = left_nullBitVectorSize;

				for (int var = 0; var < leftAttrs.size(); ++var) {
					bool isNull = ((char*)left_data)[var / 8] & ( 1 << ( 7 - ( var % 8 )));
					Attribute attr = leftAttrs[var];
					if(attr.name == condition.lhsAttr){
						if(isNull){
							//don't populate the map with null keys
							break;
						}
						if(attr.type == TypeInt){
							int value;
							memcpy(&value, left_data + left_offset, sizeof(int));
							std::map<int, vector<void*> >::iterator iterator = intMap.find(value);
							if(iterator == intMap.end()){
								vector<void*> tuples;
								tuples.push_back(left_data);
								intMap.insert(pair<int, vector<void*> >(value, tuples));
							}else{
								vector<void*> tuples = iterator->second;
								tuples.push_back(left_data);
								intMap.insert(pair<int, vector<void*> >(value, tuples));
							}
							hashTableSize += leftDataSize;
							if(hashTableSize >= PAGE_SIZE * numPages){
								isHashTableLimitReached  = true;
							}

						}else if(attr.type == TypeReal){
							float value;
							memcpy(&value, left_data + left_offset, sizeof(float));

							std::map<float, vector<void*> >::iterator iterator = floatMap.find(value);
							if(iterator == floatMap.end()){
								vector<void*> tuples;
								tuples.push_back(left_data);
								floatMap.insert(pair<float, vector<void*> >(value, tuples));
							}else{
								vector<void*> tuples = iterator->second;
								tuples.push_back(left_data);
								floatMap.insert(pair<float, vector<void*> >(value, tuples));
							}

							hashTableSize += leftDataSize;
							if(hashTableSize >= PAGE_SIZE * numPages){
								isHashTableLimitReached  = true;
							}
						}else if(attr.type == TypeVarChar){

							int length;
							memcpy(&length, left_data + left_offset, sizeof(int));
							char* charValue = (char*)malloc(PAGE_SIZE);
							memcpy(charValue, left_data + left_offset + sizeof(int), length);
							charValue[length] = '\0';

							string value = string(charValue);

							std::map<string, vector<void*> >::iterator iterator = stringMap.find(value);
							if(iterator == stringMap.end()){
								vector<void*> tuples;
								tuples.push_back(left_data);
								stringMap.insert(pair<string, vector<void*> >(value, tuples));
							}else{
								vector<void*> tuples = iterator->second;
								tuples.push_back(left_data);
								stringMap.insert(pair<string, vector<void*> >(value, tuples));
							}

							hashTableSize += leftDataSize;
							if(hashTableSize >= PAGE_SIZE * numPages){
								isHashTableLimitReached  = true;
							}
							free(charValue);
						}
						break;
					}else{
						//update left_offset
						if(isNull){
							//don't update
						}else if(attr.type == TypeInt || attr.type == TypeReal){
							left_offset += sizeof(int);
						}else if(attr.type == TypeVarChar){
							int length;
							memcpy(&length, left_data + left_offset, sizeof(int));
							left_offset += sizeof(int) + length;
						}
					}
				}
				//don't free left_data it is used in the map;
				//free(left_data);

        	}while(isHashTableLimitReached);
        	return 0;
        }

        RC getNextTuple(void *data){
        	if(leftMatchingTuples.size() > leftMatchingTuplesIndex){
        		void* left_data = leftMatchingTuples[leftMatchingTuplesIndex];
        		leftMatchingTuplesIndex ++;
        		INLJoin::joinTuples(left_data, right_data, data, leftAttrs, rightAttrs);
        		return 0;
        	}
        	bool isMatchFound = false;
        	do{
        		void* right_data = malloc(PAGE_SIZE);
				RC retVal2 = rightIn->getNextTuple(right_data);
				if(retVal2 == QE_EOF){
					RC retVal = populateHashTable();
					if(retVal == QE_EOF){
						free(right_data);
						return QE_EOF;
					}else{
						//set iterator of right table so that we loop through it from start again
						rightIn->setIterator();
					}


				}
				int right_nullBitVectorSize = ceil((double)rightAttrs.size() / CHAR_BIT);
				int right_offset = right_nullBitVectorSize;
				for (int var = 0; var < rightAttrs.size(); ++var) {
					Attribute attr = rightAttrs[var];
					bool isNull = ((char*)right_data)[var / 8] & ( 1 << ( 7 - ( var % 8 )));
					if(condition.rhsAttr == attr.name){
						if(isNull){
							isMatchFound = false;
							//the attribute value is null, so there can't be any match
							break;
						}
						if(attr.type == TypeInt){
							int value;
							memcpy(&value, right_data + right_offset, sizeof(int));
							std::map<int, vector<void*> >::iterator iterator = intMap.find(value);
							if(iterator != intMap.end()){
								isMatchFound = true;
								vector<void*> tuples = iterator->second;
								if(tuples.size() > 1){
									leftMatchingTuples = tuples;
									leftMatchingTuplesIndex = 1;
									memcpy(this->right_data, right_data, PAGE_SIZE);
								}
								void* leftTuple = tuples[0];
								INLJoin::joinTuples(leftTuple, right_data, data, leftAttrs, rightAttrs);
								break;
							}else{
								isMatchFound = false;
								break;
							}
						}else if(attr.type == TypeReal){
							float value;
							memcpy(&value, right_data + right_offset, sizeof(float));
							std::map<float, vector<void*> >::iterator iterator = floatMap.find(value);
							if(iterator != floatMap.end()){
								isMatchFound = true;
								vector<void*> tuples = iterator->second;
								if(tuples.size() > 1){
									leftMatchingTuples = tuples;
									leftMatchingTuplesIndex = 1;
									memcpy(this->right_data, right_data, PAGE_SIZE);
								}
								void* leftTuple = tuples[0];
								INLJoin::joinTuples(leftTuple, right_data, data, leftAttrs, rightAttrs);
								break;
							}else{
								isMatchFound = false;
								break;
							}
						}else if(attr.type == TypeVarChar){

							char* charValue = (char*)malloc(PAGE_SIZE);
							int length;
							memcpy(&length, right_data + right_offset, sizeof(int));
							memcpy(charValue, right_data + right_offset + sizeof(int), length);
							charValue[length] = '\0';
							string value = string(charValue);
							free(charValue);

							std::map<string, vector<void*> >::iterator iterator = stringMap.find(value);
							if(iterator != stringMap.end()){
								isMatchFound = true;
								vector<void*> tuples = iterator->second;
								if(tuples.size() > 1){
									leftMatchingTuples = tuples;
									leftMatchingTuplesIndex = 1;
									memcpy(this->right_data, right_data, PAGE_SIZE);
								}
								void* leftTuple = tuples[0];
								INLJoin::joinTuples(leftTuple, right_data, data, leftAttrs, rightAttrs);
								break;
							}else{
								isMatchFound = false;
								break;
							}
						}
					}else{
						if(isNull){
							//don't update
						}else if(attr.type == TypeInt || attr.type == TypeReal){
							right_offset += sizeof(int);
						}else if(attr.type == TypeVarChar){
							int length;
							memcpy(&length, right_data + right_offset, sizeof(int));
							right_offset += sizeof(int) + length;
						}
					}
				}

				free(right_data);
        	}while(!isMatchFound);
        	return 0;
        };
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        	for (int var = 0; var < leftAttrs.size(); ++var) {
        		attrs.push_back(leftAttrs[var]);
        	}
        	for(int var = 0; var < rightAttrs.size(); ++var){
        		attrs.push_back(rightAttrs[var]);
        	}
        };
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory for graduate teams/solos. Optional for undergrad solo teams: 5 extra-credit points
        // Basic aggregation
		Iterator *input;
		Attribute aggAttr;
		Attribute groupAttr;
		AggregateOp aggregateOP;
		vector<Attribute> inputAttrs;
		vector<int> intVector;
		vector<float> floatVector;
		map<int, vector<int> > intVsIntVectorMap;
		map<float, vector<int> > floatVsIntVectorMap;
		map<string, vector<int> > stringVsIntVectorMap;
		map<int, vector<float> > intVsFloatVectorMap;
		map<float, vector<float> > floatVsFloatVectorMap;
		map<string, vector<float> > stringVsFloatVectorMap;
		map<int, vector<int> >:: iterator intVsIntVectorIter;
		map<float, vector<int> >:: iterator floatVsIntVectorIter;
		map<string, vector<int> >:: iterator stringVsIntVectorIter;
		map<int, vector<float> >:: iterator intVsFloatVectorIter;
		map<float, vector<float> >:: iterator floatVsFloatVectorIter;
		map<string, vector<float> >:: iterator stringVsFloatVectorIter;
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){
        	this->input = input;
        	this->aggAttr = aggAttr;
        	this->aggregateOP = op;
        	input->getAttributes(inputAttrs);
        	this->groupAttr.name = "";
        };
        RC populateMaps(){
        	void* data = malloc(PAGE_SIZE);
        	bool isAggAttrNull = false;
        	bool isGroupAttrNull = false;
        	void* aggAttrData = malloc(PAGE_SIZE);
        	void* groupAttrData = malloc(PAGE_SIZE);
        	while(input->getNextTuple(data) != QE_EOF){
        		int offset = ceil((double)inputAttrs.size()/CHAR_BIT);
				for (int var = 0; var < inputAttrs.size(); ++var) {
					if(inputAttrs[var].name == aggAttr.name){
						if( ((char*)data)[var/8] & (1 << (7 - (var%8)) ) ){
							isAggAttrNull = true;
							break;
						}
						if(inputAttrs[var].type == TypeInt){
							int k;
//							cout<<k<<endl;
							memcpy(&k, (char*) data + offset, sizeof(int));
							offset += sizeof(int);
							memcpy((char*)aggAttrData, &k, sizeof(int));
						}
						else if(inputAttrs[var].type == TypeReal){
							float k;
							memcpy(&k, (char*) data + offset, sizeof(float));
							offset += sizeof(float);
							memcpy((char*) aggAttrData, &k, sizeof(float));
						}
					}
					else if(inputAttrs[var].name == groupAttr.name){
						if( ((char*)data)[var/8] & (1 << (7 - (var%8)) ) ){
							isGroupAttrNull = true;
							break;
						}
						if(inputAttrs[var].type == TypeInt){
							int k;
							memcpy(&k, (char*) data + offset, sizeof(int));
							offset += sizeof(int);
							memcpy((char*) groupAttrData, &k, sizeof(int));
						}
						else if(inputAttrs[var].type == TypeReal){
							float k;
							memcpy(&k, (char*) data + offset, sizeof(float));
							offset += sizeof(float);
							memcpy((char*) groupAttrData, &k, sizeof(float));
						}
						else{
							int len;
							memcpy((char*) data + offset, &len, sizeof(int));
							memcpy((char*) groupAttrData, (char*) data + offset, sizeof(int) + len);
							offset += sizeof(int) + len;
						}
					}
					else{
						if( ((char*)data)[var/8] & (1 << (7 - (var%8)) ) ){
							continue;
						}
						if(inputAttrs[var].type == TypeInt || inputAttrs[var].type == TypeReal){
							offset += sizeof(int);
						}
						else{
							int len;
							memcpy(&len, (char*) data + offset, sizeof(int));
							offset += sizeof(int) + len;
						}
					}
				}
				//now update the maps
				if(isAggAttrNull || isGroupAttrNull){
					continue;
				}
				if(groupAttr.type == TypeInt && aggAttr.type == TypeInt){
					int groupAttrVal;
					int aggAttrVal;
					memcpy(&groupAttrVal, (char*) groupAttrData, sizeof(int));
					memcpy(&aggAttrVal, (char*) aggAttrData, sizeof(int));
					if(intVsIntVectorMap.count(groupAttrVal) > 0){
						intVsIntVectorMap[groupAttrVal].push_back(aggAttrVal);
					}
					else{
						vector<int> vec;
						vec.push_back(aggAttrVal);
						intVsIntVectorMap.insert(make_pair(groupAttrVal, vec));
					}
//					map<int, vector<int> >::iterator iter = intVsIntVectorMap.find(groupAttrVal);
//					if(iter == intVsIntVectorMap.end()){
//						vector<int> vec;
//						vec.push_back(aggAttrVal);
//						intVsIntVectorMap.insert(make_pair(groupAttrVal, vec));
//					}
//					else{
//						vector<int> vec = iter->second;
//						vec.push_back(aggAttrVal);
//						intVsIntVectorMap.insert(make_pair(groupAttrVal, vec));
//					}
				}
				else if(groupAttr.type == TypeInt && aggAttr.type == TypeReal){
					int groupAttrVal;
					float aggAttrVal;
					memcpy(&groupAttrVal, (char*) groupAttrData, sizeof(int));
					memcpy(&aggAttrVal, (char*) aggAttrData, sizeof(float));
					if(intVsFloatVectorMap.count(groupAttrVal) > 0){
						intVsFloatVectorMap[groupAttrVal].push_back(aggAttrVal);
					}
					else{
						vector<float> vec;
						vec.push_back(aggAttrVal);
						intVsFloatVectorMap.insert(make_pair(groupAttrVal, vec));
					}
				}
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeInt){
					float groupAttrVal;
					int aggAttrVal;
					memcpy(&groupAttrVal, (char*) groupAttrData, sizeof(float));
					memcpy(&aggAttrVal, (char*) aggAttrData, sizeof(int));
					if(floatVsIntVectorMap.count(groupAttrVal) > 0){
						floatVsIntVectorMap[groupAttrVal].push_back(aggAttrVal);
					}
					else{
						vector<int> vec;
						vec.push_back(aggAttrVal);
						floatVsIntVectorMap.insert(make_pair(groupAttrVal, vec));
					}
				}
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeReal){
					float groupAttrVal;
					float aggAttrVal;
					memcpy(&groupAttrVal, (char*) groupAttrData, sizeof(float));
					memcpy(&aggAttrVal, (char*) aggAttrData, sizeof(float));
					if(floatVsFloatVectorMap.count(groupAttrVal) > 0){
						floatVsFloatVectorMap[groupAttrVal].push_back(aggAttrVal);
					}
					else{
						vector<float> vec;
						vec.push_back(aggAttrVal);
						floatVsFloatVectorMap.insert(make_pair(groupAttrVal, vec));
					}
				}
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeInt){
					char* str = (char*) malloc(PAGE_SIZE);
					int len;
					memcpy(&len, (char*) groupAttrData, sizeof(int));
					memcpy(str, (char*) groupAttrData + sizeof(int), len);
					str[len] = '\0';
					string groupAttrVal(str);
					free(str);
					int aggAttrVal;
					memcpy(&aggAttrVal, (char*) aggAttrData, sizeof(int));
					if(stringVsIntVectorMap.count(groupAttrVal) > 0){
						stringVsIntVectorMap[groupAttrVal].push_back(aggAttrVal);
					}
					else{
						vector<int> vec;
						vec.push_back(aggAttrVal);
						stringVsIntVectorMap.insert(make_pair(groupAttrVal, vec));
					}
				}
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeReal){
					char* str = (char*) malloc(PAGE_SIZE);
					int len;
					memcpy(&len, (char*) groupAttrData, sizeof(int));
					memcpy(str, (char*) groupAttrData + sizeof(int), len);
					str[len] = '\0';
					string groupAttrVal(str);
					free(str);
					float aggAttrVal;
					memcpy(&aggAttrVal, (char*) aggAttrData, sizeof(float));
					if(stringVsFloatVectorMap.count(groupAttrVal) > 0){
						stringVsFloatVectorMap[groupAttrVal].push_back(aggAttrVal);
					}
					else{
						vector<float> vec;
						vec.push_back(aggAttrVal);
						stringVsFloatVectorMap.insert(make_pair(groupAttrVal, vec));
					}
				}
        	}
        	free(data);
        	free(aggAttrData);
        	free(groupAttrData);
        	intVsIntVectorIter = intVsIntVectorMap.begin();
        	intVsFloatVectorIter = intVsFloatVectorMap.begin();
        	floatVsIntVectorIter = floatVsIntVectorMap.begin();
        	floatVsFloatVectorIter = floatVsFloatVectorMap.begin();
        	stringVsIntVectorIter = stringVsIntVectorMap.begin();
        	stringVsFloatVectorIter = stringVsFloatVectorMap.begin();
        }
        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        ){
        	this->input = input;
        	this->aggAttr = aggAttr;
        	this->aggregateOP = op;
        	input->getAttributes(inputAttrs);
        	this->groupAttr = groupAttr;
        	populateMaps();
        };
        ~Aggregate(){};

        RC constructData(float value, void* data){
        	int nullBitVectorSize = 1;
        	char* nullBitVector = (char*)malloc(nullBitVectorSize);
        	memset(nullBitVector, 0, nullBitVectorSize);

        	if(value == NULL){
        		nullBitVector[0] = nullBitVector[0] | (1 << 7);
        	}else{
            	memcpy(data + nullBitVectorSize, &value, sizeof(float));
        	}
        	memcpy(data, nullBitVector, nullBitVectorSize);
        }


        RC getNextTuple(void *data){
        	if(aggAttr.type == TypeVarChar){
        		return QE_EOF;
        	}
			if(groupAttr.name.empty()){
				void *inputData = malloc(PAGE_SIZE);
				int nullBitVectorSize = ceil((float)inputAttrs.size() / CHAR_BIT);
				while(input->getNextTuple(inputData) != QE_EOF){
					int offset = nullBitVectorSize;
					for (int var = 0; var < inputAttrs.size(); ++var) {
						bool isNull = ((char*)inputData)[var / 8] & ( 1 << ( 7 - ( var % 8 )));
						Attribute attr = inputAttrs[var];
						if(attr.name == aggAttr.name){
							if(isNull){
								break;
							}
							if(attr.type == TypeInt){
								int value;
								memcpy(&value, inputData + offset, sizeof(int));
								intVector.push_back(value);
								break;
							}else if(attr.type == TypeReal){
								float value;
								memcpy(&value, inputData + offset, sizeof(float));
								floatVector.push_back(value);
								break;
							}
						}else{
							if(isNull){
								//don't update
							}else if(attr.type == TypeInt || attr.type == TypeReal){
								offset += sizeof(int);
							}else if(attr.type == TypeVarChar){
								int length;
								memcpy(&length, inputData + offset, sizeof(int));
								offset += sizeof(int) + length;
							}
						}
					}
				}
				free(inputData);
				if(intVector.size() == 0 && floatVector.size() == 0){
					return QE_EOF;
				}
				getAggregateData(data, intVector,floatVector);

				intVector.clear();
				floatVector.clear();
			}
			else{//group by aggregate
				void* aggData = malloc(PAGE_SIZE);
				if(groupAttr.type == TypeInt && aggAttr.type == TypeInt){
					if(intVsIntVectorIter == intVsIntVectorMap.end()){
						free(aggData);
						return QE_EOF;
					}
					vector<float> placeHolder;
					getAggregateData(aggData, intVsIntVectorIter->second, placeHolder);
					int val = intVsIntVectorIter->first;
					memset((char*)data, 0, 1);
					int offset = 1;
					memcpy((char*) data + offset, &val, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) data + offset, (char*) aggData + 1, sizeof(float));
					intVsIntVectorIter++;
				}
				else if(groupAttr.type == TypeInt && aggAttr.type == TypeReal){
					if(intVsFloatVectorIter == intVsFloatVectorMap.end()){
						free(aggData);
						return QE_EOF;
					}
					vector<int> placeHolder;
					getAggregateData(aggData, placeHolder, intVsFloatVectorIter->second);
					int val = intVsFloatVectorIter->first;
					memset((char*)data, 0, 1);
					int offset = 1;
					memcpy((char*) data + offset, &val, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) data + offset, (char*) aggData + 1, sizeof(float));
					intVsFloatVectorIter++;
				}
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeInt){
					if(floatVsIntVectorIter == floatVsIntVectorMap.end()){
						free(aggData);
						return QE_EOF;
					}
					vector<float> placeHolder;
					getAggregateData(aggData, floatVsIntVectorIter->second, placeHolder);
					float val = floatVsIntVectorIter->first;
					memset((char*)data, 0, 1);
					int offset = 1;
					memcpy((char*) data + offset, &val, sizeof(float));
					offset += sizeof(float);
					memcpy((char*) data + offset, (char*) aggData + 1, sizeof(float));
					floatVsIntVectorIter++;
				}
				else if(groupAttr.type == TypeReal && aggAttr.type == TypeReal){
					if(floatVsFloatVectorIter == floatVsFloatVectorMap.end()){
						free(aggData);
						return QE_EOF;
					}
					vector<int> placeHolder;
					getAggregateData(aggData, placeHolder, floatVsFloatVectorIter->second);
					float val = floatVsFloatVectorIter->first;
					memset((char*)data, 0, 1);
					int offset = 1;
					memcpy((char*) data + offset, &val, sizeof(float));
					offset += sizeof(float);
					memcpy((char*) data + offset, (char*) aggData + 1, sizeof(float));
					floatVsFloatVectorIter++;
				}
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeInt){
					if(stringVsIntVectorIter == stringVsIntVectorMap.end()){
						free(aggData);
						return QE_EOF;
					}
					vector<float> placeHolder;
					getAggregateData(aggData, stringVsIntVectorIter->second, placeHolder);
					string val = stringVsIntVectorIter->first;
					memset((char*)data, 0, 1);
					int offset = 1;
					int len = val.length();
					memcpy((char*) data + offset, &len, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) data + offset, val.c_str(), len);
					offset += len;
					memcpy((char*) data + offset, (char*) aggData + 1, sizeof(float));
					stringVsIntVectorIter++;
				}
				else if(groupAttr.type == TypeVarChar && aggAttr.type == TypeReal){
					if(stringVsFloatVectorIter == stringVsFloatVectorMap.end()){
						free(aggData);
						return QE_EOF;
					}
					vector<int> placeHolder;
					getAggregateData(aggData, placeHolder, stringVsFloatVectorIter->second);
					string val = stringVsFloatVectorIter->first;
					memset((char*)data, 0, 1);
					int offset = 1;
					int len = val.length();
					memcpy((char*) data + offset, &len, sizeof(int));
					offset += sizeof(int);
					memcpy((char*) data + offset, val.c_str(), len);
					offset += len;
					memcpy((char*) data + offset, (char*) aggData + 1, sizeof(float));
					stringVsFloatVectorIter++;
				}
				free(aggData);
			}
        	return 0;
        };
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const{
        	if(!groupAttr.name.empty()){
        		attrs.push_back(groupAttr);
        	}
        	Attribute attr = aggAttr;
        	attr.type = TypeReal;
        	attr.length = 4;
        	string str;
        	if(aggregateOP == MIN){
        		str = "MIN";
        	}
        	else if(aggregateOP == MAX){
        		str = "MAX";
        	}
        	else if(aggregateOP == SUM){
        		str = "SUM";
        	}
        	else if(aggregateOP == AVG){
        		str = "AVG";
        	}
        	else if(aggregateOP == COUNT){
        		str = "COUNT";
        	}
        	attr.name = str + "(" + attr.name + ")";
        	attrs.push_back(attr);

        };

        RC getAggregateData(void* data, vector<int> intVector, vector<float> floatVector){
        	if(aggregateOP == MIN){
				if(aggAttr.type == TypeInt){
					float min = (float)intVector[0];
					for (int var = 1; var < intVector.size(); ++var) {
						if(intVector[var] < min){
							min = (float)intVector[var];
						}
					}
					constructData(min, data);
				}else if(aggAttr.type == TypeReal){
					float min = floatVector[0];
					for (int var = 1; var < floatVector.size(); ++var) {
						if(floatVector[var] < min){
							min = floatVector[var];
						}
					}
					constructData(min, data);
				}
			}else if(aggregateOP == MAX){
				if(aggAttr.type == TypeInt){
					float max = (float)intVector[0];
					for (int var = 1; var < intVector.size(); ++var) {
						if(intVector[var] > max){
							max = (float)intVector[var];
						}
					}
					constructData(max, data);
				}else if(aggAttr.type == TypeReal){
					float max = floatVector[0];
					for (int var = 1; var < floatVector.size(); ++var) {
						if(floatVector[var] > max){
							max = floatVector[var];
						}
					}
					constructData(max, data);
				}
			}else if(aggregateOP == COUNT){
				if(aggAttr.type == TypeInt){
					float count = (float)intVector.size();
					constructData(count, data);
				}else if(aggAttr.type == TypeReal){
					float count = floatVector.size();
					constructData(count, data);

				}

			}else if(aggregateOP == SUM){
				if(aggAttr.type == TypeInt){
					float sum = 0;
					for (int var = 0; var < intVector.size(); ++var) {
						sum += (float)intVector[var];
					}
					constructData(sum, data);
				}else if(aggAttr.type == TypeReal){
					float sum = 0;
					for (int var = 0; var < floatVector.size(); ++var) {
						sum += floatVector[var];
					}
					constructData(sum, data);
				}
			}else if(aggregateOP == AVG){
				if(aggAttr.type == TypeInt){
					float sum = 0;
					for (int var = 0; var < intVector.size(); ++var) {
						sum += (float)intVector[var];
					}
					float avg = (float)sum / intVector.size();
					constructData(avg, data);
				}else if(aggAttr.type == TypeReal){
					float sum = 0;
					for (int var = 0; var < floatVector.size(); ++var) {
						sum += floatVector[var];
					}
					float avg = (float)sum / floatVector.size();
					constructData(avg, data);
				}
			}
        }

};

#endif
