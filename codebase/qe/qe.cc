
#include "qe.h"
#include <iostream>
#include <cstdio>
using namespace std;
//Filter::Filter(Iterator* input, const Condition &condition) {
//}

// ... the rest of your implementations go here

INLJoin::INLJoin(Iterator* lIn, IndexScan* rIn, const Condition &cond){
	isEOF = false;
	leftIn = lIn;
	rightIn = rIn;
	leftIn->getAttributes(leftAttributes);
	rightIn->getAttributes(rightAttributes);
	condition = cond;
	leftData = malloc(PAGE_SIZE);
	lowKey = malloc(PAGE_SIZE);
	int ret = leftIn->getNextTuple(leftData);
	if(ret == RM_EOF){
		isEOF = true;
	}
	else{
		lowKeyNULL = true;
		while(lowKeyNULL){
			findLowKey(leftData, lowKey, leftAttr, lowKeyNULL);
			if(lowKeyNULL)
				leftIn->getNextTuple(leftData);
		}
		setRightIterator(condition.op);
		if(condition.op == NE_OP){
			notEqualToLeftDone = true;
		}
	}
}

INLJoin::~INLJoin(){
	free(leftData);
	free(lowKey);
}

RC INLJoin::setRightIterator(CompOp compOp){
	if(compOp == EQ_OP){
		rightIn->setIterator(lowKey, lowKey, true, true);
	}
	else if(compOp == LT_OP){
		rightIn->setIterator(NULL, lowKey, true, false);
	}
	else if(compOp == LE_OP){
		rightIn->setIterator(NULL, lowKey, true, true);
	}
	else if(compOp == GT_OP){
		rightIn->setIterator(lowKey, NULL, false, true);
	}
	else if(compOp == GE_OP){
		rightIn->setIterator(lowKey, NULL, true, true);
	}
	else if(compOp == NO_OP){
		rightIn->setIterator(NULL, NULL, true, true);
	}
	else if(compOp == NE_OP){
		rightIn->setIterator(NULL, lowKey, true, false);
	}
}

RC INLJoin::findLowKey(void* leftData, void* lowKey, Attribute &leftAttr, bool &lowKeyNULL){
	int offset = ceil((double)leftAttributes.size()/CHAR_BIT);
	for(int i = 0; i < leftAttributes.size(); i++){
		if(leftAttributes[i].name == condition.lhsAttr){
			leftAttr.name = leftAttributes[i].name;
			leftAttr.length = leftAttributes[i].length;
			leftAttr.type = leftAttributes[i].type;
			if( ((char*)leftData)[i/8] & ( 1 << (7 - (i%8))) ){
				lowKeyNULL = true;
				break;
			}
			if(leftAttributes[i].type == TypeInt){
				int key;
				memcpy(&key, (char*) leftData + offset, sizeof(int));
				memcpy((char*)lowKey, &key, sizeof(int));
			}
			else if(leftAttributes[i].type == TypeReal){
				float key;
				memcpy(&key, (char*) leftData + offset, sizeof(float));
				memcpy((char*)lowKey, &key, sizeof(float));
			}
			else if(leftAttributes[i].type == TypeVarChar){
				int len;
				memcpy(&len, (char*) leftData + offset, sizeof(int));
				memcpy((char*)lowKey, (char*) leftData + offset, sizeof(int) + len);
			}
			lowKeyNULL = false;
			break;
		}
		else{
			if( ((char*)leftData)[i/8] & ( 1 << (7 - (i%8))) ){
				continue;
			}
			if(leftAttributes[i].type == TypeInt){
				offset += sizeof(int);
			}
			else if(leftAttributes[i].type == TypeReal){
				offset += sizeof(float);
			}
			else{
				int len;
				memcpy(&len, (char*) leftData + offset, sizeof(int));
				offset += sizeof(int) + len;
			}
		}
	}
	return 0;
}

RC INLJoin::joinTuples(void* leftData, void* rightData, void* joinedData,
		vector<Attribute> leftAttrs, vector<Attribute> rightAttrs){
	int totalNumOfAttr = leftAttrs.size() + rightAttrs.size();
	int totalNullBitVectorSize = ceil((double)totalNumOfAttr/CHAR_BIT);
	int leftNullBitVectorSize = ceil((double)leftAttrs.size()/CHAR_BIT);
	int rightNullBitVectorSize = ceil((double)rightAttrs.size()/CHAR_BIT);
	int leftOffset = leftNullBitVectorSize;
	int rightOffset = rightNullBitVectorSize;
	unsigned char* nullBitVector = (unsigned char*) malloc(totalNullBitVectorSize);
	memset(nullBitVector, 0 , totalNullBitVectorSize);
	//find length of left tuple
	for(int i = 0; i < leftAttrs.size(); i++){
		if( ((char*)leftData)[i/8] & ( 1 << (7 - (i%8) ) ) ){
			nullBitVector[i/8] = nullBitVector[i/8] | (1 << (7 - (i%8)));
			continue;
		}
		if(leftAttrs[i].type == TypeInt){
			leftOffset += sizeof(int);
		}
		else if(leftAttrs[i].type == TypeReal){
			leftOffset += sizeof(float);
		}
		else{
			int len;
			memcpy(&len, (char*) leftData + leftOffset, sizeof(int));
			leftOffset += sizeof(int) + len;
		}
	}
	//find length of right tuple
	for(int i = 0; i < rightAttrs.size(); i++){
		if( ((char*)rightData)[i/8] & ( 1 << (7 - (i%8) ) ) ){
			nullBitVector[(leftAttrs.size() + i)/8] =
					nullBitVector[(leftAttrs.size() + i)/8] | (1 << (7 - ((leftAttrs.size() + i)%8)));
			continue;
		}
		if(rightAttrs[i].type == TypeInt){
			rightOffset += sizeof(int);
		}
		else if(rightAttrs[i].type == TypeReal){
			rightOffset += sizeof(float);
		}
		else{
			int len;
			memcpy(&len, (char*) rightData + rightOffset, sizeof(int));
			rightOffset += sizeof(int) + len;
		}
	}
	//now copy null bit vector
	int offsetOnJoinedData = 0;
	memcpy((char*) joinedData + offsetOnJoinedData, nullBitVector, totalNullBitVectorSize);
	offsetOnJoinedData += totalNullBitVectorSize;
	memcpy((char*) joinedData + offsetOnJoinedData, (char*) leftData + leftNullBitVectorSize, leftOffset - leftNullBitVectorSize);
	offsetOnJoinedData += leftOffset - leftNullBitVectorSize;
	memcpy((char*) joinedData + offsetOnJoinedData, (char*) rightData + rightNullBitVectorSize, rightOffset - rightNullBitVectorSize);
	free(nullBitVector);
	return 0;
}

RC INLJoin::getNextTuple(void* data){
	if(isEOF){
		return QE_EOF;
	}
	bool foundJoin = false;
	void* rightData = malloc(PAGE_SIZE);
	while(!isEOF && !foundJoin){
		if(rightIn->getNextTuple(rightData) != RM_EOF){
			foundJoin = true;
			//join the two tuples
			INLJoin::joinTuples(leftData, rightData, data, leftAttributes, rightAttributes);
		}
		if(!foundJoin){
			if(condition.op == NE_OP){
				if(notEqualToLeftDone == true){
					rightIn->setIterator(lowKey, NULL, false, true);
				}
				else{
					do{
						RC ret = leftIn->getNextTuple(leftData);
						if(ret == RM_EOF){
							isEOF = true;
							break;
						}
						findLowKey(leftData, lowKey, leftAttr, lowKeyNULL);
					}while(lowKeyNULL);
					setRightIterator(condition.op);
				}
				notEqualToLeftDone = !notEqualToLeftDone;
			}
			else{
				do{
					RC ret = leftIn->getNextTuple(leftData);
					if(ret == RM_EOF){
						isEOF = true;
						break;
					}
					findLowKey(leftData, lowKey, leftAttr, lowKeyNULL);
				}while(lowKeyNULL && !isEOF);
				if(!isEOF){
					setRightIterator(condition.op);
				}
			}
		}
	}
	free(rightData);
	if(isEOF){
		return QE_EOF;
	}
	return 0;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	for (int var = 0; var < leftAttributes.size(); ++var) {
		attrs.push_back(leftAttributes[var]);
	}
	for(int var = 0; var < rightAttributes.size(); ++var){
		attrs.push_back(rightAttributes[var]);
	}
}
