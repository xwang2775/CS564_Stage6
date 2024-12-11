#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	cout << "Doing QU_Insert" << endl; //print statement for diff
	Status status;
  	AttrDesc *attrs;
  	int actualAttrCnt;
  	int insertOffset = 0;
	int tmpInt = 0;
	float tmpFloat = 0;

	// If no tmpInt is specified for an attribute, you should reject the insertion as Minirel does not implement NULLs. 
	for (int i = 0; i <= attrCnt - 1; ++i) {
		if (attrList[i].attrValue == NULL)
			return ATTRTYPEMISMATCH; 
	}
	
  	status = attrCat->getRelInfo(relation, actualAttrCnt, attrs);
	if (status != OK)
		return status;

	if(actualAttrCnt != attrCnt)
		return UNIXERR;
		
  	int tupleLength = 0;
  	for(int i = 0; i < attrCnt; i++)
    	tupleLength += attrs[i].attrLen;

  	char *tupleData;
  	if(!(tupleData = new char [tupleLength]))
  		return INSUFMEM;
	
  	// Since the order of the attributes in attrList[] may not be the same as in the relation, you might have to rearrange them before insertion. 

	for(int i = 0; i < attrCnt; i++)
	{
		bool attrFound = false;
		for(int j = 0; j < attrCnt; j++)
		{
			if(strcmp(attrs[i].attrName, attrList[j].attrName) == 0 && attrs[i].attrType == attrList[j].attrType)
			{
				insertOffset = attrs[i].attrOffset;
				
				switch(attrList[j].attrType)
				{
					case STRING: 
						memcpy((char *)tupleData + insertOffset, (char *)attrList[j].attrValue, attrs[i].attrLen);
						break;
				 		
					case INTEGER: 
						tmpInt = atoi((char *)attrList[j].attrValue);
				 		memcpy((char *)tupleData + insertOffset, &tmpInt, attrs[i].attrLen);
				 		break;
				 		
					case FLOAT: 
						tmpFloat = atof((char *)attrList[j].attrValue);		
						memcpy((char *)tupleData + insertOffset, &tmpFloat, attrs[i].attrLen);
				 		break;
				}
				
				attrFound = true;
				break;
			}
		}
		
		if(attrFound == false)
		{
			delete [] tupleData;
			free(attrs);
			return UNIXERR;
		}
	}
	
	InsertFileScan insertFileScan(relation, status);

  	Record insertTuple;
  	insertTuple.data = (void *) tupleData;
  	insertTuple.length = tupleLength;
	
  	RID outRID;
  	status = insertFileScan.insertRecord(insertTuple, outRID);	
	return status;
}

