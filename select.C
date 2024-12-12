#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	//QU takes in attrInfo and ScanSelect takes in attrDesc 
	Status status;
	// Step 1: Prepare projection list
    AttrDesc projList[projCnt]; //attrdesc version of projnames
    for (int i = 0; i < projCnt; ++i) {
        Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projList[i]);
        if (status != OK) {
            return status; // Return error if attribute lookup fails
        }
    }

	// Step 2: Compute length of output tuples
    int outputTupleLength = 0;
    for (int i = 0; i < projCnt; ++i) {
        outputTupleLength += projList[i].attrLen;
    }

    // Step 3: Prepare selection attribute (if applicable) and pass into scan select
    AttrDesc *attrDesc = nullptr; //attrdesc version of attr
    if (attr != nullptr) {
        attrDesc = new AttrDesc();
        Status status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
        if (status != OK) {
            delete attrDesc;
            return status; // Return error if selection attribute lookup fails
        }
		if (attrDesc->attrType == INTEGER){
			int intVal = atoi(attrValue);
			return ScanSelect(result, projCnt, projList, attrDesc, op, (char*) &intVal, outputTupleLength);
		}else if (attrDesc->attrType == FLOAT){
			float floatVal = atof(attrValue);
			return ScanSelect(result, projCnt, projList, attrDesc, op, (char*) &floatVal, outputTupleLength);
		}else if (attrDesc->attrType == STRING){ //keep same for str
			return ScanSelect(result, projCnt, projList, attrDesc, op, attrValue, outputTupleLength);
		}
    }else{
		return ScanSelect(result, projCnt, projList, NULL, op, NULL, outputTupleLength); //null out for unconditional scan
	}
}


#include "stdio.h"
#include "stdlib.h"
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	// Open the result relation,
	// Prepare to insert records
	InsertFileScan resultRel(result, status);
	int resultTupCnt = 0;
	if (status != OK) return status;

	char outputData[reclen];
	Record outputRec;
	RID outRID;
	outputRec.data = (void*)outputData;
	outputRec.length = reclen;

	HeapFileScan scanner(string(projNames[0].relName), status);
	if(status != OK) return status;
	
	if(attrDesc == NULL) {
		// Full scan
		
		// Initialize scanner
		// If filter = NULL, then it marks all records as matched.
		status = scanner.startScan(0,0,STRING,NULL,EQ);
		if(status != OK) return status;

	} else {
		// Initialize Scanner
		status = scanner.startScan(attrDesc->attrOffset,
									attrDesc->attrLen,
									(Datatype) attrDesc->attrType,
									filter,
									op);
	}

	RID rec;
	while(scanner.scanNext(rec) == OK) {
		Record tempRec;
		status = scanner.getRecord(tempRec);
		assert(status == OK);
		
		// Look through each projection attribute
		// copy the relevant entry to the corresponding position
		int outputOffset = 0;
		for(int i = 0; i < projCnt; ++i) {
			memcpy((char*)outputRec.data+outputOffset,
					(char*)tempRec.data+projNames[i].attrOffset,
					projNames[i].attrLen);
			outputOffset += projNames[i].attrLen;
		}

		// Insert the record
		status = resultRel.insertRecord(outputRec, outRID);
		assert(status == OK);
		resultTupCnt++;
	}

	return OK;
}