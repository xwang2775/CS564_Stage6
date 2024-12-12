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
    // Status to track operations
    Status status;

    // Open the result table as an InsertFileScan for output
    InsertFileScan* resultFile = nullptr;

    resultFile = new InsertFileScan(result, status);
    if (status != OK) {
        delete resultFile;
        return status;
    }

    // Determine the source relation name from the first projection
    string sourceRelation(projNames[0].relName);

    // Open the source relation as a HeapFileScan
    HeapFileScan* scanFile = nullptr;
    scanFile = new HeapFileScan(sourceRelation, status);
    if (status != OK) {
        delete resultFile;
        delete scanFile;
        return status;
    }
    // If no selection condition, start an unconditional scan
    if (attrDesc == nullptr) {
        status = scanFile->startScan(0,0,STRING,NULL,EQ);
    } else {
        // Start a scan with the specific filter condition
        status = scanFile->startScan(attrDesc->attrOffset,attrDesc->attrLen,(Datatype)attrDesc->attrType
        ,filter,op);
    }

    if (status != OK) {
        delete resultFile;
        delete scanFile;
        return status;
    }

    // Allocate memory for the output record
    Record outputRecord;
    outputRecord.length = reclen;
    char* buff = new char[reclen];
    memset(buff, 0, reclen);
    outputRecord.data = buff;
    RID newRID;
    // Scan the source relation
    RID rid;
    Record record;
    while (scanFile->scanNext(rid) == OK) {
        // Get the current record
        status = scanFile->getRecord(record);
        if (status != OK) continue;

        // Reset output record offset
        int offset = 0;

        // Copy each projected attribute
        for (int i = 0; i < projCnt; ++i) {
            // Copy the attribute from source record to output record
            memcpy(outputRecord.data + offset, 
                   record.data + projNames[i].attrOffset, 
                   projNames[i].attrLen);
            
            // Move offset
            offset += projNames[i].attrLen;
        }

        // Insert the output record into result file
        status = resultFile->insertRecord(outputRecord, newRID);
        if (status != OK) {
            // Handle insertion error
            delete resultFile;
            delete scanFile;
            return status;
        }
    }

    // Cleanup
    delete resultFile;
    delete scanFile;

    return OK;

}