#include "catalog.h"
#include "query.h"
#include "stdio.h" //includes were under scanselect for some reason
#include "stdlib.h" 

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

	// Step 1: Prepare projection list
    AttrDesc projList[projCnt]; //attrdesc version of projnames
    for (int i = 0; i < projCnt; ++i) {
        Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projList[i]);
        if (status != OK) {
            return status; // Return error if attribute lookup fails
        }
    }

    // Step 2: Prepare selection attribute (if applicable)
    AttrDesc *attrDesc = nullptr; //attrdesc version of attr
    if (attr != nullptr) {
        attrDesc = new AttrDesc();
        Status status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
        if (status != OK) {
            delete attrDesc;
            return status; // Return error if selection attribute lookup fails
        }
    }

    // Step 3: Compute length of output tuples
    int outputTupleLength = 0;
    for (int i = 0; i < projCnt; ++i) {
        outputTupleLength += projList[i].attrLen;
    }

    // Step 4: Call ScanSelect
    Status status = ScanSelect(result, projCnt, projList, attrDesc, op, attrValue, outputTupleLength);

    // Step 5: Cleanup and return
    if (attrDesc != nullptr) delete attrDesc;
    return status;
}


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
    try {
        resultFile = new InsertFileScan(result, status);
        if (status != OK) {
            delete resultFile;
            return status;
        }
    } catch (...) {
        return BADFILE;
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
        int intValue;
        float floatValue;
        switch(attrDesc->attrType)
        {
            case STRING:
                status = scanFile->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                        (Datatype)attrDesc->attrType, filter, op);
                break;
        
            case INTEGER:
                intValue = atoi(filter);
                status = scanFile->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                        (Datatype)attrDesc->attrType, (char *)&intValue, op);
                break;
        
            case FLOAT:
                floatValue = atof(filter);
                status = scanFile->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                        (Datatype)attrDesc->attrType, (char *)&floatValue, op);
                break;
        }
    }

    if (status != OK) {
        delete resultFile;
        delete scanFile;
        return status;
    }

    // Allocate memory for the output record
    char* outputRecord = new char[reclen];
    memset(outputRecord, 0, reclen);

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
            memcpy(outputRecord + offset, 
                   record.data + projNames[i].attrOffset, 
                   projNames[i].attrLen);
            
            // Move offset
            offset += projNames[i].attrLen;
        }

        // Insert the output record into result file
        RID newRid;
        status = resultFile->insertRecord(record, newRid);
        if (status != OK) {
            // Handle insertion error
            delete[] outputRecord;
            delete resultFile;
            delete scanFile;
            return status;
        }
    }

    // Cleanup
    delete[] outputRecord;
    delete resultFile;
    delete scanFile;

    return OK;

    
}
