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
    
    Record rec;
    RID rid;
    Status status;
    
    // Create a HeapFileScan for the first relation in the projection list
    HeapFileScan* hfs = new HeapFileScan(projNames[0].relName, status);
    if (status != OK)
    {
        delete hfs;
        return status;
    }

    // Start the scan based on whether a filter is provided
    if (attrDesc == NULL)
    {
        // Unconditional scan - start scan with dummy condition that always matches
        if ((status = hfs->startScan(0, 0, STRING, NULL, EQ)) != OK)
        {
            delete hfs;
            return status;
        }
    }
    else
    {
        // Conditional scan based on the attribute type
        int intValue;
        float floatValue;
        switch(attrDesc->attrType)
        {
            case STRING:
                status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                        (Datatype)attrDesc->attrType, filter, (Operator)op);
                break;
        
            case INTEGER:
                intValue = atoi(filter);
                status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                        (Datatype)attrDesc->attrType, (char *)&intValue, (Operator)op);
                break;
        
            case FLOAT:
                floatValue = atof(filter);
                status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                        (Datatype)attrDesc->attrType, (char *)&floatValue, (Operator)op);
                break;
        }
        
        if (status != OK)
        {
            delete hfs;
            return status;
        }
    }
    
    // Iterate through matching records
    while ((status = hfs->scanNext(rid)) == OK)
    {
        if (status == OK)
        {
            // Get the current record
            status = hfs->getRecord(rec);
            if (status != OK)
                break;
            
            // Prepare attribute list for insertion
            attrInfo attrList[projCnt];
            int intValue = 0;
            float floatValue;
            
            for (int i = 0; i < projCnt; i++)
            {
                AttrDesc attrDesc = projNames[i];
                
                // Set up attribute info
                strcpy(attrList[i].relName, attrDesc.relName);
                strcpy(attrList[i].attrName, attrDesc.attrName);
                attrList[i].attrType = attrDesc.attrType;
                attrList[i].attrLen = attrDesc.attrLen;
                
                // Allocate memory for attribute value
                attrList[i].attrValue = (void *)malloc(attrDesc.attrLen);
                
                // Copy attribute value based on type
                switch(attrList[i].attrType)
                {
                    case STRING: 
                        memcpy((char *)attrList[i].attrValue, 
                               (char *)(rec.data + attrDesc.attrOffset), 
                               attrDesc.attrLen);
                        break;
                        
                    case INTEGER: 
                        memcpy(&intValue, 
                               (int *)(rec.data + attrDesc.attrOffset), 
                               attrDesc.attrLen);
                        sprintf((char *)attrList[i].attrValue, "%d", intValue);
                        break;
                        
                    case FLOAT: 
                        memcpy(&floatValue, 
                               (float *)(rec.data + attrDesc.attrOffset), 
                               attrDesc.attrLen);
                        sprintf((char *)attrList[i].attrValue, "%f", floatValue);
                        break;
                }
            }
        
            // Insert the selected record into the result relation
            status = QU_Insert(result, projCnt, attrList);
            
            // // Free allocated memory
            // for (int i = 0; i < projCnt; i++)
            // {
            //     free(attrList[i].attrValue);
            // }
            
            // Check for insertion error
            if (status != OK)
            {
                delete hfs;
                return status;
            }
        }
    }
    
    // Clean up the HeapFileScan
    delete hfs;
    
    return OK;

    
}
