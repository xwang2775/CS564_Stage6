#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
                      const string & attrName, 
                      const Operator op,
                      const Datatype type, 
                      const char *attrValue)
{
    cout << "Doing QU_Delete" << endl; //print statement for diff
    Status status;
    HeapFileScan* hfs = new HeapFileScan(relation, status);
    if(status != OK)
        return status;

    AttrDesc attrDesc;
    RID rid;
    int intValue;
    float floatValue;
    
    // Get attribute information if attribute name is provided
    if (!attrName.empty()) {
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK) {
            delete hfs;
            return status;
        }
    }
    
    // Handle different data types for scanning
    int offset = attrDesc.attrOffset;
    int length = attrDesc.attrLen;
    
    switch(type)
    {
        case STRING:
            status = hfs->startScan(offset, length, type, attrValue, op);
            break;
        
        case INTEGER:
            intValue = atoi(attrValue);
            status = hfs->startScan(offset, length, type, (char *)&intValue, op);
            break;
        
        case FLOAT:
            floatValue = atof(attrValue);
            status = hfs->startScan(offset, length, type, (char *)&floatValue, op);
            break;
    }
        
    if (status != OK)
    {
        delete hfs;
        return status;
    }
    
    // Delete matching records
    while((status = hfs->scanNext(rid)) == OK) 
    {
        if ((status = hfs->deleteRecord()) != OK) {
            delete hfs;
            return status;
        }
    }

    // Check if we reached end of file (expected case)
    if (status != FILEEOF) {
        delete hfs;
        return status;
    }

    status = hfs->endScan();
    delete hfs;
    
    return OK;
}

