#include "catalog.h"
#include "query.h"

/**
 * FUNCTION: QU_Delete
 *
 * PURPOSE:  Deletes records from a specified relation.
 *
 * PARAMETERS:
 *    relation    (out)    Relation name from where records are to be deleted
 *    attrName    (in)     Name of the attribute to match when deleting records
 *    op          (in)     Operator to be used for matching
 *    type        (in)     Datatype of the attribute
 *    attrValue   (in)     Value to be used for matching
 *  
 * RETURN VALUES:
 *    Status  OK              Records successfully deleted from the relation
 *            BADCATPARM      Relation name is empty
 *            BADSCANPARM     Error in allocating page: All buffer frames are pinned
 *            FILEEOF         Reached the end of file while scanning for the record
 *            BUFFEREXCEEDED  All buffer frames are pinned
 *            HASHTBLERROR    Hash table error occurred
 *            PAGENOTPINNED   Pin count is already 0
 *            HASHNOTFOUND    Page is not in the buffer pool hash table
 **/
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

