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
	Status status;
	HeapFileScan *hfs;

	// Create a HeapFileScan instance
	hdf = new HeapFileScan(relation, status);
	if(status != OK){
		delete hfs;
		return status;
	}

	// If no attribute specified (attrName is empty), delete all the records
    if (attrName.empty()) {
        status = hfs->startScan(0, 0, STRING, NULL, EQ);
    } else {
        // Get attribute catalog info to find its offset
        AttrCatInfoattrInfo;
        status = attrCat->getInfo(relation, attrName, *attrInfo);
        if (status != OK) {
            delete hfs;
            return status;
        }

        // Start filtered scan based on the attribute
        status = hfs->startScan(attrInfo->attrOffset, 
                              attrInfo->attrLen,
                              type,
                              attrValue,
                              op);
        delete attrInfo;
    }

    if (status != OK) {
        delete hfs;
        return status;
    }

    // Iterate through matching records and delete them
    RID rid;
    while ((status = hfs->scanNext(rid)) == OK) {
        status = hfs->deleteRecord();
        if (status != OK) {
            delete hfs;
            return status;
        }
    }

    // FILEEOF is expected when scan completes
    if (status != FILEEOF) {
        delete hfs;
        return status;
    }

    // Clean up
    status = hfs->endScan();
    delete hfs;

    return status;
}


