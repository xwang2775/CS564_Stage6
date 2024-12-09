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
	// Step 1: Prepare projection list
    AttrDesc projList[projCnt];
    for (int i = 0; i < projCnt; ++i) {
        Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projList[i]);
        if (status != OK) {
            return status; // Return error if attribute lookup fails
        }
    }

    // Step 2: Prepare selection attribute (if applicable)
    AttrDesc *attrDesc = nullptr;
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
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


}
