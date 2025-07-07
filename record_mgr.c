#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

// Globals

// global schema
Schema *rm_schema; 

// NAME: Record_Manager
// PURPOSE: Global struct that holds record manager information such as num tuples, scan counts, 
// scan expression. This is used throughout the file to manage pointer information that way we don't have to
// re-allocate memory every function call. 
typedef struct Record_Manager
{
    // handle for buffer pool
    BM_BufferPool bm_handle; 

    // page handle for buffer pool 
    BM_PageHandle page_handle; 

    // file handle
    SM_FileHandle file_handle; 

    // record id
    RID rid; 

    // scan condition ptr
    Expr *condition;

    // tuple counter
	int num_tuples; 

    // scan counter
    int num_scanned; 

    // capacity of our buffer pool 
    int buffer_pool_capacity; 

    // first free page tracker
    int first_free_page; 

} Record_Manager;

// initialize pointer to the record manager
Record_Manager *rm;

// NAME: initRecordManager
// PURPOSE: initialize the record manager. This calls initStorageManager so we can begin writing files to disk
// PARAMS: 
// - mgmtData: void data pointer
// RETURN VAL: RC_OK
extern RC initRecordManager (void *mgmtData){

    // initialize storage manager
    initStorageManager();

    // return success
    return RC_OK;
}

// NAME: shutdownRecordManager
// PURPOSE: shuts down the record manager, frees the recordmanager pointer
// PARAMS: 
// - NONE
// RETURN VAL: RC_OK
extern RC shutdownRecordManager (){ 

    // free record manager pointer
    rm = NULL; 
    free(rm);

    // return success
    return RC_OK;
}

// NAME: find_slot
// PURPOSE: helper function to find an available slot in the page. 'X' denotes a free slot within the page
// PARAMS: 
// - data: pointer to the data within a page
// - record_size: size of each record in the page
// RETURN VAL: free slot within page
int find_slot(char *data, int record_size)
{
    // locals
	int total_slots = PAGE_SIZE / record_size; 
    int free_slot; 

    // loop through total slots for empty slot
	for (int i = 0; i < total_slots; i++){

        // if the slot isn't taken, return as free slot
        if (data[i * record_size] != 'X'){

            free_slot = i; 
            return free_slot;
        }

    }

    // if we find no free slot, return empty page
	return -1;
}

// NAME: create_table_info_page
// PURPOSE: helper function to create table data. This will create a new page file, initialize the buffer pool
// and initialze the schema. 
// PARAMS: 
// - name: name of the page file to create
// - schema: the schema which 
// RETURN VAL: true, false
bool create_table_info_page(char *name, Schema *schema){

    // locals 
    SM_FileHandle fh;
    RC rc_return;
	rm = (Record_Manager*) malloc(sizeof(Record_Manager));
    rm_schema = (Schema*) malloc(sizeof(Schema));
    char empty_page[PAGE_SIZE];

    // set schema to global schema
    rm_schema = schema;

    // initialize record manager attributes
    rm->buffer_pool_capacity = 50; 
    rm->num_tuples = 0; 
    rm->first_free_page = 1;  

	// Initalizing the Buffer Pool using FIFO page replacement policy
	initBufferPool(&rm->bm_handle, name, rm->buffer_pool_capacity, RS_FIFO, NULL);
		
	// create new page file with name from name parameter
	if((rc_return = createPageFile(name)) != RC_OK)
		return false;
		
	// open new page file 
	else if((rc_return = openPageFile(name, &fh)) != RC_OK)
		return false;
		
	// writing info page to first block of page file
	else if((rc_return = writeBlock(0, &fh, empty_page)) != RC_OK)
		return false;
		
	// closing file
	else if((rc_return = closePageFile(&fh)) != RC_OK)
		return false;

    // if file operations worked as expected, return true. 
    else
        return true; 

}

// NAME: createTable
// PURPOSE: The purpose of this function is to create the page file with the associated schema information
// and initialze the schema. 
// PARAMS: 
// - name: name of the page file to create
// - schema: the schema which belongs to the page file
// RETURN VAL: RC_OK, RC_CREATE_TABLE_ERROR
extern RC createTable (char *name, Schema *schema){ 

    // create new record page file 
    if(create_table_info_page(name, schema)){

        // return okay if no errors arose
        return RC_OK;
    }
    else{

        // error occured
        printf("ERROR, COULD NOT CREATE TABLE");
        return RC_CREATE_TABLE_ERROR;
    }
    
}

// NAME: openTable
// PURPOSE: The purpose of this function is to open the specifed page file and set the record manager parameters 
// relation information
// PARAMS: 
// - rel: table data struct
// - name: name of the page file associated with rel
// RETURN VAL: RC_OK
extern RC openTable (RM_TableData *rel, char *name)
{
	// point table data to the record manager struct
	rel->mgmtData = rm;

	// Setting rel name to the name of the page file in the record manager
	rel->name = name;
	
	// Setting rel schema to the schema of the record manager
	rel->schema = rm_schema;	

	return RC_OK;
}

// NAME: closeTable
// PURPOSE: The purpose of this function is to close the table. 'shutdownbufferpool' not only flushes all contents to disk, 
// but also closes the page file associated with the buffer pool
// PARAMS: 
// - rel: table data struct
// RETURN VAL: RC_OK
extern RC closeTable (RM_TableData *rel)
{

    // shut down buffer pool
	Record_Manager *rm = rel->mgmtData;
	shutdownBufferPool(&rm->bm_handle);

    return RC_OK; 
}

// NAME: deleteTable
// PURPOSE: The purpose of this function is to delete the page file associated with the table
// PARAMS: 
// - name: name of the file to delete
// RETURN VAL: RC_OK, RC_DESTROY_PAGE_ERROR
extern RC deleteTable (char *name)
{
    // locals
    RC rc_return; 

    // destroy specified page file
	if((rc_return = destroyPageFile(name)) != RC_OK){
        return RC_DESTROY_PAGE_ERROR;
    }

	return RC_OK;
}

// NAME: getNumTuples
// PURPOSE: The purpose of this function is to retrieve the number of tuples in a given page file
// PARAMS: 
// - rel: table data struct which to retrieve tuple count
// RETURN VAL: number of tuples
extern int getNumTuples (RM_TableData *rel){

    // locals
    rm = (Record_Manager*) malloc(sizeof(Record_Manager));
    int num_tuples; 

    // assign num tuples 
    num_tuples = rm->num_tuples;

    // return the number of tuples in record
    return num_tuples; 
}

// NAME: insertRecord
// PURPOSE: The purpose of this function is to insert a defined record into the page file
// PARAMS: 
// - rel: table data struct
// - record: record in which to insert
// RETURN VAL: RC_OK, RC_WRITE_FAILED
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	// locals
	Record_Manager *rm = rel->mgmtData;	
    RID *rid = &record->id; 
	SM_PageHandle page_data; 
    SM_PageHandle slot_data;
    RC rc_return; 
	int record_size;
    
    // get the size of the record
    record_size = getRecordSize(rel->schema);
	
    // pin first free page
	rid->page = rm->first_free_page;
	if((rc_return == pinPage(&rm->bm_handle, &rm->page_handle, rid->page)) != RC_OK){
        return RC_WRITE_FAILED;
    }
	
	// find first free slot
    page_data = rm->page_handle.data;
	rid->slot = find_slot(page_data, record_size);
	
    // set slot data to retrieved page data
    slot_data = page_data;
    
	// mark dirty since we have new page data
	if((rc_return = markDirty(&rm->bm_handle, &rm->page_handle)) != RC_OK){
        return RC_WRITE_FAILED;
    }

    // unpin the page
	if((rc_return = unpinPage(&rm->bm_handle, &rm->page_handle)) != RC_OK){
            return RC_WRITE_FAILED;
    }	
	if((rc_return = pinPage(&rm->bm_handle, &rm->page_handle, rid->page)) != RC_OK){
        return RC_WRITE_FAILED;
    }

    // if page operations worked correctly
    if(rc_return == RC_OK){

        // inserting 'X' to indicate that this is not an empty record
        slot_data += (rid->slot * record_size);
        *slot_data = 'X';
        memcpy(++slot_data, record->data + 1, record_size - 1);

        // increase the number of tuples in the page file
        rm->num_tuples++;

        // free pointers and return success
        free(&rm->page_handle);
        return rc_return;

    }
}

// NAME: deleteRecord
// PURPOSE: The purpose of this function is to delete a specific record given record ID
// PARAMS: 
// - rel: table data struct
// - id: id of record to delete
// RETURN VAL: RC_OK, RC_WRITE_FAILED
extern RC deleteRecord (RM_TableData *rel, RID id){

    // locals
	Record_Manager *rm = rel->mgmtData;
    SM_PageHandle page_data;
    RC rc_return; 
    int record_size;

    // get size of the record we are deleting
    record_size = getRecordSize(rel->schema);

    // pin the page requested to delete
    if((rc_return = pinPage(&rm->bm_handle, &rm->page_handle, id.page)) != RC_OK){
        return RC_WRITE_FAILED;
    }

    // first free page is now pointed to which one we want to delete
	rm->first_free_page = id.page;
	
    // move page_data ptr to correct location
	page_data = rm->page_handle.data;
	page_data += + (id.slot * record_size);
	
	// assign 'O' to indicate this slot has been deleted and is open
	*page_data = 'O';
		
	// mark page dirty since it's contents have been updated
	if((rc_return = markDirty(&rm->bm_handle, &rm->page_handle)) != RC_OK){
        return RC_WRITE_FAILED;
    }
	// unpin page from buffer pool 
	if((rc_return = unpinPage(&rm->bm_handle, &rm->page_handle)) != RC_OK){
        return RC_WRITE_FAILED;
    }

    // return success
	return RC_OK; 
}

// NAME: updateRecord
// PURPOSE: The purpose of this function is to update the data of a record
// PARAMS: 
// - rel: table data struct
// - record: record who's data we are updating
// RETURN VAL: RC_OK, RC_WRITE_FAILED
extern RC updateRecord (RM_TableData *rel, Record *record)
{	
	// locals
	Record_Manager *rm = rel->mgmtData;
    RID rid = record->id;
    SM_PageHandle record_data;
    RC rc_return; 
    int record_size;

    // get record size we want to update
    record_size = getRecordSize(rel->schema);
	
	// pinning the page which has the record which we want to update
	if((rc_return = pinPage(&rm->bm_handle, &rm->page_handle, record->id.page)) != RC_OK){
        return RC_WRITE_FAILED;
    }

	// get the records data
	record_data = rm->page_handle.data;

    // calculating position based on slot and record size
	record_data += (rid.slot * record_size);
	
	// 'X' to indicate this record is not empty
	*record_data = 'X';

    // copy record data into temp record data to save to disk
	memcpy(++record_data, record->data + 1, record_size - 1);
	
    // mark the page dirty since it's contents have been updated
	if((rc_return = markDirty(&rm->bm_handle, &rm->page_handle)) != RC_OK){
        return RC_WRITE_FAILED;
    }

	// unpin page from buffer pool 
	if((rc_return = unpinPage(&rm->bm_handle, &rm->page_handle)) != RC_OK){
        return RC_WRITE_FAILED;
    }
	
    // return success if page functions worked as expected
	return RC_OK;	
}

// NAME: getRecord
// PURPOSE: The purpose of this function is to retrieve the contents of a specific record denoted by 'id'
// PARAMS: 
// - rel: table data struct
// - id: id of the record to retrieve
// - record: record who's data we are copying into
// RETURN VAL: RC_OK, RC_WRITE_FAILED
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	// locals
	Record_Manager *rm = rel->mgmtData;
    SM_PageHandle page_data; 
    SM_PageHandle record_data;
    RC rc_return; 
    int record_size;

    // get the record size given the schema
    record_size = getRecordSize(rel->schema);
	
	// pin the page associated to the record
    if((rc_return = pinPage(&rm->bm_handle, &rm->page_handle, id.page)) != RC_OK){
        return RC_WRITE_FAILED;
    }

    // navigate to the page's data
    page_data = rm->page_handle.data;
	page_data += (id.slot * record_size);

    // update the record id to match the rel ID
    record->id = id;

    // set record data to the data of passed in record
    record_data = record->data;

    // copy the page data into the record data
    memcpy(++record_data, page_data + 1, record_size - 1);
	
	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
    if((rc_return = unpinPage(&rm->bm_handle, &rm->page_handle)) != RC_OK){
        return RC_WRITE_FAILED;
    }

    // return success if page functions worked as expected
	return RC_OK;
}

// NAME: startScan
// PURPOSE: The purpose of this function is to initialize the scan data. 2 pointers are of 'record_manager' types
// they use different parts of the record manager that should not be updated by others. 
// PARAMS: 
// - rel: table data struct
// - scan: bookkeeping for scans
// - cond: value of the scan expression
// RETURN VAL: RC_OK
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // locals
    Record_Manager *sm;
	Record_Manager *tm;
    sm = (Record_Manager*) malloc(sizeof(Record_Manager));

    // initialize scan data
    scan->mgmtData = sm;
    sm->rid.page = 1;       // start scan from page 1
	sm->rid.slot = 0;  	    // start scan from slot 0
	sm->num_scanned = 0;    // 0 num scanned
    sm->condition = cond;   // set condition to parameter condition
    tm = rel->mgmtData;
    tm->num_tuples = rm->num_tuples; // set num tuples to the number of tuples in the record manager
    scan->rel= rel;

    // return success
	return RC_OK;
}

// NAME: next
// PURPOSE: The purpose of this function is to find tuples that belond to a specific scan condition
// PARAMS: 
// - scan: bookkeeping for scans
// - record: value of the scan expression
// RETURN VAL: RC_OK, RC_WRITE_FAILED, RC_RM_NO_MORE_TUPLES
extern RC next (RM_ScanHandle *scan, Record *record)
{
	// locals
	Record_Manager *sm = scan->mgmtData;
	Record_Manager *tm = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;
    SM_PageHandle page_data;
    SM_PageHandle record_data; 
    Value *result = (Value *) malloc(sizeof(Value));
    RC rc_return; 
    int record_size;
    int num_scanned;
    int total_slots;
   
   	
	// Get record size of schema and calculate the total slots in the page file
	record_size = getRecordSize(schema);
    total_slots = PAGE_SIZE / record_size;

	// store off the number of records scanned to local var
	num_scanned = sm->num_scanned;

	// loop through the total number of tuples in the page file
    for(int i = 0; i <= tm->num_tuples; i++){

        // starting position of the record's page and slot number when num_scanned of 0
		if (num_scanned == 0)
		{
			sm->rid.page = 1;
			sm->rid.slot = 0;
		}
		else
		{
            // increment slot to find next tuple
			sm->rid.slot++;

			// scan has reached the end
			if(sm->rid.slot >= total_slots)
			{
				sm->rid.slot = 0;
				sm->rid.page++;
			}
		}

        // pin the page of the found in scan
        if((rc_return = pinPage(&tm->bm_handle, &sm->page_handle, sm->rid.page)) != RC_OK){
            return RC_WRITE_FAILED;
        }
			
		// get pinned page's data	
		page_data = sm->page_handle.data;

		// move pointer to slot location
		page_data += (sm->rid.slot * record_size);
		
		// set the page and slot based off scan manager data
		record->id.page = sm->rid.page;
		record->id.slot = sm->rid.slot;

		// point record_data to the parameters data
		record_data = record->data;

		// mark the page as empty and copy the data retrieved from the pinned page
		*record_data = 'O';
		memcpy(++record_data, page_data + 1, record_size - 1);

		// increment the number of scans in record manager and local variable
		sm->num_scanned++;
		num_scanned++;

		// check scan condition of the retrieved record
		evalExpr(record, schema, sm->condition, &result); 

		// condition met
		if(result->v.boolV == TRUE)
		{
			// unpin the page
            if((rc_return = unpinPage(&tm->bm_handle, &sm->page_handle)) != RC_OK){
                return RC_WRITE_FAILED;
            }
			// return success if page functions worked as expected			
			return RC_OK;
		}
    }
	
	// reset values if we exit scan loop
	sm->rid.page = 1;
	sm->rid.slot = 0;
	sm->num_scanned = 0;
	
	// return error if no tuples matched our condition
	return RC_RM_NO_MORE_TUPLES;
}

// NAME: closeScan
// PURPOSE: The purpose of this function is to shut down the scan
// PARAMS: 
// - scan: bookkeeping for scans
// RETURN VAL: RC_OK
extern RC closeScan (RM_ScanHandle *scan)
{
    // locals
	Record_Manager *sm = scan->mgmtData;
	Record_Manager *rm = scan->rel->mgmtData;
    
    // set scan data to default
    sm->num_scanned = 0;
    sm->rid.page = 1;
    sm->rid.slot = 0;
	
    // free the scan pointer
    free(scan->mgmtData);  
	
    // return success
	return RC_OK;
}

// NAME: getRecordSize
// PURPOSE: The purpose of this function is to retrieve the size of a record given a specified schema
// PARAMS: 
// - schema: schema of the records
// RETURN VAL: record_size
extern int getRecordSize (Schema *schema){

    // locals
	int record_size = 0;

	// loop thru 'numattrs' to get size of each datatype
	for (int i = 0; i < schema->numAttr; i++) {

        // int case
		if (schema->dataTypes[i] == DT_INT) {
			record_size += sizeof(int);
		} 
        // float case
        else if (schema->dataTypes[i] == DT_FLOAT) {
			record_size += sizeof(float);
		} 
        // bool case
        else if (schema->dataTypes[i] == DT_BOOL) {
			record_size += sizeof(bool);
		}
        // string case
        else if (schema->dataTypes[i] == DT_STRING) {
			record_size += schema->typeLength[i];
		}
	}

    // return record size
	return record_size;
} 

// NAME: createSchema
// PURPOSE: The purpose of this function is to initialize a schema
// PARAMS: 
// - numAttr: number of attributes in the schema
// - attrNames: names of the attributes
// - dataTypes: datatypes of the attributes
// - typeLength: length of the data types
// - keySize: size of the schema keys
// - keys: keys
// RETURN VAL: schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

	// Allocate memory space to schema
	Schema *new_schema = (Schema *) malloc(sizeof(Schema));

	// Assign all of the values of the new schema with passed in parameters
	new_schema->numAttr = numAttr;
	new_schema->attrNames = attrNames;
	new_schema->dataTypes = dataTypes;
	new_schema->typeLength = typeLength;
	new_schema->keySize = keySize;
	new_schema->keyAttrs = keys;

    // return the newly created schema
    return new_schema; 
}

// NAME: createSchema
// PURPOSE: The purpose of this function is to destroy a schema
// PARAMS: 
// - schema: schema to set to default values
// RETURN VAL: RC_OK
extern RC freeSchema (Schema *schema){

    // set schema data to default
    schema->numAttr = 0;
	schema->attrNames = NULL;
	schema->dataTypes = NULL;
	schema->typeLength = NULL;
	schema->keyAttrs = NULL;
	schema->keySize = 0;

    // free the schema value
    free(schema);

    // return success
    return RC_OK;
}

// NAME: createRecord
// PURPOSE: The purpose of this function is to create a new record
// PARAMS: 
// - record: record which we will be creating
// - schema: schema which the record will be defined by
// RETURN VAL: RC_OK
extern RC createRecord (Record **record, Schema *schema)
{
	// initialize new record
	Record *temp_record = (Record*) malloc(sizeof(Record));
    SM_PageHandle page_ptr; 
	
	// Retrieve the record size
	int record_size = getRecordSize(schema);

    // Set temp record data
    // Set slot and page to -1, this denotes empty in the buffer pool 
	temp_record->id.page = -1;
    temp_record->id.slot = -1;   

    // allocate space for the empty record
	temp_record->data= (char*) malloc(record_size);

	// new page pointer to point to the new record
	page_ptr = temp_record->data;
	
	// 'O' denotes that this record is empty, append null byte due to 'C' strings
	*page_ptr = 'O';
	*(++page_ptr) = '\0';

	// return the new record
	*record = temp_record;

    // return success
	return RC_OK;
}

// NAME: freeRecord
// PURPOSE: The purpose of this function is to free the memory associated with a specific record
// PARAMS: 
// - record: record who's memory we are freeing
// RETURN VAL: RC_OK
extern RC freeRecord (Record *record){

    // free call to release the memory
    free(record);
    return RC_OK;

}

// NAME: get_attribute_offset
// PURPOSE: The purpose of this function is to provide the offset value of a specified attribute
// PARAMS: 
// - schema: schema which holds the attribute type
// - attrNum: attribute number to get the offset value
// RETURN VAL: attr_offset, RC_GET_ATTR_ERROR
int get_attribute_offset (Schema *schema, int attrNum)
{
    // locals
	int i = 0;
	int attr_offset = 1;

	// loop through all attributes to find our specified attribute
	while (i < attrNum){

        // DT_STRING case
        if(schema->dataTypes[i] == DT_STRING){
            attr_offset += schema->typeLength[i];
        }
        // DT_INT case
		else if(schema->dataTypes[i] == DT_INT){
            attr_offset += sizeof(int);
        }
        // DT_FLOAT case
        else if(schema->dataTypes[i] == DT_FLOAT){
            attr_offset += sizeof(float);
        }
        // DT_BOOL case
        else if(schema->dataTypes[i] == DT_BOOL){
            attr_offset += sizeof(bool);
        }
        // no match, return error
        else{
            return RC_GET_ATTR_ERROR;
        }
        i++;
	}

	return attr_offset;
}

// NAME: getAttr
// PURPOSE: The purpose of this function is to retrieve a specific attribute in a record
// PARAMS: 
// - record: record to retrieve attribute from
// - schema: schema which holds the definition of the record
// - attrNum: number of the attribute in the schema to retrieve
// - value: value to write the attribute data to
// RETURN VAL: RC_OK, RC_GET_ATTR_ERROR
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    // locals
	int offset = 0;
    DataType *data_type = schema->dataTypes;
    Value *attr = (Value*) malloc(sizeof(Value));
    SM_PageHandle record_ptr = record->data;

	// Get offset of the attribute
	offset = get_attribute_offset(schema, attrNum);
	
	// move ptr to offset position
	record_ptr += offset;
	
	// DT_STRING case
    if(data_type[attrNum] == DT_STRING){

        // get the length for the string
        int type_length = schema->typeLength[attrNum];
        
        // allocate enough space for a string type
        attr->v.stringV = (SM_PageHandle) malloc(type_length + 1);
        strncpy(attr->v.stringV, record_ptr, type_length);

        // add null byte at end of string due to 'C' conventions
        attr->v.stringV[type_length] = '\0';
        attr->dt = DT_STRING;
    }

    // DT_INT case
    else if(data_type[attrNum] == DT_INT){

        int i = 0;
        memcpy(&i, record_ptr, sizeof(int));
        attr->v.intV = i;
        attr->dt = DT_INT;
    }

    // DT_FLOAT case
    else if(data_type[attrNum] == DT_FLOAT){

        float f;
        memcpy(&f, record_ptr, sizeof(float));
        attr->v.floatV = f;
        attr->dt = DT_FLOAT;

    }
    // DT_BOOL case
    else if(data_type[attrNum] == DT_BOOL){

        bool b;
        memcpy(&b,record_ptr, sizeof(bool));
        attr->v.boolV = b;
        attr->dt = DT_BOOL;

    }
    else{ // return error
        return RC_GET_ATTR_ERROR;
	}

    // set input parameter value to our attribute
	*value = attr;
	return RC_OK;

}

// NAME: getAttr
// PURPOSE: The purpose of this function is to set the value of a specified attribute
// PARAMS: 
// - record: record to write the attribute value to
// - schema: schema which holds the definition of the record
// - attrNum: number of the attribute in the schema to write
// - value: value to write the attribute data to
// RETURN VAL: RC_OK, RC_GET_ATTR_ERROR
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    // locals
	int offset = 0;
    SM_PageHandle record_ptr = record->data;
    DataType *data_type = schema->dataTypes;

	// get offset of the attribute
	offset = get_attribute_offset(schema, attrNum);

    // add offset to make space for attr value
	record_ptr += offset;
	
    // DT_STRING case
	if(data_type[attrNum] == DT_STRING){

        int type_length = schema->typeLength[attrNum];
        strncpy(record_ptr, value->v.stringV, type_length);

    }
    // DT_INT case
	else if(data_type[attrNum] == DT_INT){

        // cast record pointer as an integer and set it's value
        *(int *) record_ptr = value->v.intV;	  

    }
	// DT_FLOAT case	
	else if(data_type[attrNum] == DT_FLOAT){

        // cast record pointer as a float and set it's value
        *(float *) record_ptr = value->v.floatV;

	}
	// DT_BOOL case
	else if(data_type[attrNum] == DT_BOOL){

        // cast record pointer as a boolean and set it's value
        *(bool *) record_ptr = value->v.boolV;

    }

    else{ // return error
        return RC_SET_ATTR_ERROR;
	}		

    // return success if no errors were thrown
	return RC_OK;
}
