EXECUTING: To run this program, compile the code via make file. To test the test_assign3 code, you must uncomment the 'test_assign3' block and leave the 'test_expr' block commented out, then run make. This will create your test_assign3 exe. To test the expr.c, you must comment out the 'test_assign3' block and uncomment the test_expr block.  

ABOUT THE SOLUTION: This program is the implementation to supply a user a record manager that allows them to store records onto pages on disk. Each page file is called a 'Table' which has a schema associated with it like one would in SQL. The user can write many records per page in the file. 

IMPLEMENTATION: To utilitze the member functions of this file, you must call the initRecordManager, createTable and openTable. Once these are executed, you can call the member functions createRecord, insertRecord, updateRecord so on and so forth. 

Main member functions that the user will interact with: 
- createRecord - allows the user to create a new record to write to a page file. When this function is called, the new record is denoted with an 'O' meaning that this record is open to be written on.
- updateRecord - allows the user to update a specified record by RID, when this update happens, the record is marked with an 'X', meaning this record is not free to be written over. 
- insertRecord - allows the user insert a record of their choice. Similar to update record, when the user calls this, the record they are trying to insert gets marked with an 'X' meaning that the slot this record is held in does not get to be modified. 
- deleteRecord - allows the user to delete a record, once this delete occurs, the associated slot is marked with an 'O' to denote that it is open to be written. 
- shutdownBufferPool - this function is called to fully clear out the buffer pool. This will destroy all memory associated with the pool as well as write all pages to disk.
- next - this function is a part of the scan implementation. The user will pass in a condition and this function will scan the page file to return the tuples which meet it.

Other:
- Several helper functions were made to alleviate some of the clutter these functions can gather with the amount of computation needed. These functions include create_table_info_page, find_slot and get_attribute_offset. These are not meant to be interfaced by the user directly. 

CONTRIBUTORS:

Alec Burnworth: author, presenter, implementer
