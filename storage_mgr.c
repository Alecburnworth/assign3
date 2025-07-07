#include "storage_mgr.h"
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

SM_FileHandle File_Handle; 
SM_PageHandle Page_Handle; 

// NAME: initStorageManager
// PURPOSE: initialize disk handle variables
// PARAMS: none
// RETURN VAL: none
void initStorageManager (void) {

    // clear all handle data
    File_Handle.fileName = ""; 
    File_Handle.totalNumPages = 0;
    File_Handle.curPagePos = 0;
    File_Handle.mgmtInfo = NULL;

}

// NAME: createPageFile
// PURPOSE: creates new binary file
// PARAMS: 
// fileName - file to be created
// RETURN VAL: RC_OK or RC_FILE_NOT_FOUND
RC createPageFile (char *fileName) {

    // Create file
    File_Handle.mgmtInfo = fopen (fileName, "wb");

    // if file was created successfully, fill rest of struct
    if (File_Handle.mgmtInfo != NULL) {

        // allocate empty buffer of size 'Page_Size'
        Page_Handle = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));

        // write the empty page
        fwrite(Page_Handle, sizeof(char), PAGE_SIZE, File_Handle.mgmtInfo);

        int seek;
        seek = fseek(File_Handle.mgmtInfo, 0, SEEK_END);

        // set the file handle data
        File_Handle.fileName = fileName;
        File_Handle.curPagePos = 0;  
        File_Handle.totalNumPages = 1;

        //cleanup
        free(Page_Handle);
        fclose(File_Handle.mgmtInfo);
        return RC_OK;
    }
    // file not found
    else{
        return RC_FILE_NOT_FOUND;
    }
}

// NAME: openPageFile
// PURPOSE: opens specified file
// PARAMS: 
// fileName - file to be created
// fHandle - file handle for memory
// RETURN VAL: RC_OK or RC_FILE_NOT_FOUND
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {

    // open file
    File_Handle.mgmtInfo = fopen (fileName, "rb+");

    // check if file was opened correctly
    if (File_Handle.mgmtInfo != NULL) {

        // set the parameter to File_Handle data
        fHandle->fileName = File_Handle.fileName;
        fHandle->curPagePos = File_Handle.curPagePos;
        fHandle->totalNumPages = File_Handle.totalNumPages;
        fHandle->mgmtInfo = File_Handle.mgmtInfo;

        return RC_OK; 
    }

    // file not found
    else{
        return RC_FILE_NOT_FOUND;
    }
}

// NAME: closePageFile
// PURPOSE: close the currently opened file
// fHandle - file handle for memory
// RETURN VAL: RC_OK or RC_FILE_NOT_FOUND
RC closePageFile (SM_FileHandle *fHandle) {

    // close the file
    if(fclose(File_Handle.mgmtInfo) != 0){
      return RC_FILE_NOT_FOUND;
    }
    else{
        return RC_OK; 
    }

}

// NAME: destroyPageFile
// PURPOSE: delete file from disk
// PARAMS: 
// fileName - file to be deleted
// RETURN VAL: RC_OK or RC_FILE_NOT_FOUND
RC destroyPageFile (char *fileName) {

    // close page file before removing
    closePageFile(File_Handle.mgmtInfo);

    // Attempt to delete the file
    if (remove(fileName) == 0) {
        // printf("File Deleted!\n");
        return RC_OK;
    } 
    // file doesn't exist
    else {
        // printf("File not found Deleted!\n");
        return RC_FILE_NOT_FOUND;
    } 
}

// NAME: readBlock
// PURPOSE: read data from specified page
// PARAMS: 
// - pageNum: denotes which page to read 
// data from, note - this file is 0 indexed
// so the user must pass in n-1 pages 
// ie. to read page 1, pass in 0
// for 5, pass in 4 etc
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_READ_NON_EXISTING_PAGE, RC_SEEK_FAIL
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    
    // define local vars
    int offset = pageNum * PAGE_SIZE; //calculate which byte to start on for writing
    int seek; 
    int read; 
    
    if (pageNum + 1 > File_Handle.totalNumPages || pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }
    else{
        // move pointer to offset calculation
        seek = fseek(File_Handle.mgmtInfo, offset, SEEK_SET);
        if (seek == 0){

            // read data from offset position
            read = fread(memPage, sizeof(char), PAGE_SIZE, File_Handle.mgmtInfo);

            // printf("mempage %ld\n", memPage);

            // check that fread read all elements in the page
            if (read == PAGE_SIZE){
                File_Handle.curPagePos = pageNum;
                fHandle->curPagePos = File_Handle.curPagePos;
                return RC_OK;
            }

            // page doesn't exist
            else{
                return RC_READ_NON_EXISTING_PAGE; 
            }
        }
        // seek fail
        else{
            return RC_SEEK_FAIL;
        }
    }
}

// NAME: getBlockPos
// PURPOSE: retrieve the open file's current page
// position
// PARAMS:
// - fHandle - file handle for memory
// RETURN VAL: current page position
int getBlockPos (SM_FileHandle *fHandle){
    return File_Handle.curPagePos;
}

// NAME: readFirstBlock
// PURPOSE: read data from first page
// PARAMS: 
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_READ_NON_EXISTING_PAGE, RC_SEEK_FAIL
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    // define local vars
    int seek; 
    int read; 

    // move pointer to beginning of file
    seek = fseek(File_Handle.mgmtInfo, 0, SEEK_SET);

    if (seek == 0){

        // read data from beginning of the file till the end of the 
        // page
        read = fread(memPage, sizeof(char), PAGE_SIZE, File_Handle.mgmtInfo);

        // check that fread read all elements in the page
        if (read == PAGE_SIZE){
            return RC_OK;
        }
        else{
            return RC_READ_NON_EXISTING_PAGE; 
        }
    }
    // could not move pointer to desired location
    else{
        return RC_SEEK_FAIL; 
    }
}

// NAME: readPreviousBlock
// PURPOSE: read data from previous page relative to the current
// page position
// PARAMS: 
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_READ_NON_EXISTING_PAGE, RC_SEEK_FAIL
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    // define local vars
    int pageNum = File_Handle.curPagePos - 1;
    
    // check that page number is within range
    if (pageNum > File_Handle.totalNumPages || pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }
    else{
        // call read block with previous page position
        return readBlock(pageNum, fHandle, memPage);
    }
}

// NAME: readCurrentBlock
// PURPOSE: read data from current page position
// PARAMS: 
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_READ_NON_EXISTING_PAGE, RC_SEEK_FAIL
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    // define local vars
    int pageNum = File_Handle.curPagePos;
    
    // check that page number is within range
    if (pageNum > File_Handle.totalNumPages || pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }
    else{
        // call read block with current file position
        return readBlock(pageNum, fHandle, memPage);
    }
    
}

// NAME: readNextBlock
// PURPOSE: read data from next page relative to 
// current page position
// PARAMS: 
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_READ_NON_EXISTING_PAGE, RC_SEEK_FAIL
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    // define local vars
    int pageNum = File_Handle.curPagePos + 1;
    
    if (pageNum > File_Handle.totalNumPages || pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }
    else{
        // call read block with pageNum equal to 
        // the next page
        return readBlock(pageNum, fHandle, memPage);
    }
}

// NAME: readLastBlock
// PURPOSE: read data from last page of the file
// PARAMS: 
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_READ_NON_EXISTING_PAGE, RC_SEEK_FAIL
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
        // define local vars
    int seek; 
    int read; 

    // move pointer to beginning of file
    seek = fseek(File_Handle.mgmtInfo, (-PAGE_SIZE), SEEK_END);

    // valid seek
    if (seek == 0){

        // read data from beginning of the file till the end of the 
        // page
        read = fread(memPage, sizeof(char), PAGE_SIZE, File_Handle.mgmtInfo);

        // check that fread read all elements in the page
        if (read == PAGE_SIZE){
            return RC_OK;
        }
        // return error
        else{
            return RC_READ_NON_EXISTING_PAGE; 
        }
    }
    // could not move pointer to desired location
    else{
        return RC_SEEK_FAIL; 
    }
}

// NAME: writeBlock
// PURPOSE: write data to a specified page
// PARAMS: 
// - pageNum: denotes which page to write 
// data to, note - this file is 0 indexed
// so the user must pass in n-1 pages 
// ie. to read page 1, pass in 0
// for 5, pass in 4 etc
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_PAGE_NOT_FOUND, RC_SEEK_FAIL
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    
    // define local vars
    int offset = pageNum * PAGE_SIZE; //calculate which byte to start on for writing
    int seek; 
    int write_elems;   

    // ensure page number is within range
    if(pageNum + 1 > File_Handle.totalNumPages){
        return RC_PAGE_NOT_FOUND; 
    }
    else{

        // move pointer to the offset
        seek = fseek(File_Handle.mgmtInfo, offset, SEEK_SET);

        // valid seek
        if (seek == 0){
            write_elems = fwrite(memPage, sizeof(char), PAGE_SIZE, File_Handle.mgmtInfo);
            return RC_OK; 
        }
        // could not move pointer to desired location
        else{
            return RC_SEEK_FAIL;
        }
    }
}

// NAME: writeCurrentBlock
// PURPOSE: write data to the current page position
// PARAMS: 
// - fHandle - file handle for memory
// - memPage - page handle to store read data into memory
// RETURN VAL: RC_OK, RC_PAGE_NOT_FOUND, RC_SEEK_FAIL
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    // define local vars
    int pageNum = File_Handle.curPagePos; 
    
    // call write function with pageNum equal to current position
    return writeBlock(pageNum, fHandle, memPage);

}

// NAME: appendEmptyBlock
// PURPOSE: add an empty block of data of PAGE_SIZE to the
// end of the file
// PARAMS: 
// - fHandle - file handle for memory
// RETURN VAL: RC_OK, RC_SEEK_FAIL
RC appendEmptyBlock (SM_FileHandle *fHandle) {
    int seek; 
    int write_elems; 

    // allocate empty buffer of size 'Page_Size'
    Page_Handle = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));

    // move pointer to end of file
    seek = fseek(File_Handle.mgmtInfo, 0, SEEK_END);

    // valid seek
    if (seek == 0){

        // write the empty page
        write_elems = fwrite(Page_Handle, sizeof(char), PAGE_SIZE, File_Handle.mgmtInfo);

        // printf("WRITE elements from append: %i", write_elems);

        File_Handle.totalNumPages = File_Handle.totalNumPages + 1; 
        fHandle->totalNumPages = File_Handle.totalNumPages; 
    }
    // could not move pointer to desired location
    else{
        return RC_SEEK_FAIL; 
    }
    
    // clear buffer
    free(Page_Handle);

    // return okay if error hasn't been raised
    return RC_OK;
}

// NAME: ensureCapacity
// PURPOSE: ensure the file contains enough pages
// based on input paramenter 'numberOfPages', 
// if not, increase the files capacity to support
// the page amount
// PARAMS: 
// - numberOfPages: specifies the number of pages
// the user is checking capacity for
// - fHandle - file handle for memory
// RETURN VAL: RC_OK, RC_SEEK_FAIL
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {

    //define locals
    int pages_needed; 
    int buffer_size;
    int seek;

    // check if the current capacity can support the 
    // user's request
    if (numberOfPages <= File_Handle.totalNumPages){
        return RC_OK;
    }
    // create desired pages
    else{

        // move pointer to end of file
        seek = fseek(File_Handle.mgmtInfo, 0, SEEK_END);

        // valid seek
        if (seek == 0){

            // calculate number of pages to write
            pages_needed = numberOfPages - File_Handle.totalNumPages; 

            // calculate number of bytes in buffer
            buffer_size = pages_needed * PAGE_SIZE;

            // allocate buffer space of for buffer elements
            Page_Handle = (SM_PageHandle) calloc(buffer_size, sizeof(char));

            // write the empty page
            int n = fwrite(Page_Handle, sizeof(char), buffer_size, File_Handle.mgmtInfo);

            // increase total page count to that we added
            File_Handle.totalNumPages += pages_needed; 
            fHandle->totalNumPages = File_Handle.totalNumPages; 

            // clear buffer
            free(Page_Handle);
            
            return RC_OK; 
        }
        // could not move pointer to desired location
        else{
            return RC_SEEK_FAIL;
        }
    }
}