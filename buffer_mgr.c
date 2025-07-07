#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include <stdlib.h>
 
//global vars

#define QUEUE_EMPTY 0

typedef struct Page_Frame{
	PageNumber page_num;  // page number of the frame
	int page_dirty;       // indicates that this page is modified
	int fix_count;        // how many users are currently reading this page
	int num_hit;        // how many users are currently reading this page
	SM_PageHandle contents;       // data-contents of the page
} Page_Frame;

typedef struct {
    Page_Frame *Page_Frame; 
    int head, tail, num_entries, max_entries, num_files_written, num_files_read;
} Queue; 


// NAME: initBufferPool
// PURPOSE: initialize all elements of the buffer pool
// PARAMS: 
// - bm: buffer pool 
// - pageFileName: name of the file we will be writing to
// - numPages: number of pages in our buffer pool
// - strategy - replacement strategy
// - stratData - page handle to store read data into memory
// RETURN VAL: RC_OK
RC initBufferPool(BM_BufferPool *const bm, 
					const char *const pageFileName, 
					const int numPages, 
					ReplacementStrategy strategy, 
					void *stratData){

	// allocate empty buffer of size 'Page_Size'

	Queue *q = (Queue *) malloc(sizeof(Queue));

	q->Page_Frame = malloc(sizeof(Page_Frame)*(numPages));
	q->max_entries = numPages;
	q->head = 0;
	q->tail = 0; 
	q->num_entries = 0; 

	for(int i = 0; i < numPages; i++){
		q->Page_Frame[i].page_num = -1; 
		q->Page_Frame[i].page_dirty = 0; 
		q->Page_Frame[i].fix_count = 0; 
		q->Page_Frame[i].num_hit = 0; 
		q->num_files_written = 0;
		q->num_files_read = 0;
		q->Page_Frame[i].contents = NULL; 
	}

	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->mgmtData = q; 

	return RC_OK; 

}

// check if queue is empty
bool queue_empty(Queue *q){
    return (q->num_entries == 0);
}

// remove page frame from pool
bool dequeue(Queue *q, BM_BufferPool *const bm){

    SM_FileHandle fh;

    if(queue_empty(q)){
        return QUEUE_EMPTY; 
    }

	if(q->Page_Frame[q->head].page_num != -1 && q->Page_Frame[q->head].fix_count == 0){

		openPageFile(bm->pageFile, &fh);
		writeBlock(q->Page_Frame[q->head].page_num, &fh, q->Page_Frame[q->head].contents);
		fclose(fh.mgmtInfo);
		q->Page_Frame[q->head].page_dirty = 0; 

		q->head = (q->head + 1) % q->max_entries; 
		q->num_entries--;

		return true; 

	}

	else{

		q->tail = (q->tail + 1) % q->max_entries; 
		q->head = (q->head + 1) % q->max_entries; 

		return false; 
	}

}

// add page frame to pool
bool enqueue(Queue *q, 
				PageNumber pageNum, 
				Page_Frame new_frame){

    q->Page_Frame[q->tail] = new_frame; 
	q->Page_Frame[q->tail].fix_count++; 
    q->num_entries++; 
    q->tail = (q->tail+1) % q->max_entries;

    return true; 

}

// NAME: FIFO
// PURPOSE: FIFO page replacement strategy for buffer pool
// PARAMS: 
// - bm: active buffer pool
// - page: page we will be writing to disk
// - pageNum: page number
// RETURN VAL: true, false
bool FIFO(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum){

	// Get the queue handle
	Queue *q = (Queue *)bm->mgmtData;

	// instantiate new page frame
	Page_Frame new_frame;

	// file handle
	SM_FileHandle fh;

	// pages needed to ensure capacity
	int pages_needed = pageNum + 1;

	// dequeue flags
	bool dequeue_success; 
	int dequeue_attempts = 0; 

	// initialize new page data
	new_frame.page_num = pageNum;
	new_frame.contents = (SM_PageHandle)malloc(PAGE_SIZE);
	new_frame.fix_count = 0; 
	new_frame.page_dirty = 0;

	//open file to read data from
	openPageFile(bm->pageFile, &fh);

	//ensure file has enough capacity for the frame
	ensureCapacity(pages_needed, &fh);

	if(readBlock(pageNum, &fh, new_frame.contents) == RC_OK){

		page->data = new_frame.contents;
		page->pageNum = pageNum;

		for(int i = 0; i < q->max_entries; i++){
			
			if(q->Page_Frame[i].page_num == pageNum){

				// page found, return contents
				page->data = q->Page_Frame[i].contents;
				q->Page_Frame[i].fix_count++; 

				// close file
				fclose(fh.mgmtInfo);

				return true;  

			}
			else if(q->Page_Frame[i].page_num == -1){

				// fill empty slots
				enqueue(q, pageNum, new_frame);

				// increment files read
				q->num_files_read++;

				// close numbers of files read
				fclose(fh.mgmtInfo);

				return true; 

			}
		}
		
		while ((dequeue_success = dequeue(q, bm)) != true){

			printf("Can't dequeue! moving to next position in buffer");

			// if we have gone through the whole buffer without a successful dequeue, exit program
			if(dequeue_attempts > q->max_entries){

				printf("Could not remove pages from the buffer, please unpin a page to continue");
				return false;

			}
			dequeue_attempts++; 
		}

		if(enqueue(q, pageNum, new_frame)){

			// after enqueue, reset dequeue attempts
			dequeue_attempts = 0; 

			q->num_files_read++;

			//close instance of page file to prevent memory leak
			fclose(fh.mgmtInfo);
			return true; 

		}
		else{

			// enqueue was not successful, close file and return error
			fclose(fh.mgmtInfo);
			return false; 

		}
	}

	else{
		return false; 
	}
}

// NAME: LRU
// PURPOSE: LRU page replacement strategy for buffer pool
// PARAMS: 
// - bm: active buffer pool
// - page: page we will be writing to disk
// - pageNum: page number
// RETURN VAL: true, false
bool LRU(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum){

	// Get the queue handle
	Queue *q = (Queue *)bm->mgmtData;

	// instantiate new page frame
	Page_Frame new_frame;

	// file handle
	SM_FileHandle fh;

	// pages needed to ensure capacity
	int pages_needed = pageNum + 1;

	// initialize new page data
	new_frame.page_num = pageNum;

	new_frame.contents = (SM_PageHandle)malloc(PAGE_SIZE);

	new_frame.fix_count = 0; 

	new_frame.page_dirty = 0;

	new_frame.num_hit++;

	//open file to read data from
	openPageFile(bm->pageFile, &fh);

	//ensure file has enough capacity for the frame
	ensureCapacity(pages_needed, &fh);

	if(readBlock(pageNum, &fh, new_frame.contents) == RC_OK){

		page->data = new_frame.contents;
		page->pageNum = pageNum;

		for(int i = 0; i < q->max_entries; i++){
			
			if(q->Page_Frame[i].page_num == pageNum){

				page->data = q->Page_Frame[i].contents;
				q->Page_Frame[i].fix_count++; 
				fclose(fh.mgmtInfo);
				return true;  

			}
			else if(q->Page_Frame[i].page_num == -1){

				enqueue(q, pageNum, new_frame);

				q->num_files_read++;

				fclose(fh.mgmtInfo);
				return true; 

			}
		}
	}
	else{
		return false; 
	}
}

// NAME: shutdownBufferPool
// PURPOSE: frees up all memory associated with the buffer pool 
// PARAMS: 
// - bm: buffer pool to shut down
// RETURN VAL: RC_OK, RC_PAGE_NOT_FOUND
RC shutdownBufferPool(BM_BufferPool *const bm){

	Queue *q = (Queue *)bm->mgmtData;

	// write all dirty pages back to disk
	forceFlushPool(bm);

	for(int i = 0; i < q->max_entries; i++)
	{
		// If fixCount != 0, it means that the contents of the page was modified by some client and has not been written back to disk.
		if(q->Page_Frame[i].fix_count > 0)
		{
			// return error code
			return RC_PAGE_NOT_FOUND;
		}
	}

	// Releasing space occupied by the page
	free(q);
	bm->mgmtData = NULL;
	return RC_OK; 

}

// NAME: forceFlushPool
// PURPOSE: Writes all dirty pages back to disk 
// PARAMS: 
// - bm: buffer pool to shut down
// RETURN VAL: RC_OK
RC forceFlushPool(BM_BufferPool *const bm){

	// Page_Frame *pf = (Page_Frame *) bm->mgmtData;
	Queue *q = (Queue *)bm->mgmtData;
	SM_FileHandle fh; 

	for (int i = 0; i < q->max_entries; i++){

		//find the page the user is requesting
		if (q->Page_Frame[i].page_dirty != 0){

			//decrement fix count
			q->Page_Frame[i].page_dirty = 0; 
			openPageFile(bm->pageFile, &fh);
			writeBlock(q->Page_Frame[i].page_num, &fh, q->Page_Frame[i].contents);
			fclose(fh.mgmtInfo);
			q->num_files_written++;
		}
	} 

	return RC_OK; 
}

// NAME: markDirty
// PURPOSE: marks specified page number as dirty
// PARAMS: 
// - bm: active buffer pool
// - page: page we will be marking dirty
// RETURN VAL: RC_OK
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	
	// Page_Frame *pf = (Page_Frame *) bm->mgmtData;
	Queue *q = (Queue *)bm->mgmtData;

	for (int i = 0; i < q->max_entries; i++){

		// if page isn't empty
		if (q->Page_Frame[i].page_num != -1 && q->Page_Frame[i].page_num == page->pageNum){

			q->Page_Frame[i].page_dirty = 1; 

			return RC_OK; 
		}
	}

	return RC_OK; 

}

// NAME: unpinPage
// PURPOSE: decrements the fix count from specifed page
// PARAMS: 
// - bm: active buffer pool
// - page: page we will be unpinning
// RETURN VAL: RC_OK
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){

	// Page_Frame *pf = (Page_Frame *) bm->mgmtData;
	Queue *q = (Queue *)bm->mgmtData;

	for (int i = 0; i < q->max_entries; i++){
		//find the page the user is requesting
		// if (pf[i].page_num == page->pageNum){
		if (q->Page_Frame[i].fix_count > 0 && q->Page_Frame[i].page_num == page->pageNum){

			//decrement fix count
			q->Page_Frame[i].fix_count--; 

		}
	} 
	return RC_OK;
}

// NAME: forcePage
// PURPOSE: forces a page to be written to the disk
// PARAMS: 
// - bm: active buffer pool
// - page: page we will be writing to disk
// RETURN VAL: RC_OK, RC_FILE_NOT_FOUND
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){

	// Page_Frame *pf = (Page_Frame *) bm->mgmtData;
	Queue *q = (Queue *)bm->mgmtData;
	SM_FileHandle fh; 

	for (int i = 0; i < q->max_entries; i++){

		//find the page the user is requesting
		if (q->Page_Frame[i].page_num == page->pageNum){

			//open page file
			openPageFile(bm->pageFile, &fh);

			// write the contents of the page
			writeBlock(q->Page_Frame[i].page_num, &fh, q->Page_Frame[i].contents);

			//close instance of page file to prevent memory leak
			fclose(fh.mgmtInfo);

			// mark page as clean
			q->Page_Frame[i].page_dirty = 0;  

			// increment the number of files written
			q->num_files_written++; 

			return RC_OK;
		}
	} 
	return RC_FILE_NOT_FOUND;
}

// NAME: pinPage
// PURPOSE: Pins page and fills up the buffer
// PARAMS: 
// - bm: active buffer pool
// - page: page handle
// - pageNum: page that we will be pinning
// RETURN VAL: RC_OK
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum){

	if(bm->strategy == RS_FIFO){
		if(FIFO(bm, page, pageNum)){
			
			return RC_OK; 

		}
		else{

			return RC_FILE_NOT_FOUND;

		}
	}
	else if(bm->strategy == RS_LRU){
		if(LRU(bm, page, pageNum)){

			return RC_OK;

		}
		else{

			return RC_FILE_NOT_FOUND;

		}
	}

	else{
		return RC_FILE_NOT_FOUND;
	}

}

// NAME: getFrameContents
// PURPOSE: Returns the pages found in the frames of the buffer pool
// PARAMS: 
// - bm: active buffer pool
// RETURN VAL: page numbers
PageNumber *getFrameContents (BM_BufferPool *const bm){

	PageNumber *page_numbers = malloc(sizeof(int)*bm->numPages);
	Queue *q = (Queue *)bm->mgmtData;

	for (int i = 0; i < q->max_entries; i++){

		//if page info is not null
		if(q->Page_Frame[i].page_num != -1){

			page_numbers[i] = q->Page_Frame[i].page_num; 

		}
		//otherwise return NO_PAGE
		else{

			page_numbers[i] = NO_PAGE;

		}
	}
	//return pagenumber array
	return page_numbers; 
}

// NAME: getDirtyFlags
// PURPOSE: Returns the pages that are marked dirty in the buffer pool
// PARAMS: 
// - bm: active buffer pool
// RETURN VAL: page numbers
bool *getDirtyFlags (BM_BufferPool *const bm){
	
	bool *dirty_flags = malloc(sizeof(bool)*bm->numPages);
	Queue *q = (Queue *)bm->mgmtData;

	for (int i = 0; i < q->max_entries; i++){

		if(q->Page_Frame[i].page_dirty != 0){

			dirty_flags[i] = true; 

		}
		else{

			dirty_flags[i] = false;

		}
	}
	//return dirty flags array
	return dirty_flags; 
}

// NAME: getFixCounts
// PURPOSE: Returns the number of users who are currently editing the pages
// PARAMS: 
// - bm: active buffer pool
// RETURN VAL: the number of users who are currently editing the pages
int *getFixCounts (BM_BufferPool *const bm){

	int *fix_counts = malloc(sizeof(int)*bm->numPages);
	Queue *q = (Queue *)bm->mgmtData;

	int i = 0;

	// Iterating through all the pages in the buffer pool and setting fixCounts' value to page's fixCount
	for (int i = 0; i < q->max_entries; i++){
	
		fix_counts[i] = (q->Page_Frame[i].fix_count != -1) ? q->Page_Frame[i].fix_count : 0;

	}
	return fix_counts; 
}

// NAME: getNumReadIO
// PURPOSE: Returns the number of pages that have been read from the disk
// PARAMS: 
// - bm: active buffer pool
// RETURN VAL: the number of total reads
int getNumReadIO (BM_BufferPool *const bm){
	Queue *q = (Queue *)bm->mgmtData;
	return q->num_files_read; 
}

// NAME: getNumWriteIO
// PURPOSE: Returns the number of pages that have been written to the disk
// PARAMS: 
// - bm: active buffer pool
// RETURN VAL: the number of total writes
int getNumWriteIO (BM_BufferPool *const bm){
	Queue *q = (Queue *)bm->mgmtData;
	return q->num_files_written; 
}