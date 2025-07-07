CC=gcc
CFLAGS=-I.
DEPS = dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h expr.h record_mgr.h rm_serializer.h 

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign3: dberror.o test_assign3_1.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o
	$(CC) -o test_assign3 dberror.o test_assign3_1.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o

# test_expr: dberror.o test_expr.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o
# 	$(CC) -o test_expr dberror.o test_expr.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o