//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <stdio.h>

extern int g_io_number_of_partitions;
extern int g_io_number_of_files;
extern int g_io_partitions_per_rank;

// length = g_io_number_of_files
extern FILE ** g_io_files;

typedef struct {
	int file;
	int offset;
	int size;
	int lp_count;
	int event_count;
} io_partition;

// length = g_io_number_of_partitions
extern io_partition * g_io_partitions;

// API Functions
void io_init(int num_files, int num_partitions);
void io_final();
void io_read_master_header(char * master_filename);
void io_write_master_header(char * master_filename);

