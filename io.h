//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <stdio.h>

extern char * g_io_master_filename;
extern int g_io_number_of_partitions;
extern int g_io_number_of_files;

// length = g_io_number_of_files
extern FILE ** g_io_files;

typedef struct {
	int file;
	int offest;
	int size;
	int lp_count;
	int event_count;
} io_partition;

// length = g_io_number_of_partitions
extern io_partition * g_io_partitions;

void io_read_master_header(char * path, char * filename);
void io_write_master_header(char * check_point_name);

