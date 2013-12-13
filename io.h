//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <stdio.h>

extern char * g_io_master_filename;
extern int g_io_number_of_partitions;
extern int g_io_number_of_files;

// length = g_io_number_of_files
extern FILE* g_io_files[];

// length = g_io_number_of_partitions
extern int g_io_partition_file[];
extern int g_io_partition_offest[];
extern int g_io_partition_size[];
extern int g_io_partition_lp_count[];
extern int g_io_partition_event_count[];

void io_read_master_header(char * filename);
void io_write_master_header(char * check_point_name);

