//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "io.h"

// Null Initializations
FILE ** g_io_files;
io_partition * g_io_partitions;

// Default Values
g_io_number_of_files = 0;
g_io_number_of_partitions = 0;
g_io_partitions_per_rank = 0;

// local init flag (space has been allocated)
int l_init_flag = 0;

void io_init(int num_files, int num_partitions) {
	g_io_number_of_files = num_files;
	g_io_number_of_partitions = num_partitions;

	g_io_files = (FILE **) calloc(g_io_number_of_files, sizeof(FILE*));
    g_io_partitions = (io_partition *) calloc(g_io_number_of_partitions, sizeof(io_partition));

    l_init_flag = 1;
}

void io_final() {
	g_io_number_of_files = 0;
	g_io_number_of_partitions = 0;

	free(g_io_files);
	free(g_io_partitions);
	
	l_init_flag = 0;
}

void io_read_master_header(char * master_filename) {
    FILE * master_header = fopen(master_filename, "r");
    assert(master_header && "Can not open master header to read\n");

    int num_files = 0;
    int num_partitions = 0;
    fscanf(master_header, "%d %d", &num_files, &num_partitions);

    if (!l_init_flag) {
    	io_init(num_files, num_partitions);
    }

    assert(num_files == g_io_files && "Error: Master Header indicated a different number of files than was previously allocated\n");
    assert(num_partitions == g_io_partitions && "Error: Master Header indicated a different number of partitions than was previouly allocated\n");

	int i;
	while (fscanf(master_header, "%d", &i) != EOF) {
		fscanf(master_header, "%d %d %d %d %d", &(g_io_partitions[i].file), &(g_io_partitions[i].offset), &(g_io_partitions[i].size), &(g_io_partitions[i].lp_count), &(g_io_partitions[i].event_count));
	}

	fclose(master_header);
}

void io_write_master_header(char * master_filename) {

	FILE * master_header = fopen(master_filename, "w");
	assert(master_header && "Can not open master header to write\n");

	fprintf(master_header, "%d %d\n", g_io_number_of_files, g_io_number_of_partitions);
	int i;
	for (i = 0; i < g_io_number_of_partitions; i++) {
		fprintf(master_header, "%d %d %d %d %d %d\n", i, g_io_partitions[i].file, g_io_partitions[i].offset, g_io_partitions[i].size, g_io_partitions[i].lp_count, g_io_partitions[i].event_count);
	}

	fclose(master_header);
}

void io_load_checkpoint(char * master_filename) {
	int self, number_of_mpitasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &self);
	MPI_Comm_size(MPI_COMM_WORLD, &number_of_mpitasks);

	if (self == 0) {
		// Open master header file
		FILE * master_header = fopen(master_filename, "r");
		assert(master_header && "MPI_Task 0 can not open master header to write checkpoint\n");

		// Read first line for global vars
		fscanf(master_header, "%d %d", &g_io_number_of_files, &g_io_number_of_partitions);
		g_io_partitions_per_rank = g_io_number_of_partitions / number_of_mpitasks;

		// close the file
		fclose(master_header);
	}

	// Broadcast vars across comm
	MPI_Bcast(&g_io_number_of_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&g_io_number_of_partitions, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&g_io_partitions_per_rank, 1, MPI_INT, 0, MPI_COMM_WORLD);

}
