//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "mpi.h"
#include "io.h"

// Null Initializations
FILE ** g_io_files;
io_partition * g_io_partitions;

// Default Values
int g_io_number_of_files = 0;
int g_io_number_of_partitions = 0;
int g_io_partitions_per_rank = 0;

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

    assert(num_files == g_io_number_of_files && "Error: Master Header indicated a different number of files than was previously allocated\n");
    assert(num_partitions == g_io_number_of_partitions && "Error: Master Header indicated a different number of partitions than was previouly allocated\n");

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

void process_metadata(char * data_block, int self) {
	int i, offset, count;
	int partition_number;
	for (i = 0; i < g_io_partitions_per_rank; i++) {
		count = sscanf(data_block, "%d %d %d %d %d %d%n", &partition_number, &g_io_partitions[i].file, &g_io_partitions[i].offset, &g_io_partitions[i].size, &g_io_partitions[i].lp_count, &g_io_partitions[i].event_count, &offset);
		assert(count == 6 && "Error: could not read correct number of ints during partition_metadata processing\n");
		assert(partition_number == (self * g_io_partitions_per_rank) + i && "Error: an MPI Task is reading the metadata from an unexpected partition\n");
		data_block += offset;
	}
}

void io_load_checkpoint(char * master_filename) {
	int i;
	int self, number_of_mpitasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &self);
	MPI_Comm_size(MPI_COMM_WORLD, &number_of_mpitasks);

	FILE * master_header;
	int partition_md_size;
	MPI_Request r;
	
	if (self == 0) {
		// Open master header file
		master_header = fopen(master_filename, "r");
		assert(master_header && "MPI_Task 0 can not open master header to read checkpoint\n");

		// Read first line for global vars
		fscanf(master_header, "%d %d %d", &g_io_number_of_files, &g_io_number_of_partitions, &partition_md_size);
	}

	// Broadcast vars across comm
	MPI_Bcast(&g_io_number_of_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&g_io_number_of_partitions, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&partition_md_size, 1, MPI_INT, 0, MPI_COMM_WORLD);

	g_io_partitions_per_rank = g_io_number_of_partitions / number_of_mpitasks;

	// Init local partitions
	g_io_partitions = (io_partition *) calloc(g_io_partitions_per_rank, sizeof(io_partition));
	char * block = (char *) calloc(g_io_partitions_per_rank, partition_md_size);

	if (self == 0) {
		// Read and distribute meta-data
		for (i = 0; i < number_of_mpitasks; i++) {
			fread(block, partition_md_size, g_io_partitions_per_rank, master_header);
			if (i == self) {
				process_metadata(block, self);
			} else {
				MPI_Isend(block, partition_md_size * g_io_partitions_per_rank, MPI_CHAR, i, 0, MPI_COMM_WORLD, &r);
			}
		}

		// close the file
		fclose(master_header);
	} else {
		// receive meta-data
		MPI_Irecv(block, partition_md_size * g_io_partitions_per_rank, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &r);
		process_metadata(block, self);
	}
}
