//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "io.h"

char * g_io_master_filename;
FILE ** g_io_files;
io_partition * g_io_partitions;

void io_read_master_header(char * path, char * filename) {
	// path = dirname(argv[0]);

	g_io_master_filename = (char *) calloc(1024, sizeof(char));
    strcpy(g_io_master_filename, path);
    strcat(g_io_master_filename, filename);
    
    FILE * master_header = fopen(g_io_master_filename, "r");
    assert(master_header && "Can not open master header to read\n");

    fscanf(master_header, "%d %d", &g_io_number_of_files, &g_io_number_of_partitions);

    g_io_files = (FILE **) calloc(g_io_number_of_files, sizeof(FILE*));
    g_io_partitions = (io_partition *) calloc(g_io_number_of_partitions, sizeof(io_partition));

	int i;
	while (fscanf(master_header, "%d", &i) != EOF) {
		fscanf(master_header, "%d %d %d %d %d", &(g_io_partitions[i].file), &(g_io_partitions[i].offest), &(g_io_partitions[i].size), &(g_io_partitions[i].lp_count), &(g_io_partitions[i].event_count));
	}

	fclose(master_header);
}

void io_write_master_header(char * check_point_name) {
	char filename[1024];
	strcpy(filename, g_io_master_filename);
	strcat(filename, ".");
	strcat(filename, check_point_name);

	FILE * master_header = fopen(filename, "w");
	assert(master_header && "Can not open master header to write checkpoint\n");

	fprintf(master_header, "%d %d\n", g_io_number_of_files, g_io_number_of_partitions);
	int i;
	for (i = 0; i < g_io_number_of_partitions; i++) {
		fprintf(master_header, "%d %d %d %d %d %d\n", i, g_io_partitions[i].file, g_io_partitions[i].offest, g_io_partitions[i].size, g_io_partitions[i].lp_count, g_io_partitions[i].event_count);
	}

	fclose(master_header);
}
