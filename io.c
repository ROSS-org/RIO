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
io_partition g_io_partition;

// Default Values
int g_io_number_of_files = 0;
int g_io_number_of_partitions = 0;
int g_io_partitions_per_rank = 0;

// local init flag (space has been allocated)
int l_init_flag = 0;

void io_init(int num_files, int num_partitions) {
    g_io_number_of_files = num_files;
    g_io_number_of_partitions = num_partitions;

    //g_io_files = (FILE **) calloc(g_io_number_of_files, sizeof(FILE*));
    //g_io_partition = (io_partition) calloc(1, sizeof(io_partition));

    l_init_flag = 1;
}

void io_final() {
    g_io_number_of_files = 0;
    g_io_number_of_partitions = 0;
        
    //free(g_io_files);
    //free(g_io_partition);
	
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
        //fscanf(master_header, "%d %d %d %d %d", &(g_io_partitions[i].file), &(g_io_partitions[i].offset), &(g_io_partitions[i].size), &(g_io_partitions[i].data_count), &(g_io_partitions[i].data_size));
    }

    fclose(master_header);
}

void io_write_master_header(char * master_filename) {

    FILE * master_header = fopen(master_filename, "w");
    assert(master_header && "Can not open master header to write\n");

    fprintf(master_header, "%d %d\n", g_io_number_of_files, g_io_number_of_partitions);
    int i;
    for (i = 0; i < g_io_number_of_partitions; i++) {
        //fprintf(master_header, "%d %d %d %d %d %d\n", i, g_io_partitions[i].file, g_io_partitions[i].offset, g_io_partitions[i].size, g_io_partitions[i].data_count, g_io_partitions[i].data_size);
    }

    fclose(master_header);
}

void process_metadata(char * data_block, int mpi_rank) {
    int i, offset, count;
    int partition_number;
    for (i = 0; i < g_io_partitions_per_rank; i++) {
        //count = sscanf(data_block, "%d %d %d %d %d %d%n", &partition_number, &g_io_partitions[i].file, &g_io_partitions[i].offset, &g_io_partitions[i].size, &g_io_partitions[i].data_count, &g_io_partitions[i].data_size, &offset);
        assert(count == 6 && "Error: could not read correct number of ints during partition_metadata processing\n");
        assert(partition_number == (mpi_rank * g_io_partitions_per_rank) + i && "Error: an MPI Task is reading the metadata from an unexpected partition\n");
        data_block += offset;
    }
}

void io_load_checkpoint(char * master_filename) {
    int i;
    int mpi_rank, number_of_mpitasks;
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &number_of_mpitasks);

    FILE * master_header;
    int partition_md_size;
    MPI_Request r;
	
    if (mpi_rank == 0) {
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
    //g_io_partition = (io_partition *) calloc(g_io_partitions_per_rank, sizeof(io_partition));
    char * block = (char *) calloc(g_io_partitions_per_rank, partition_md_size);

    if (mpi_rank == 0) {
        // Read and distribute meta-data
        for (i = 0; i < number_of_mpitasks; i++) {
            fread(block, partition_md_size, g_io_partitions_per_rank, master_header);
            if (i == mpi_rank) {
                process_metadata(block, mpi_rank);
            } else {
                MPI_Isend(block, partition_md_size * g_io_partitions_per_rank, MPI_CHAR, i, 0, MPI_COMM_WORLD, &r);
            }
        }

        // close the file
        fclose(master_header);
    } else {
        // receive meta-data
        MPI_Irecv(block, partition_md_size * g_io_partitions_per_rank, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &r);
        process_metadata(block, mpi_rank);
    }
}

void io_store_checkpoint(char * master_filename) {
    int i;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    FILE * file;
    int partition_md_size;
    MPI_Request r;

    // ASSUMPTION: A checkpoint writes LP data (only)
    // TODO: support event data writing
    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "Error: IO variables not set: # of file or # of parts\n");

    // Model must fill in g_io_partition data?
    printf("MPI Rank %d reports %lu lps (starting with gid %lu)\n", mpi_rank, g_tw_nlp, g_tw_lp[0]->gid);

    // TODO: write more than just lp pointer
    g_io_partition.data_size = sizeof(tw_lp *);

    g_io_partitions_per_rank = g_io_number_of_partitions / number_of_mpitasks;
    int io_partitions_per_file = g_io_number_of_partitions / g_io_number_of_files;
    int io_ranks_per_file = number_of_mpitasks / g_io_number_of_files;

    // calculate write position
    int file_number = mpi_rank / io_ranks_per_file;
    int write_position = mpi_rank % io_ranks_per_file;
    int offset = 0;
    
    // if not first, wait for prev to write
    if (write_position != 0) {
        MPI_Recv(&offset, 1, MPI_INT, mpi_rank-1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    
    // Write
    // open at end of files
    char filename[100];
    sprintf(filename, "%s.data-%d", master_filename, file_number);
    if (write_position == 0) {
        file = fopen(filename, "w");
    } else {
        file = fopen(filename, "a");
    }
    // each rank goes through the LP list
    for (i = 0; i < g_tw_nlp; i++) {
        fwrite(g_tw_lp, sizeof(tw_lp *), g_tw_nlp, file);
    }
    // close
    fclose(file);
    printf("Rank %d just wrote to file %d\n", mpi_rank, file_number);

    g_io_partition.offset = offset;
    
    // if not last, send to next writer
    if (write_position != io_ranks_per_file - 1){
        offset += g_tw_nlp * sizeof(tw_lp *);
        MPI_Send(&offset, 1, MPI_INT, mpi_rank+1, 0, MPI_COMM_WORLD);
    }

    // each rank fills in its partition data
    g_io_partition.file = file_number;
    g_io_partition.data_count = g_tw_nlp;
    if (g_io_partition.data_size != 0) {
        g_io_partition.size = g_io_partition.data_count * g_io_partition.data_size;
    } else {
        // TODO: 
        // non-static data size
        // call model defined function
    }

    // MPI_Gather on partition data
    io_partition *partitions;
    if (mpi_rank == 0) {
        // rank 0 can't gather with g_io_partitions
        partitions = (io_partition *) calloc(g_io_number_of_partitions, sizeof(io_partition *));
    }
    
    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(5, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);
    MPI_Gather(&g_io_partition, 1, MPI_IO_PART, partitions, 1, MPI_IO_PART, 0, MPI_COMM_WORLD);
    
    if (mpi_rank == 0) {
        // write master header
        sprintf(filename, "%s.mh", master_filename);
        file = fopen(filename, "w");
        fprintf(file, "%d %d %d\n", g_io_number_of_files, g_io_number_of_partitions, 10); 
        for(i = 0; i < number_of_mpitasks; i++){
            printf("Partition %d:\n", i);
            printf("\tfile\t%d\n", partitions[i].file);
            printf("\toffset\t%d\n", partitions[i].offset);
            printf("\tsize\t%d\n", partitions[i].size);
            printf("\tdata_count\t%d\n", partitions[i].data_count);
            printf("\tdata_size\t%d\n", partitions[i].data_size);
            fprintf(file, "%d %d %d %d %d\n", partitions[i].file, partitions[i].offset, partitions[i].size, partitions[i].data_count, partitions[i].data_size);
        }
        fclose(file);
    }
}
