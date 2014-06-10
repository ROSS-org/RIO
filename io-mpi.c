//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "mpi.h"
#include "io.h"

// Null Initializations
io_partition * g_io_partitions;
datatype_function model_datatype;
serialize_function model_serialize;
deserialize_function model_deserialize;
size_t model_size;

// Default Values
int g_io_number_of_files = 0;
int g_io_number_of_partitions = 0;
int g_io_partitions_per_rank = 0;

// local init flag (space has been allocated)
int l_init_flag = 0;

void io_init(int num_files, int num_partitions) {
    if (num_partitions == 0) {
        num_partitions = tw_nnodes();
    }

    g_io_number_of_files = num_files;
    g_io_number_of_partitions = num_partitions;
    g_io_partitions_per_rank = num_partitions / tw_nnodes();
    l_init_flag = 1;
    if (g_tw_mynode == 0) {
        printf("*** IO SYSTEM INIT ***\n\tFiles: %d\n\tParts: %d\n\tPartsPerRank: %d\n\n", g_io_number_of_files, g_io_number_of_partitions, g_io_partitions_per_rank);
    }
}

void io_setup(datatype_function model_dt, serialize_function model_s, deserialize_function model_ds, size_t store_size) {
    model_datatype = model_dt;
    model_serialize = model_s;
    model_deserialize = model_ds;
    model_size = store_size;
}

void io_final() {
    g_io_number_of_files = 0;
    g_io_number_of_partitions = 0;
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

    printf("Rank %ld scanning line \"%s\"\n", g_tw_mynode, data_block);

    for (i = 0; i < g_io_partitions_per_rank; i++) {
        count = sscanf(data_block, "%d %d %d %d %d %d%n", &partition_number, &g_io_partitions[i].file, &g_io_partitions[i].offset, &g_io_partitions[i].size, &g_io_partitions[i].data_count, &g_io_partitions[i].data_size, &offset);
        assert(count == 6 && "Error: could not read correct number of ints during partition_metadata processing\n");
        assert(partition_number == (mpi_rank * g_io_partitions_per_rank) + i && "Error: an MPI Task is reading the metadata from an unexpected partition\n");
        data_block += offset;
    }
}

void io_load_checkpoint(char * master_filename) {
    int i;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    FILE * file;
    char filename[100];
    MPI_Request r;
    int partition_md_size;
	
    if (mpi_rank == 0) {
        // Open master header file
        sprintf(filename, "%s.mh", master_filename);
        file = fopen(filename, "r");
        assert(file && "MPI_Task 0 can not open master header to read checkpoint\n");

        // Read first line for global vars
        fscanf(file, "%d %d %d\n", &g_io_number_of_files, &g_io_number_of_partitions, &partition_md_size);
    }

    // Broadcast vars across comm
    MPI_Bcast(&g_io_number_of_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&g_io_number_of_partitions, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&partition_md_size, 1, MPI_INT, 0, MPI_COMM_WORLD);

    g_io_partitions_per_rank = g_io_number_of_partitions / number_of_mpitasks;

    // Init local partitions
    char * block = (char *) calloc(g_io_partitions_per_rank, partition_md_size);

    if (mpi_rank == 0) {
        // Read and distribute meta-data
        for (i = 0; i < number_of_mpitasks; i++) {
            fread(block, partition_md_size, g_io_partitions_per_rank, file);
            if (i == mpi_rank) {
                process_metadata(block, mpi_rank);
            } else {
                MPI_Isend(block, partition_md_size * g_io_partitions_per_rank, MPI_CHAR, i, 0, MPI_COMM_WORLD, &r);
            }
        }
        // close the file
        //fclose(file);
    } else {
        // receive meta-data
        MPI_Irecv(block, partition_md_size * g_io_partitions_per_rank, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &r);
        MPI_Wait(&r, MPI_STATUS_IGNORE);
        process_metadata(block, mpi_rank);
    }

    // This example sort of works.
    printf("\n** Unserialize of LP data **\n\n");
    tw_lp test_array[g_tw_nlp];
    void *buffer, *b; // TODO: fix.
    int lp_size = sizeof(io_lp_store) + model_size;

    for (i = 0, b = buffer; i < g_tw_nlp; i++, b += lp_size) {
        printf("Reading from buffer at %p\n", b);
        io_deserialize_lp(b, &(test_array[i]));
        model_deserialize(b + sizeof(io_lp_store), &(test_array[i].cur_state));
        printf("Rank %d deserialized LP %lu\n", mpi_rank, test_array[i].gid);
    }
    return;
}

void io_store_checkpoint(char * master_filename) {
    int i;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    // ASSUMPTION: A checkpoint writes LP data (only)
    // TODO: support event data writing
    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "Error: IO variables not set: # of file or # of parts\n");

    // Gather LP data
    int lp_size = sizeof(io_lp_store) + model_size;
    char buffer[g_tw_nlp * lp_size];
    void * b;
    for (i = 0, b = buffer; i < g_tw_nlp; i++, b += lp_size) {
        io_serialize_lp(g_tw_lp[i], b);
        model_serialize(g_tw_lp[i]->cur_state, b + sizeof(io_lp_store));
    }

    // Create joint datatype
    MPI_Datatype LP, MODEL, LP_STATE;
    MPI_Datatype oldtypes[2];
    int blockcounts[2];
    MPI_Aint offsets[2], extent;

    io_mpi_datatype_lp(&LP);
    model_datatype(&MODEL);

    offsets[0] = 0;
    oldtypes[0] = LP;
    blockcounts[0] = 1;

    MPI_Type_extent(oldtypes[0], &extent);
    offsets[1] = blockcounts[0] * extent;
    oldtypes[1] = MODEL;
    blockcounts[1] = 1;

    MPI_Type_struct(2, blockcounts, offsets, oldtypes, &LP_STATE);
    MPI_Type_commit(&LP_STATE);

    // ASSUMPTION: static LP model size

    g_io_partitions_per_rank = g_io_number_of_partitions / number_of_mpitasks;
    int io_partitions_per_file = g_io_number_of_partitions / g_io_number_of_files;
    int io_ranks_per_file = number_of_mpitasks / g_io_number_of_files;

    // Set up Comms
    MPI_File fh;
    MPI_Status status;
    MPI_Comm file_comm;

    int file_number = mpi_rank / io_ranks_per_file;
    int file_position = mpi_rank % io_ranks_per_file;
    MPI_Offset offset = (long long) lp_size * g_tw_nlp * file_position;
    // assume const g_tw_nlp

    MPI_Comm_split(MPI_COMM_WORLD, file_number, file_position, &file_comm);

    // Write
    char filename[100];
    sprintf(filename, "%s.data-%d", master_filename, file_number);
    MPI_File_open(file_comm, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
    MPI_File_seek(fh, offset, MPI_SEEK_SET);

    MPI_File_write(fh, &buffer, g_tw_nlp, LP_STATE, &status);

    MPI_File_close(&fh);

    // each rank fills in its local partition data
    io_partition my_partitions[g_io_partitions_per_rank];
    for (int i = 0; i < g_io_partitions_per_rank; ++i) {
        my_partitions[i].file = file_number;
        my_partitions[i].data_count = g_tw_nlp / g_io_partitions_per_rank;
        my_partitions[i].data_size = lp_size;
        my_partitions[i].size = my_partitions[i].data_count * my_partitions[i].data_size;
    }

    // MPI_Gather on partition data
    io_partition *partitions;
    if (mpi_rank == 0) {
        partitions = (io_partition *) calloc(g_io_number_of_partitions, sizeof(io_partition *));
    }
    
    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(5, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);
    MPI_Gather(&my_partitions, g_io_partitions_per_rank, MPI_IO_PART, partitions, g_io_partitions_per_rank, MPI_IO_PART, 0, MPI_COMM_WORLD);
    
    if (mpi_rank == 0) {
        FILE *file;
        // write master header
        sprintf(filename, "%s.mh", master_filename);
        file = fopen(filename, "w");
//        fprintf(file, "%d %d %d\n", g_io_number_of_files, g_io_number_of_partitions, 78); 
        for(i = 0; i < g_io_number_of_partitions; i++){
            fprintf(file, "%12d %12d %12d %12d %12d %12d\n", i, partitions[i].file, partitions[i].offset, partitions[i].size, partitions[i].data_count, partitions[i].data_size);
        }
        fclose(file);
    }
}

void io_mpi_datatype_lp (MPI_Datatype *datatype) {
    int i;

    int typecount = 1 + g_tw_nRNG_per_lp;
    int rng_types = 1;
#ifdef RAND_NORMAL
    typecount += 2 * g_tw_nRNG_per_lp;
    rng_types = 3;
#endif

    MPI_Datatype oldtypes[typecount];
    int blockcounts[typecount];
    MPI_Aint offsets[typecount], extent;

    // Type 1: tw_lpid x 1
    offsets[0] = 0;
    oldtypes[0] = MPI_UNSIGNED_LONG_LONG;
    blockcounts[0] = 1;

    for (i = 0; i < g_tw_nRNG_per_lp; i++) {
        // Type 2: int32_t x 12
        MPI_Type_extent(oldtypes[rng_types*i+0], &extent);
        offsets[rng_types*i+1] = blockcounts[rng_types*i+0] * extent;
        oldtypes[rng_types*i+1] = MPI_INT32_T;
        blockcounts[rng_types*i+1] = 12;

#ifdef RAND_NORMAL
        // Type 3: double x 2
        MPI_Type_extent(oldtypes[rng_types*i+1], &extent);
        offsets[rng_types*i+2] = offsets[rng_types*i+1] + blockcounts[rng_types*i+1] * extent;
        oldtypes[rng_types*i+2] = MPI_DOUBLE;
        blockcounts[rng_types*i+2] = 2;

        //Type 4: int x 1
        MPI_Type_extent(oldtypes[rng_types*i+2], &extent);
        offsets[rng_types*i+3] = offsets[rng_types*i+2] + blockcounts[rng_types*i+2] * extent;
        oldtypes[rng_types*i+3] = MPI_INT;
        blockcounts[rng_types*i+3] = 1;
#endif
    }

    MPI_Type_struct(typecount, blockcounts, offsets, oldtypes, datatype);
    MPI_Type_commit(datatype);
}

void io_serialize_lp (tw_lp *lp, void *store) {
    int i, j;

    io_lp_store tmp;

    tmp.gid = lp->gid;
    for (i = 0; i < g_tw_nRNG_per_lp; i++) {
        for (j = 0; j < 4; j++) {
            tmp.rng[j] = lp->rng->Ig[j];
            tmp.rng[j+4] = lp->rng->Lg[j];
            tmp.rng[j+8] = lp->rng->Cg[j];
        }
#ifdef RAND_NORMAL
        tmp.tw_normal_u1 = lp->rng->tw_normal_u1;
        tmp.tw_normal_u2 = lp->rng->tw_normal_u2;
        tmp.tw_normal_flipflop = lp->rng->tw_normal_flipflop;
#endif
    }

    memcpy(store, &tmp, sizeof(io_lp_store));
}

void io_deserialize_lp (void *store, tw_lp *lp) {
    int i, j;

    io_lp_store tmp;
    memcpy(&tmp, store, sizeof(io_lp_store));

    lp->gid = tmp.gid;

    for (i = 0; i < g_tw_nRNG_per_lp; i++) {
        for (j = 0; j < 4; j++) {
            lp->rng->Ig[j] = tmp.rng[j];
            lp->rng->Lg[j] = tmp.rng[j+4];
            lp->rng->Cg[j] = tmp.rng[j+8];
        }
#ifdef RAND_NORMAL
        lp->rng->tw_normal_u1 = tmp.tw_normal_u1;
        lp->rng->tw_normal_u2 = tmp.tw_normal_u2;
        lp->rng->tw_normal_flipflop = tmp.tw_normal_flipflop;
#endif
    }
}
