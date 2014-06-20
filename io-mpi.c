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

// Default Values
int g_io_number_of_files = 0;
int g_io_number_of_partitions = 0;
int g_io_partitions_per_rank = 0;
io_lptype * g_io_lp_types = NULL;

// local init flag (space has been allocated)
int l_init_flag = 0;

// Command Line Options
const tw_optdef io_opt[] = {
    TWOPT_GROUP("ROSSIO"),
    TWOPT_UINT("io-files", g_io_number_of_files, "io files"),
    TWOPT_UINT("io-parts", g_io_number_of_partitions, "io partitions"),
    TWOPT_END()
};

void io_opts () {
    tw_opt_add(io_opt);
}

char model_version[41];

void io_register_model_version (char *sha1) {
    strcpy(model_version, sha1);
}

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

    // assert that IO system has been init
    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "ERROR: IO variables not set: # of files or # of parts\n");

    g_io_partitions_per_rank = g_io_number_of_partitions / number_of_mpitasks;

    MPI_File fh;
    MPI_Status status;
    char filename[100];

    // Read MH

    // Metadata datatype
    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(6, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);
    int partition_md_size;
    MPI_Type_size(MPI_IO_PART, &partition_md_size);
    MPI_Offset offset = (long long) partition_md_size * mpi_rank * g_io_partitions_per_rank;

    io_partition my_partitions[g_io_partitions_per_rank];

    sprintf(filename, "%s.mh", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
	MPI_File_seek(fh, offset, MPI_SEEK_SET);

    MPI_File_read(fh, &my_partitions, g_io_partitions_per_rank, MPI_IO_PART, &status);

    MPI_File_close(&fh);

    for (i = 0; i < g_io_partitions_per_rank; i++) {
        printf("Rank %d read metadata\n\tpart %d\n\tfile %d\n\toffset %d\n\tsize %d\n\tcount %d\n\tdsize %d\n\n", mpi_rank, my_partitions[i].part, my_partitions[i].file, my_partitions[i].offset, my_partitions[i].size, my_partitions[i].data_count, my_partitions[i].data_size);
    }
    
    // Now data files
    // ASSUME UNIFORM DATA SIZE
    for (i = 1; i < g_io_partitions_per_rank; i++) {
        assert(my_partitions[i].file == my_partitions[0].file && "ERROR: Some rank has partitions spread across files\n");
        assert(my_partitions[i].data_size == my_partitions[0].data_size && "ASSUMPTION: all data size is the same\n");
    }

    // Set up datatypes
    MPI_Datatype LP, MODEL, LP_STATE;
    MPI_Datatype oldtypes[2];
    int blockcounts[2];
    MPI_Aint offsets[2], extent;

    io_lp_mpi_datatype(&LP);
    ((datatype_f)g_io_lp_types[0].datatype)(&MODEL);

    offsets[0] = 0;
    oldtypes[0] = LP;
    blockcounts[0] = 1;

    MPI_Type_extent(oldtypes[0], &extent);
    offsets[1] = blockcounts[0] * extent;
    oldtypes[1] = MODEL;
    blockcounts[1] = 1;

    MPI_Type_struct(2, blockcounts, offsets, oldtypes, &LP_STATE);
    MPI_Type_commit(&LP_STATE);

    // Read file
    MPI_Comm file_comm;
    int file_number = my_partitions[0].file;
    int lp_size = my_partitions[0].data_size;
    int partitions_size = 0;
    int partitions_count = 0;
    for (i = 0; i < g_io_partitions_per_rank; i++) {
        partitions_size += my_partitions[i].size;
        partitions_count += my_partitions[i].data_count;
    }
    char buffer[partitions_size];
    void * b = buffer;
    sprintf(filename, "%s.data-%d", master_filename, file_number);

    MPI_Comm_split(MPI_COMM_WORLD, file_number, mpi_rank, &file_comm);
    MPI_File_open(file_comm, filename, MPI_MODE_RDWR, MPI_INFO_NULL, &fh);

    // FOR SOME REASON WE CAN'T DO A SINGLE READ OF MULTIPLE PARTITIONS
    for (i = 0; i < g_io_partitions_per_rank; i++){
        offset = (long long) my_partitions[i].offset;
        MPI_File_seek(fh, offset, MPI_SEEK_SET);
        MPI_File_read(fh, b, my_partitions[i].data_count, LP_STATE, &status);
        b += my_partitions[i].size;
    }

    MPI_File_close(&fh);

    // Load Data
    for (i = 0, b = buffer; i < partitions_count; i++, b += lp_size) {
        io_lp_deserialize(b, g_tw_lp[i]);
        ((deserialize_f)g_io_lp_types[0].deserialize)(b + sizeof(io_lp_store), g_tw_lp[i]);
    }
    return;
}

void io_store_checkpoint(char * master_filename) {
    int i;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    // Create joint datatype
    MPI_Datatype LP, MODEL, LP_STATE;
    MPI_Datatype oldtypes[2];
    int blockcounts[2];
    MPI_Aint offsets[2], extent;

    io_lp_mpi_datatype(&LP);
    ((datatype_f)g_io_lp_types[0].datatype)(&MODEL);

    offsets[0] = 0;
    oldtypes[0] = LP;
    blockcounts[0] = 1;

    MPI_Type_extent(oldtypes[0], &extent);
    offsets[1] = blockcounts[0] * extent;
    oldtypes[1] = MODEL;
    blockcounts[1] = 1;

    MPI_Type_struct(2, blockcounts, offsets, oldtypes, &LP_STATE);
    MPI_Type_commit(&LP_STATE);

    // ASSUMPTION: A checkpoint writes LP data (only)
    // TODO: support event data writing
    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "Error: IO variables not set: # of file or # of parts\n");

    // Gather LP data
    int lp_size;
    MPI_Type_size(LP_STATE, &lp_size);
    lp_size = sizeof(io_lp_store); // these are not always the same!
    int total_size = lp_size + g_io_lp_types[0].model_size;
    char buffer[g_tw_nlp * total_size];
    void * b;
    for (i = 0, b = buffer; i < g_tw_nlp; i++, b += total_size) {
        io_lp_serialize(g_tw_lp[i], b);
        ((serialize_f)g_io_lp_types[0].serialize)(g_tw_lp[i], b + lp_size);
    }

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
    MPI_Offset offset = (long long) total_size * g_tw_nlp * file_position;
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
        my_partitions[i].part = (mpi_rank * g_io_partitions_per_rank) + i;
        my_partitions[i].file = file_number;
        my_partitions[i].data_count = g_tw_nlp / g_io_partitions_per_rank;
        my_partitions[i].data_size = total_size;
        my_partitions[i].size = my_partitions[i].data_count * my_partitions[i].data_size;
        my_partitions[i].offset = offset;
    }

    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(6, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);
    
    // TWO OPTIONS HERE
    // 1. gather all to rank 0 and write ascii via posix
    // 2. a single write operation across MPI_COMM_WORLD to all write binary data
    // ==> implementing two now (1 in previous commit)

    for (i = 0; i < g_io_partitions_per_rank; i++) {
        printf("Rank %d wrote metadata\n\tpart %d\n\tfile %d\n\toffset %d\n\tsize %d\n\tcount %d\n\tdsize %d\n\n", mpi_rank, my_partitions[i].part, my_partitions[i].file, my_partitions[i].offset, my_partitions[i].size, my_partitions[i].data_count, my_partitions[i].data_size);
    }
    int psize;
    MPI_Type_size(MPI_IO_PART, &psize);
    printf("Metadata size: %d or %d\n", psize, sizeof(io_partition));

    offset = (long long) sizeof(io_partition) * g_io_partitions_per_rank * mpi_rank;
    sprintf(filename, "%s.mh", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
    MPI_File_seek(fh, offset, MPI_SEEK_SET);

    MPI_File_write(fh, &my_partitions, g_io_partitions_per_rank, MPI_IO_PART, &status);

    MPI_File_close(&fh);

    // WRITE READ ME
    if (mpi_rank == 0) {
        FILE *file;
        sprintf(filename, "%s.read-me.txt", master_filename);
        file = fopen(filename, "w");
        fprintf(file, "This file was auto-generated by ROSS-IO.\n\n");
#if HAVE_CTIME
        time_t raw_time;
        time(&raw_time);
        fprintf(file, "Date Created:\t%s\n", ctime(&raw_time));
#endif
#ifdef ROSS_VERSION
        fprintf(file, "ROSS Version:\t%s\n", ROSS_VERSION);
#endif
#ifdef ROSSIO_VERSION
        fprintf(file, "ROSSIO Version:\t%s\n", ROSSIO_VERSION);
#endif
        fprintf(file, "MODEL Version:\t%s\n", model_version);
        fprintf(file, "Checkpoint:\t%s\n", master_filename);
        fprintf(file, "Model Size:\t%d\n", g_io_lp_types[0].model_size);
        fprintf(file, "Data Files:\t%d\n", g_io_number_of_files);
        fprintf(file, "Partitions:\t%d\n", g_io_number_of_partitions);
#ifdef RAND_NORMAL
        fprintf(file, "RAND_NORMAL\tON\n");
#else
        fprintf(file, "RAND_NORMAL\tOFF\n");
#endif
    }
}

static void io_lp_mpi_datatype (MPI_Datatype *datatype) {
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

static void io_lp_serialize (tw_lp *lp, void *store) {
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

static void io_lp_deserialize (void *store, tw_lp *lp) {
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
