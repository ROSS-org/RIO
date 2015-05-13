//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "mpi.h"
#include "io.h"
#include "io-config.h"

// Null Initializations
io_partition * g_io_partitions;

// Default Values
int g_io_number_of_files = 0;
int g_io_number_of_partitions = 0;
int g_io_partitions_on_rank = 0;
io_lptype * g_io_lp_types = NULL;
io_load_type g_io_load_at = NONE;
char g_io_checkpoint_name[1024];
int g_io_events_buffered_per_rank = 0;
tw_eventq g_io_buffered_events;
tw_eventq g_io_free_events;

// local init flag (space has been allocated)
int l_init_flag = 0;

// Command Line Options
const tw_optdef io_opt[] = {
    TWOPT_GROUP("ROSSIO"),
    TWOPT_UINT("io-files", g_io_number_of_files, "io files"),
    TWOPT_UINT("io-parts", g_io_number_of_partitions, "io partitions"),
    TWOPT_UINT("io-ppr", g_io_partitions_on_rank, "io partitions per rank"),
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
    int i;
    if (num_partitions == 0) {
        num_partitions = tw_nnodes();
    }

    g_io_number_of_files = num_files;
    g_io_number_of_partitions = num_partitions;
    g_io_partitions_on_rank = num_partitions / tw_nnodes();
    l_init_flag = 1;
    if (g_tw_mynode == 0) {
        printf("*** IO SYSTEM INIT ***\n\tFiles: %d\n\tParts: %d\n\tPartsPerRank: %d\n\n", g_io_number_of_files, g_io_number_of_partitions, g_io_partitions_on_rank);
    }

    // tw_eventq_alloc(&g_io_free_events, g_io_events_buffered_per_rank);
    g_io_free_events.size = 0;
    g_io_free_events.head = g_io_free_events.tail = NULL;
    g_io_buffered_events.size = 0;
    g_io_buffered_events.head = g_io_buffered_events.tail = NULL;
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
        //fscanf(master_header, "%d %d %d %d", &(g_io_partitions[i].file), &(g_io_partitions[i].offset), &(g_io_partitions[i].size), &(g_io_partitions[i].data_count));
    }

    fclose(master_header);
}

void io_write_master_header(char * master_filename) {

    FILE * master_header = fopen(master_filename, "w");
    assert(master_header && "Can not open master header to write\n");

    fprintf(master_header, "%d %d\n", g_io_number_of_files, g_io_number_of_partitions);
    int i;
    for (i = 0; i < g_io_number_of_partitions; i++) {
        //fprintf(master_header, "%d %d %d %d %d\n", i, g_io_partitions[i].file, g_io_partitions[i].offset, g_io_partitions[i].size, g_io_partitions[i].data_count);
    }

    fclose(master_header);
}

void process_metadata(char * data_block, int mpi_rank) {
    int i, offset, count;
    int partition_number;

    printf("Rank %ld scanning line \"%s\"\n", g_tw_mynode, data_block);

    for (i = 0; i < g_io_partitions_on_rank; i++) {
        count = sscanf(data_block, "%d %d %d %d %d %d%n",
            &partition_number, &g_io_partitions[i].file, &g_io_partitions[i].offset,
            &g_io_partitions[i].lp_size, &g_io_partitions[i].lp_count, &g_io_partitions[i].ev_count,
            &offset);
        assert(count == 7 && "Error: could not read correct number of ints during partition_metadata processing\n");
        assert(partition_number == (mpi_rank * g_io_partitions_on_rank) + i && "Error: an MPI Task is reading the metadata from an unexpected partition\n");
        data_block += offset;
    }
}

void io_load_checkpoint(char * master_filename) {
    int i;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    // assert that IO system has been init
    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "ERROR: IO variables not set: # of files or # of parts\n");

    g_io_partitions_on_rank = g_io_number_of_partitions / number_of_mpitasks;

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
    MPI_Offset offset = (long long) partition_md_size * mpi_rank * g_io_partitions_on_rank;

    io_partition my_partitions[g_io_partitions_on_rank];

    sprintf(filename, "%s.mh", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
	MPI_File_seek(fh, offset, MPI_SEEK_SET);

    MPI_File_read(fh, &my_partitions, g_io_partitions_on_rank, MPI_IO_PART, &status);

    // error check
    int count_sum = 0;
    for (i = 0; i < g_io_partitions_on_rank; i++) {
        count_sum += my_partitions[i].lp_count;
    }
    assert(count_sum == g_tw_nlp && "ERROR: wrong number of LPs in partitions");

    // read size array
    offset = (long long) 0;
    long long contribute = (long long) g_tw_nlp;
    MPI_Exscan(&contribute, &offset, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    offset += (long long) (sizeof(io_partition) * g_io_number_of_partitions);

    size_t model_sizes[g_tw_nlp];
    int index = 0;
    for (i = 0; i < g_io_partitions_on_rank; i++){
        MPI_File_seek(fh, offset, MPI_SEEK_SET);
        int data_count = my_partitions[i].lp_count;
        MPI_File_read(fh, &model_sizes[index], data_count, MPI_UNSIGNED_LONG, &status);
        index += data_count;
        offset += (long long) data_count;
    }

    MPI_File_close(&fh);

    for (i = 0; i < g_io_partitions_on_rank; i++) {
        printf("Rank %d read metadata\n\tpart %d\n\tfile %d\n\toffset %d\n\tlp size %d\n\tlp count %d\n\tevents %d\n\n", mpi_rank,
            my_partitions[i].part, my_partitions[i].file, my_partitions[i].offset,
            my_partitions[i].lp_size, my_partitions[i].lp_count, my_partitions[i].ev_count);
    }

    // Now data files
    for (i = 1; i < g_io_partitions_on_rank; i++) {
        assert(my_partitions[i].file == my_partitions[0].file && "ERROR: Some rank has partitions spread across files\n");
    }

    // Read file
    MPI_Comm file_comm;
    int file_number = my_partitions[0].file;
    int lp_size = sizeof(io_lp_store);
    int partitions_size = 0;
    int partitions_events = 0;
    for (i = 0; i < g_io_partitions_on_rank; i++) {
        partitions_size += my_partitions[i].lp_size + (my_partitions[i].ev_count *  g_tw_event_msg_sz);
        partitions_events += my_partitions[i].ev_count;
    }
    char buffer[partitions_size];
    void * b = buffer;
    sprintf(filename, "%s.data-%d", master_filename, file_number);

    MPI_Comm_split(MPI_COMM_WORLD, file_number, mpi_rank, &file_comm);
    MPI_File_open(file_comm, filename, MPI_MODE_RDWR, MPI_INFO_NULL, &fh);

    // FOR SOME REASON WE CAN'T DO A SINGLE READ OF MULTIPLE PARTITIONS
    for (i = 0; i < g_io_partitions_on_rank; i++){
        offset = (long long) my_partitions[i].offset;
        MPI_File_seek(fh, offset, MPI_SEEK_SET);
        MPI_File_read(fh, b, my_partitions[i].lp_size + (partitions_events * g_tw_event_msg_sz), MPI_BYTE, &status);
        b += my_partitions[i].lp_size + (partitions_events * g_tw_event_msg_sz);
    }

    MPI_File_close(&fh);

    tw_eventq load_events;

    // Load Data
    int p;
    int l = 0;
    for (p = 0, b = buffer; p < g_io_partitions_on_rank; p++) {
        for (i = 0; i < my_partitions[p].lp_count; i++, l++) {
            io_lp_deserialize(g_tw_lp[l], b);
            ((deserialize_f)g_io_lp_types[0].deserialize)(g_tw_lp[l]->cur_state, b + lp_size, g_tw_lp[l]);
            b += lp_size + model_sizes[l];
        }
        assert(my_partitions[p].ev_count <= g_io_free_events.size);
        for (i = 0; i < my_partitions[p].ev_count; i++) {
            // SEND THESE EVENTS
            tw_event *ev = tw_eventq_pop(&g_io_free_events);
            memcpy(ev, b, g_tw_event_msg_sz);
            //undo pointer to GID conversion
            if (g_tw_mapping == LINEAR) {
                ev->src_lp = g_tw_lp[((tw_lpid)ev->src_lp) - g_tw_lp_offset];
            } else if (g_tw_mapping == CUSTOM) {
                ev->src_lp = g_tw_custom_lp_global_to_local_map((tw_lpid)ev->src_lp);
            } else {
                tw_error(TW_LOC, "RIO ERROR: Unsupported mapping");
            }
            // buffer event to send after initialization
            tw_eventq_push(&g_io_buffered_events, ev);
            printf("Buffering event with recv_ts %f\n", ev->recv_ts);
            b += g_tw_event_msg_sz;
        }
    }

    return;
}

void io_load_events(tw_pe * me) {
    int i;
    int event_count = g_io_buffered_events.size;
    for (i = 0; i < event_count; i++) {
        me->cur_event = me->abort_event;
        me->cur_event->caused_by_me = NULL;

        printf("bufferedq size: %d, freeq size: %d\n", g_io_buffered_events.size, g_io_free_events.size);
        tw_event *e = tw_eventq_pop(&g_io_buffered_events);
        tw_event_send(e);
        tw_eventq_push(&g_io_free_events, e);

        if (me->cev_abort) {
            tw_error(TW_LOC, "ran out of events during io_load_events");
        }
    }
}

void io_store_checkpoint(char * master_filename) {
    int i;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    // ASSUMPTION: A checkpoint writes LP data (only)
    // TODO: support event data writing
    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "Error: IO variables not set: # of file or # of parts\n");

    // Gather LP size data
    int lp_size = sizeof(io_lp_store);
    size_t model_sizes[g_tw_nlp];
    int sum_model_size;

    // always do this loop to allow for interleaved LP types in g_tw_lp
    // TODO: add short cut for one-type, non-dynamic models?
    for (i = 0; i < g_tw_nlp; i++) {
        int lp_type_index = g_tw_lp_typemap(g_tw_lp[i]->gid);
        model_sizes[i] = ((model_size_f)g_io_lp_types[lp_type_index].model_size)(g_tw_lp[i]->cur_state, g_tw_lp[i]);
        sum_model_size += model_sizes[i];
    }

    // Events
    int event_count = g_io_buffered_events.size;
    int sum_event_size = event_count * g_tw_event_msg_sz;

    int sum_lp_size = g_tw_nlp * lp_size;
    int sum_size = sum_lp_size + sum_model_size + sum_event_size;

    char buffer[sum_size];
    void * b;
    for (i = 0, b = buffer; i < g_tw_nlp; i++) {
        io_lp_serialize(g_tw_lp[i], b);
        int lp_type_index = g_tw_lp_typemap(g_tw_lp[i]->gid);
        ((serialize_f)g_io_lp_types[lp_type_index].serialize)(g_tw_lp[i]->cur_state, b + lp_size, g_tw_lp[i]);
        b += lp_size + model_sizes[i];
    }

    // Events need src_lp pointer to be converted to GID
    for (i = 0; i < event_count; i++) {
        tw_event *ev = tw_eventq_pop(&g_io_buffered_events);
        ev->src_lp = ev->src_lp->gid;
        memcpy(b, ev, g_tw_event_msg_sz);
        tw_eventq_push(&g_io_free_events, ev);
        b += g_tw_event_msg_sz;
    }

    g_io_partitions_on_rank = g_io_number_of_partitions / number_of_mpitasks;
    int io_partitions_per_file = g_io_number_of_partitions / g_io_number_of_files;
    int io_ranks_per_file = number_of_mpitasks / g_io_number_of_files;

    // Set up Comms
    MPI_File fh;
    MPI_Status status;
    MPI_Comm file_comm;

    int file_number = mpi_rank / io_ranks_per_file;
    int file_position = mpi_rank % io_ranks_per_file;
    MPI_Offset offset = (long long) 0;
    long long contribute = (long long) sum_size;

    MPI_Comm_split(MPI_COMM_WORLD, file_number, file_position, &file_comm);
    MPI_Exscan(&contribute, &offset, 1, MPI_LONG_LONG, MPI_SUM, file_comm);

    // Write
    char filename[100];
    sprintf(filename, "%s.data-%d", master_filename, file_number);
    MPI_File_open(file_comm, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
    MPI_File_seek(fh, offset, MPI_SEEK_SET);

    MPI_File_write(fh, &buffer, sum_size, MPI_BYTE, &status);

    MPI_File_close(&fh);

    // each rank fills in its local partition data
    io_partition my_partitions[g_io_partitions_on_rank];
    for (int i = 0; i < g_io_partitions_on_rank; ++i) {
        my_partitions[i].part = (mpi_rank * g_io_partitions_on_rank) + i;
        my_partitions[i].file = file_number;
        my_partitions[i].offset = offset;
        my_partitions[i].lp_count = g_tw_nlp / g_io_partitions_on_rank;
        my_partitions[i].lp_size = sum_lp_size + sum_model_size;
        my_partitions[i].ev_count = event_count;
    }

    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(6, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);

    for (i = 0; i < g_io_partitions_on_rank; i++) {
        printf("Rank %d wrote metadata\n\tpart %d\n\tfile %d\n\toffset %d\n\tlp size %d\n\tlp count %d\n\tevents %d\n\n", mpi_rank,
            my_partitions[i].part, my_partitions[i].file, my_partitions[i].offset,
            my_partitions[i].lp_size, my_partitions[i].lp_count, my_partitions[i].ev_count);
    }
    int psize;
    MPI_Type_size(MPI_IO_PART, &psize);
    printf("Metadata size: %d or %lu\n", psize, sizeof(io_partition));

    offset = (long long) sizeof(io_partition) * g_io_partitions_on_rank * mpi_rank;
    sprintf(filename, "%s.mh", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
    MPI_File_seek(fh, offset, MPI_SEEK_SET);

    MPI_File_write(fh, &my_partitions, g_io_partitions_on_rank, MPI_IO_PART, &status);

    // Write model size array
    offset = (long long) 0;
    contribute = (long long) g_tw_nlp;
    MPI_Exscan(&contribute, &offset, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    offset += (long long) (sizeof(io_partition) * g_io_number_of_partitions);
    MPI_File_seek(fh, offset, MPI_SEEK_SET);

    MPI_File_write(fh, model_sizes, g_tw_nlp, MPI_UNSIGNED_LONG, &status);

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
        fprintf(file, "Data Files:\t%d\n", g_io_number_of_files);
        fprintf(file, "Partitions:\t%d\n", g_io_number_of_partitions);
#ifdef RAND_NORMAL
        fprintf(file, "RAND_NORMAL\tON\n");
#else
        fprintf(file, "RAND_NORMAL\tOFF\n");
#endif
    }
}

static void io_lp_serialize (tw_lp *lp, void *buffer) {
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

    memcpy(buffer, &tmp, sizeof(io_lp_store));
}

static void io_lp_deserialize (tw_lp *lp, void *buffer) {
    int i, j;

    io_lp_store tmp;
    memcpy(&tmp, buffer, sizeof(io_lp_store));

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
