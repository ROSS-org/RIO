//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "mpi.h"
#include "ross.h"
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

// Local Variables
static int l_io_init_flag = 0;

// Command Line Options
const tw_optdef io_opts[] = {
    TWOPT_GROUP("RIO"),
    TWOPT_UINT("io-files", g_io_number_of_files, "io files"),
    TWOPT_UINT("io-parts", g_io_number_of_partitions, "io partitions"),
    TWOPT_UINT("io-ppr", g_io_partitions_on_rank, "io partitions per rank"),
    TWOPT_END()
};

char model_version[41];

void io_register_model_version (char *sha1) {
    strcpy(model_version, sha1);
}

tw_event * io_event_grab(tw_pe *pe) {
    if (!l_io_init_flag) {
      // the RIO system has not been initialized
      return pe->abort_event;
    }

    tw_clock start = tw_clock_read();
    tw_event  *e = tw_eventq_pop(&g_io_free_events);

    if (e) {
        e->cancel_next = NULL;
        e->caused_by_me = NULL;
        e->cause_next = NULL;
        e->prev = e->next = NULL;

        memset(&e->state, 0, sizeof(e->state));
        memset(&e->event_id, 0, sizeof(e->event_id));
        tw_eventq_push(&g_io_buffered_events, e);
    } else {
        printf("WARNING: did not allocate enough events to RIO buffer\n");
        e = pe->abort_event;
    }
    pe->stats.s_rio += (tw_clock_read() - start);
    return e;
}

// IDENTICALLY CALLED FROM EACH MPI RANK
// SUM TOTAL GLOBAL VALUES FOR num_files AND num_partitions ON EACH RANK
void io_init_global(int global_num_files, int global_num_partitions) {
    int i;

    assert(l_io_init_flag == 0 && "ERROR: RIO system already initialized");

    if (global_num_partitions == 0) {
        global_num_partitions = tw_nnodes();
    }

    g_io_number_of_files = global_num_files;
    g_io_number_of_partitions = global_num_partitions;
    g_io_partitions_on_rank = global_num_partitions / tw_nnodes();
    l_io_init_flag = 1;
    if (g_tw_mynode == 0) {
        printf("*** IO SYSTEM INIT ***\n\tFiles: %d\n\tParts: %d\n\tPartsPerRank: %d\n\n", g_io_number_of_files, g_io_number_of_partitions, g_io_partitions_on_rank);
    }

    g_io_free_events.size = 0;
    g_io_free_events.head = g_io_free_events.tail = NULL;
    g_io_buffered_events.size = 0;
    g_io_buffered_events.head = g_io_buffered_events.tail = NULL;
}

// CALLED INDEPENDENTLY FROM EACH MPI RANK
// PER-RANK LOCAL VALUES FOR num_partitions
// ASSUME 1 FILE PER PROCESSOR
void io_init_local(int local_num_partitions) {
    int i;

    assert(l_io_init_flag == 0 && "ERROR: RIO system already initialized");

    if (local_num_partitions == 0) {
        local_num_partitions = 1;
    }

    g_io_number_of_files = tw_nnodes();
    MPI_Allreduce(&local_num_partitions, &g_io_number_of_partitions, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    g_io_partitions_on_rank = local_num_partitions;
    l_io_init_flag = 1;
    if (g_tw_mynode == 0) {
        printf("*** IO SYSTEM INIT ***\n\tFiles: %d\n\tParts: %d\n\n", g_io_number_of_files, g_io_number_of_partitions);
    }

    g_io_free_events.size = 0;
    g_io_free_events.head = g_io_free_events.tail = NULL;
    g_io_buffered_events.size = 0;
    g_io_buffered_events.head = g_io_buffered_events.tail = NULL;
}


void io_final() {
    g_io_number_of_files = 0;
    g_io_number_of_partitions = 0;
    l_io_init_flag = 0;
}

void io_read_master_header(char * master_filename) {
    FILE * master_header = fopen(master_filename, "r");
    assert(master_header && "Can not open master header to read\n");

    int num_files = 0;
    int num_partitions = 0;
    fscanf(master_header, "%d %d", &num_files, &num_partitions);

    if (!l_io_init_flag) {
    	io_init_global(num_files, num_partitions);
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
            &g_io_partitions[i].size, &g_io_partitions[i].lp_count, &g_io_partitions[i].ev_count,
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

    // TODO: check to make sure io system is init'd?

    MPI_File fh;
    MPI_Status status;
    char filename[100];

    // Read MH

    // Metadata datatype
    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(io_partition_field_count, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);
    int partition_md_size;
    MPI_Type_size(MPI_IO_PART, &partition_md_size);
    MPI_Offset offset = (long long) partition_md_size * mpi_rank * g_io_partitions_on_rank;

    io_partition my_partitions[g_io_partitions_on_rank];

    sprintf(filename, "%s.mh", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
	MPI_File_read_at_all(fh, offset, &my_partitions, g_io_partitions_on_rank, MPI_IO_PART, &status);

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
        int data_count = my_partitions[i].lp_count;
        MPI_File_read_at_all(fh, offset, &model_sizes[index], data_count, MPI_UNSIGNED_LONG, &status);
        index += data_count;
        offset += (long long) data_count;
    }

    MPI_File_close(&fh);

    // for (i = 0; i < g_io_partitions_on_rank; i++) {
    //     printf("Rank %d read metadata\n\tpart %d\n\tfile %d\n\toffset %d\n\tsize %d\n\tlp count %d\n\tevents %d\n\n", mpi_rank,
    //         my_partitions[i].part, my_partitions[i].file, my_partitions[i].offset,
    //         my_partitions[i].size, my_partitions[i].lp_count, my_partitions[i].ev_count);
    // }

    // Now data files
    for (i = 1; i < g_io_partitions_on_rank; i++) {
        assert(my_partitions[i].file == my_partitions[0].file && "ERROR: Some rank has partitions spread across files\n");
    }

    // Read file
    MPI_Comm file_comm;
    int file_number = my_partitions[0].file;
    int partitions_size = 0;
    for (i = 0; i < g_io_partitions_on_rank; i++) {
        partitions_size += my_partitions[i].size;
    }
    char buffer[partitions_size];
    void * b = buffer;
    sprintf(filename, "%s.data-%d", master_filename, file_number);

    MPI_Comm_split(MPI_COMM_WORLD, file_number, mpi_rank, &file_comm);
    MPI_File_open(file_comm, filename, MPI_MODE_RDWR, MPI_INFO_NULL, &fh);

    // FOR SOME REASON WE CAN'T DO A SINGLE READ OF MULTIPLE PARTITIONS
    for (i = 0; i < g_io_partitions_on_rank; i++){
        offset = (long long) my_partitions[i].offset;
        MPI_File_read_at_all(fh, offset, b, my_partitions[i].size, MPI_BYTE, &status);
        b += my_partitions[i].size;
    }

    MPI_File_close(&fh);

    // Load Data
    int p;
    int l = 0;
    for (p = 0, b = buffer; p < g_io_partitions_on_rank; p++) {
        for (i = 0; i < my_partitions[p].lp_count; i++, l++) {
            b += io_lp_deserialize(g_tw_lp[l], b);
            ((deserialize_f)g_io_lp_types[0].deserialize)(g_tw_lp[l]->cur_state, b, g_tw_lp[l]);
            b += model_sizes[l];
        }
        assert(my_partitions[p].ev_count <= g_io_free_events.size);
        for (i = 0; i < my_partitions[p].ev_count; i++) {
            // SEND THESE EVENTS
            tw_event *ev = tw_eventq_pop(&g_io_free_events);
            b += io_event_deserialize(ev, b);
            void * msg = tw_event_data(ev);
            memcpy(msg, b, g_tw_msg_sz);
            b += g_tw_msg_sz;
            // buffer event to send after initialization
            tw_eventq_push(&g_io_buffered_events, ev);
        }
    }

    return;
}

void io_load_events(tw_pe * me) {
    int i;
    int event_count = g_io_buffered_events.size;
    tw_stime original_lookahead = g_tw_lookahead;
    //These messages arrive before the first conservative window
    //checking for valid lookahead is unnecessary
    g_tw_lookahead = 0;
    for (i = 0; i < event_count; i++) {
        me->cur_event = me->abort_event;
        me->cur_event->caused_by_me = NULL;

        tw_event *e = tw_eventq_pop(&g_io_buffered_events);
        tw_event *n = tw_event_new(e->dest_lp, e->recv_ts, e->src_lp);
        void *emsg = tw_event_data(e);
        void *nmsg = tw_event_data(n);
        memcpy(&(n->cv), &(e->cv), sizeof(tw_bf));
        memcpy(nmsg, emsg, g_tw_msg_sz);
        tw_eventq_push(&g_io_free_events, e);
        tw_event_send(n);

        if (me->cev_abort) {
            tw_error(TW_LOC, "ran out of events during io_load_events");
        }
    }
    g_tw_lookahead = original_lookahead;
}

void io_store_multiple_partitions(char * master_filename) {
    int i, c, cur_kp;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "Error: IO variables not set: # of file or # of parts\n");

    if (g_io_partitions_on_rank > 1) {
        // Assume each KP is becoming a partition
        assert((g_tw_nkp == g_io_partitions_on_rank) && "Error: Writing a checkpoint with multiple partitions per rank with wrong number of KPs\n");
    }

    // Set up Comms
    MPI_File fh;
    MPI_Status status;
    MPI_Comm file_comm;
    int file_number = mpi_rank;
    MPI_Comm_split(MPI_COMM_WORLD, file_number, 0, &file_comm);
    MPI_Offset offset = (long long) 0;

    char filename[100];
    sprintf(filename, "%s.data-%d", master_filename, file_number);

    // ASSUMPTION FOR MULTIPLE PARTS-PER-RANK
    // Each MPI-Rank gets its own file
    g_io_partitions_on_rank = g_tw_nkp;
    io_partition my_partitions[g_io_partitions_on_rank];

    size_t all_lp_sizes[g_tw_nlp];
    int all_lp_i = 0;

    for (cur_kp = 0; cur_kp < g_tw_nkp; cur_kp++) {
        int lps_on_kp = g_tw_kp[cur_kp]->lp_count;

        // Gather LP size data
        int lp_size = sizeof(io_lp_store);
        size_t model_sizes[lps_on_kp];
        int sum_model_size = 0;

        // always do this loop to allow for interleaved LP types in g_tw_lp
        // TODO: add short cut for one-type, non-dynamic models?
        for (c = 0, i = 0; c < g_tw_nlp; c++) {
            if (g_tw_lp[c]->kp->id == cur_kp) {
                int lp_type_index = g_tw_lp_typemap(g_tw_lp[c]->gid);
                model_sizes[i] = ((model_size_f)g_io_lp_types[lp_type_index].model_size)(g_tw_lp[c]->cur_state, g_tw_lp[c]);
                sum_model_size += model_sizes[i];
                all_lp_sizes[all_lp_i] = model_sizes[i];
                i++;
                all_lp_i++;
            }
        }

        int event_count = 0;
        int sum_event_size = 0;
        if (cur_kp == 0) {
            // Event Metadata
            int event_count = g_io_buffered_events.size;
            int sum_event_size = event_count * (g_tw_msg_sz + sizeof(io_event_store));
        }

        int sum_lp_size = lps_on_kp * lp_size;
        int sum_size = sum_lp_size + sum_model_size + sum_event_size;

        // ** START Serialize **
        char buffer[sum_size];
        void * b;

        // LPs
        for (c = 0, i = 0, b = buffer; c < g_tw_nlp; c++) {
            if (g_tw_lp[c]->kp->id == cur_kp) {
                b += io_lp_serialize(g_tw_lp[c], b);
                int lp_type_index = g_tw_lp_typemap(g_tw_lp[c]->gid);
                ((serialize_f)g_io_lp_types[lp_type_index].serialize)(g_tw_lp[c]->cur_state, b, g_tw_lp[c]);
                b += model_sizes[i];
                i++;
            }
        }

        // Events
        for (i = 0; i < event_count; i++) {
            tw_event *ev = tw_eventq_pop(&g_io_buffered_events);
            b += io_event_serialize(ev, b);
            void * msg = tw_event_data(ev);
            memcpy(b, msg, g_tw_msg_sz);
            tw_eventq_push(&g_io_free_events, ev);
            b += g_tw_msg_sz;
        }

        // Write
        // in this case each MPI rank gets its own file
        int file_position = 0;
        MPI_File_open(file_comm, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
        MPI_File_write_at_all(fh, offset, &buffer, sum_size, MPI_BYTE, &status);
        MPI_File_close(&fh);

        my_partitions[cur_kp].part = cur_kp;
        my_partitions[cur_kp].file = file_number;
        my_partitions[cur_kp].offset = offset;
        my_partitions[cur_kp].size = sum_size;
        my_partitions[cur_kp].lp_count = lps_on_kp;
        my_partitions[cur_kp].ev_count = event_count;

        offset += (long long) sum_size;
    }

    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(io_partition_field_count, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);

    int psize;
    MPI_Type_size(MPI_IO_PART, &psize);

    offset = (long long) sizeof(io_partition) * g_io_partitions_on_rank * mpi_rank;
    sprintf(filename, "%s.mh", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
    MPI_File_write_at_all(fh, offset, &my_partitions, g_io_partitions_on_rank, MPI_IO_PART, &status);

    // Write model size array
    offset = (long long) 0;
    MPI_Offset contribute = (long long) g_tw_nlp;
    MPI_Exscan(&contribute, &offset, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    offset += (long long) (sizeof(io_partition) * g_io_number_of_partitions);
    MPI_File_write_at_all(fh, offset, all_lp_sizes, g_tw_nlp, MPI_UNSIGNED_LONG, &status);
    MPI_File_close(&fh);

    // WRITE READ ME
    if (mpi_rank == 0) {
        FILE *file;
        sprintf(filename, "%s.read-me.txt", master_filename);
        file = fopen(filename, "w");
        fprintf(file, "This file was auto-generated by RIO.\n\n");
#if HAVE_CTIME
        time_t raw_time;
        time(&raw_time);
        fprintf(file, "Date Created:\t%s\n", ctime(&raw_time));
#endif
        fprintf(file, "## BUILD CONFIGURATION\n\n");
#ifdef ROSS_VERSION
        fprintf(file, "ROSS Version:\t%s\n", ROSS_VERSION);
#endif
#ifdef RIO_VERSION
        fprintf(file, "RIO Version:\t%s\n", RIO_VERSION);
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
        fprintf(file, "\n## RUN TIME SETTINGS by GROUP:\n\n");
        tw_opt_settings(file);
    }
}

static size_t io_lp_serialize (tw_lp *lp, void *buffer) {
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
    return sizeof(io_lp_store);
}

static size_t io_lp_deserialize (tw_lp *lp, void *buffer) {
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
    return sizeof(io_lp_store);
}

static size_t io_event_serialize (tw_event *e, void *buffer) {
    int i;

    io_event_store tmp;

    memcpy(&(tmp.cv), &(e->cv), sizeof(tw_bf));
    tmp.dest_lp = e->dest_lp; // dest_lp is gid
    tmp.src_lp = e->src_lp->gid;
    tmp.recv_ts = e->recv_ts - g_tw_ts_end;

    memcpy(buffer, &tmp, sizeof(io_event_store));
    // printf("Storing event going to %lu at %f\n", tmp.dest_lp, tmp.recv_ts);
    return sizeof(io_event_store);
}

static size_t io_event_deserialize (tw_event *e, void *buffer) {
    int i;

    io_event_store tmp;
    memcpy(&tmp, buffer, sizeof(io_event_store));

    memcpy(&(e->cv), &(tmp.cv), sizeof(tw_bf));
    e->dest_lp = tmp.dest_lp;
    //undo pointer to GID conversion
    if (g_tw_mapping == LINEAR) {
        e->src_lp = g_tw_lp[((tw_lpid)tmp.src_lp) - g_tw_lp_offset];
    } else if (g_tw_mapping == CUSTOM) {
        e->src_lp = g_tw_custom_lp_global_to_local_map((tw_lpid)tmp.src_lp);
    } else {
        tw_error(TW_LOC, "RIO ERROR: Unsupported mapping");
    }
    e->recv_ts = tmp.recv_ts;
    // printf("Loading event going to %lu at %f\n", tmp.dest_lp, tmp.recv_ts);
    return sizeof(io_event_store);
}
