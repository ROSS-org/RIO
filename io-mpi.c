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
int g_io_partitions_offset = 0;
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
    if (!l_io_init_flag || g_io_events_buffered_per_rank == 0) {
      // the RIO system has not been initialized
      // or we are not buffering events
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
    pe->stats.s_rio_load += (tw_clock_read() - start);
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
    MPI_Exscan(&g_io_partitions_on_rank, &g_io_partitions_offset, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
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
    int i, cur_part, rc;
    int mpi_rank = g_tw_mynode;
    int number_of_mpitasks = tw_nnodes();

    // assert that IO system has been init
    assert(g_io_number_of_files != 0 && g_io_number_of_partitions != 0 && "ERROR: IO variables not set: # of files or # of parts\n");

    // TODO: check to make sure io system is init'd?

    MPI_File fh;
    MPI_Status status;
    char filename[257];

    // Read MH

    // Metadata datatype
    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(io_partition_field_count, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);
    int partition_md_size;
    MPI_Type_size(MPI_IO_PART, &partition_md_size);
    MPI_Offset offset = (long long) partition_md_size * g_io_partitions_offset;


    io_partition my_partitions[g_io_partitions_on_rank];

    sprintf(filename, "%s.mh", master_filename);
    rc = MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
    if (rc != 0) {
        printf("ERROR: could not MPI_File_open %s\n", filename);
    }
	MPI_File_read_at_all(fh, offset, &my_partitions, g_io_partitions_on_rank, MPI_IO_PART, &status);
    MPI_File_close(&fh);

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
    offset *= sizeof(size_t);

    size_t * model_sizes = (size_t *) calloc(g_tw_nlp, sizeof(size_t));
    int index = 0;

    sprintf(filename, "%s.lp", master_filename);
    rc = MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
    if (rc != 0) {
        printf("ERROR: could not MPI_File_open %s\n", filename);
    }
    for (cur_part = 0; cur_part < g_io_partitions_on_rank; cur_part++){
        int data_count = my_partitions[cur_part].lp_count;
        MPI_File_read_at(fh, offset, &model_sizes[index], data_count, MPI_UNSIGNED_LONG, &status);
        index += data_count;
        offset += (long long) data_count * sizeof(size_t);
    }
    MPI_File_close(&fh);

    // for (i = 0; i < g_io_partitions_on_rank; i++) {
    //     printf("Rank %d read metadata\n\tpart %d\n\tfile %d\n\toffset %d\n\tsize %d\n\tlp count %d\n\tevents %d\n\n", mpi_rank,
    //         my_partitions[i].part, my_partitions[i].file, my_partitions[i].offset,
    //         my_partitions[i].size, my_partitions[i].lp_count, my_partitions[i].ev_count);
    // }

    // DATA FILES
    int all_lp_i = 0;
    for (cur_part = 0; cur_part < g_io_partitions_on_rank; cur_part++) {
        // Read file
        char buffer[my_partitions[cur_part].size];
        void * b = buffer;
        sprintf(filename, "%s.data-%d", master_filename, my_partitions[cur_part].file);

        // Must use non-collectives, can't know status of other MPI-ranks
        rc = MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
        if (rc != 0) {
            printf("ERROR: could not MPI_File_open %s\n", filename);
        }
        MPI_File_read_at(fh, (long long) my_partitions[cur_part].offset, buffer, my_partitions[cur_part].size, MPI_BYTE, &status);
        MPI_File_close(&fh);

        // Load Data
        for (i = 0; i < my_partitions[cur_part].lp_count; i++, all_lp_i++) {
            b += io_lp_deserialize(g_tw_lp[all_lp_i], b);
            ((deserialize_f)g_io_lp_types[0].deserialize)(g_tw_lp[all_lp_i]->cur_state, b, g_tw_lp[all_lp_i]);
            b += model_sizes[all_lp_i];
        }
        assert(my_partitions[cur_part].ev_count <= g_io_free_events.size);
        for (i = 0; i < my_partitions[cur_part].ev_count; i++) {
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

    free(model_sizes);

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

void io_store_multiple_partitions(char * master_filename, int append_flag, int data_file_number) {
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
    int file_number = data_file_number;
    MPI_Comm_split(MPI_COMM_WORLD, file_number, 0, &file_comm);
    MPI_Offset offset = (long long) 0;

    char filename[256];
    sprintf(filename, "%s.data-%d", master_filename, file_number);

    // ASSUMPTION FOR MULTIPLE PARTS-PER-RANK
    // Each MPI-Rank gets its own file
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
        MPI_File_open(file_comm, filename, MPI_MODE_CREATE | MPI_MODE_UNIQUE_OPEN | MPI_MODE_WRONLY | MPI_MODE_APPEND, MPI_INFO_NULL, &fh);
        MPI_File_write(fh, &buffer, sum_size, MPI_BYTE, &status);
        MPI_File_close(&fh);

        my_partitions[cur_kp].part = cur_kp;
        my_partitions[cur_kp].file = file_number;
        my_partitions[cur_kp].offset = offset;
        my_partitions[cur_kp].size = sum_size;
        my_partitions[cur_kp].lp_count = lps_on_kp;
        my_partitions[cur_kp].ev_count = event_count;

        offset += (long long) sum_size;
    }

    MPI_Comm_free(&file_comm);

    int amode;
    if (append_flag) {
        amode = MPI_MODE_CREATE | MPI_MODE_UNIQUE_OPEN | MPI_MODE_RDWR | MPI_MODE_APPEND;
    } else {
        amode = MPI_MODE_CREATE | MPI_MODE_UNIQUE_OPEN | MPI_MODE_RDWR;
    }

    // Write Metadata
    MPI_Datatype MPI_IO_PART;
    MPI_Type_contiguous(io_partition_field_count, MPI_INT, &MPI_IO_PART);
    MPI_Type_commit(&MPI_IO_PART);

    int psize;
    MPI_Type_size(MPI_IO_PART, &psize);

    offset = (long long) sizeof(io_partition) * g_io_partitions_on_rank * mpi_rank;
    sprintf(filename, "%s.mh", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, amode, MPI_INFO_NULL, &fh);
    MPI_File_write(fh, &my_partitions, g_io_partitions_on_rank, MPI_IO_PART, &status);
    MPI_File_close(&fh);

    // Write model size array
    offset = (long long) 0;
    MPI_Offset contribute = (long long) g_tw_nlp;
    MPI_Exscan(&contribute, &offset, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    sprintf(filename, "%s.lp", master_filename);
    MPI_File_open(MPI_COMM_WORLD, filename, amode, MPI_INFO_NULL, &fh);
    MPI_File_write(fh, all_lp_sizes, g_tw_nlp, MPI_UNSIGNED_LONG, &status);
    MPI_File_close(&fh);

    if (append_flag == 1) {
        printf("%d parts written\n", g_io_partitions_on_rank);
    }

    // WRITE READ ME
    if (mpi_rank == 0 && (append_flag == 0 || data_file_number == 0) ) {
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
        if (append_flag == 0) {
            fprintf(file, "Data Files:\t%d\n", g_io_number_of_files);
            fprintf(file, "Partitions:\t%d\n", g_io_number_of_partitions);
        } else {
            fprintf(file, "Data Files:\t%d+?\n", g_io_number_of_files);
            fprintf(file, "Partitions:\t%d+?\n", g_io_number_of_partitions);
        }
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
