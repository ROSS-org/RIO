#ifndef INC_io_h
#define INC_io_h

//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

// ** Global IO System variables ** //

// Set with command line --io-files
// should be consistent across the system
extern int g_io_number_of_files;
// Set with command line --io_store
extern unsigned int g_io_store;
// Set with command line --io-evt-ts-mode
// Determines how event timestamps are serialized, default Mode 0 makes the serialize timestamp be (evt_ts - g_tw_ts_end)
// Mode 1 leaves the timestamp unaltered, i.e. serialized timestamp = evt_ts
extern unsigned int g_io_evt_ts_mode;



// Register opts with ROSS
extern const tw_optdef io_opts[5];

enum io_load_e {
	NONE,		// default value
	PRE_INIT,	// load LPs then lp->init
	INIT,		// load LPs instead lp->init
	POST_INIT,	// load LPs after lp->init
};
typedef enum io_load_e io_load_type;
extern io_load_type g_io_load_at;
extern char g_io_checkpoint_name[1024];

// Should be set in main, before call to io_init
// Maximum number of events that will be scheduled past end time
extern int g_io_events_buffered_per_rank;

// ** API Functions, Types, and Variables ** //

void io_register_model_version(char *sha1);
void io_init();

void io_load_checkpoint(char * master_filename, io_load_type load_at);
void io_store_checkpoint(char * master_filename, int data_file_number);
void io_appending_job();

// LP type map and function struct
typedef void (*serialize_f)(void * state, void * buffer, tw_lp *lp);
typedef void (*deserialize_f)(void * state, void * buffer, tw_lp *lp);
typedef size_t (*model_size_f)(void * state, tw_lp *lp);

typedef struct io_lptype io_lptype;

struct io_lptype{
    serialize_f serialize;
    deserialize_f deserialize;
    model_size_f model_size;
};

extern io_lptype * g_io_lp_types;
extern io_lptype * io_lp_type_registry;


// ** Codes simulator integration API ** //

/* Initializes the size of the g_io_lp_types so that lptypes can be registered
* 		This is necessary as complicated models may not have a singular location where all LP types
* 		can be defined or referenced in a single file. Overcomes scope issues with the Codes simulator
* @param total_lp_types the total number of unique LP types that need their own io_lptype struct
* @note LP types that are derivative and super'd by the codes modelnet_base_lp should not be
*		considered in this count. They are considered modelnet_base_lps.
*/
void io_init_lp_types(size_t total_lp_types);

/* Registers an io_lptype with RIO. Not necessary if g_io_lp_types is manually defined.
* 		This is necessary as complicated models may not have a singular location where all LP types
* 		can be defined or referenced in a single file. Overcomes scope issues with the Codes simulator
* @param new_lptype io_lptype struct necessary to serialize/deserialize the LP related to it
* @param size_t map_position where in the g_tw_lp_typemap is the LP that relates to these io_lptypes
* @note codes does not initialize g_tw_lp_typemap or g_tw_lp_types
*/
void io_register_lp_type(io_lptype* new_lptype, size_t map_position);

void io_finalize_registry();


/* Cleans up the g_io_lp_types variable as RIO alloc's this during io_init_lp_types() */
void io_cleanup_lp_types();


// ** Internal IO types, variables, and functions ** //

typedef struct {
	int part;
	int file;
	int offset;
	int size;
	int lp_count;
	int ev_count;
} io_partition;
static int io_partition_field_count = 6;

typedef struct {
	tw_lpid gid;
	int32_t rng[12];
#ifdef RAND_NORMAL
	double tw_normal_u1;
	double tw_normal_u2;
	int tw_normal_flipflop;
#endif
} io_lp_store;

typedef struct {
	tw_bf cv;
	tw_lpid dest_lp;
	tw_lpid src_lp;
	tw_stime recv_ts;
	// NOTE: not storing tw_memory or tw_out
} io_event_store;

extern io_partition * g_io_partitions;

// Functions Called Directly from ROSS
void io_load_events(tw_pe * me);
void io_event_cancel(tw_event *e);
void io_read_checkpoint();

// SERIALIZE FUNCTIONS for LP and EVENT structs
// found in io-serialize.c
size_t io_lp_serialize (tw_lp * lp, void * buffer);
size_t io_lp_deserialize (tw_lp * lp, void * buffer);
size_t io_event_serialize (tw_event * e, void * buffer);
size_t io_event_deserialize (tw_event * e, void * buffer);

// INLINE function for buffering events past end time
extern tw_eventq g_io_buffered_events;
extern tw_eventq g_io_free_events;
extern tw_event * io_event_grab(tw_pe *pe);
#endif
