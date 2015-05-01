#ifndef INC_io_h
#define INC_io_h

//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include "ross.h"

// ** Global IO System variables ** //

// Set with command line --io-parts
// should be consistent across the system
extern int g_io_number_of_partitions;

// Set with command line --io-files
// should be consistent across the system
extern int g_io_number_of_files;

// Set with command line --io-ppr
// If set with command line, should be consistent across system
// otherwise it can vary by rank (uneven load)
extern int g_io_partitions_on_rank;

enum io_load_e {
	NONE,		// don't from checkpoint
	PRE_INIT,	// load LPs then lp->init
	INIT,		// load LPs instead lp->init
	POST_INIT,	// load LPs after lp->init
};
typedef enum io_load_e io_load_type;
extern io_load_type g_io_load_at;
extern char g_io_checkpoint_name[1024];

extern int g_io_events_buffered_per_rank;

// ** API Functions, Types, and Variables ** //

void io_opts();
void io_register_model_version(char *sha1);
void io_init(int num_files, int num_partitions);
void io_final();
void io_read_master_header(char * master_filename);
void io_write_master_header(char * master_filename);

void io_load_checkpoint(char * master_filename);
void io_load_events();
void io_store_checkpoint(char * master_filename);

// LP type map and function struct
typedef void (*serialize_f)(void * state, void * buffer, tw_lp *lp);
typedef void (*deserialize_f)(void * state, void * buffer, tw_lp *lp);
typedef size_t (*model_size_f)(void * state, tw_lp *lp);

typedef struct {
    serialize_f serialize;
    deserialize_f deserialize;
    model_size_f model_size;
} io_lptype;

extern io_lptype * g_io_lp_types;


// ** Internal IO types, variables, and functions ** //

typedef struct {
	int part;
	int file;
	int offset;
	int lp_count;
	int lp_size;
	int ev_count;
} io_partition;

typedef struct {
	tw_lpid gid;
	int32_t rng[12];
#ifdef RAND_NORMAL
	double tw_normal_u1;
	double tw_normal_u2;
	int tw_normal_flipflop;
#endif
} io_lp_store;

extern io_partition * g_io_partitions;

static void io_lp_serialize (tw_lp * lp, void * buffer);
static void io_lp_deserialize (tw_lp * lp, void * buffer);

extern tw_eventq g_io_buffered_events;
extern tw_eventq g_io_free_events;
#endif
