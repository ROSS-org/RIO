//Elsa Gonsiorowski
//Rensselaer Polytechnic Institute
//Decemeber 13, 2013

#include "ross.h"
#include "io-config.h"

extern int g_io_number_of_partitions;
extern int g_io_number_of_files;
extern int g_io_partitions_per_rank;

typedef struct {
	int part;
	int file;
	int offset;
	int size;
	int data_count;
	int data_size;
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

// length = g_io_partitions_per_rank
extern io_partition * g_io_partitions;

// API Functions
void io_opts();
void io_register_model_version(char *sha1);
void io_init(int num_files, int num_partitions);
void io_final();
void io_read_master_header(char * master_filename);
void io_write_master_header(char * master_filename);

void io_load_checkpoint(char * master_filename);
void io_store_checkpoint(char * master_filename);

// Internal functions
void io_mpi_datatype_lp (MPI_Datatype *datatype);
void io_serialize_lp (tw_lp *lp, void *store);
void io_deserialize_lp (void *store, tw_lp *lp);

// Functions on a per LP Type basis
typedef void (*datatype_f)(MPI_Datatype *dt);
typedef void (*serialize_f)(tw_lp *lp, void *store);
typedef void (*deserialize_f)(void *store, tw_lp *lp);

typedef struct {
    datatype_f datatype;
    serialize_f serialize;
    deserialize_f deserialize;
    size_t model_size;
} io_lptype;

extern io_lptype * g_io_lp_types;
