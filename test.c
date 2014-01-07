#include <string.h>
#include <stdlib.h>
#include <libgen.h>
#include "mpi.h"
#include "io.h"

int main (int argc, char * argv[]) {
	MPI_Init(&argc, &argv);
	
	int self, np;
	MPI_Comm_rank(MPI_COMM_WORLD, &self);
	MPI_Comm_size(MPI_COMM_WORLD, &np);

	char *filename = dirname(argv[0]);
	strcat(filename, "/mh_test_2x4.txt");

	io_load_checkpoint(filename);

}
