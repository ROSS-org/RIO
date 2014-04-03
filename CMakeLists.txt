PROJECT(ROSSIO)

SET(rossio_srcs io.h)

OPTION(USE_MPI_IO "Use MPI for I/O operations? Defaults to POSIX" OFF)
IF(USE_MPI_IO)
	SET(rossio_srcs ${rossio_srcs} io_mpi.c)
ELSE(USE_MPI_IO)
	SET(rossio_srcs ${rossio_srcs} io_posix.c)
ENDIF(USE_MPI_IO)

ADD_LIBRARY(ROSSIO ${rossio_srcs})

# find ross.h
INCLUDE_DIRECTORIES(${ROSS_SOURCE_DIR} ${ROSS_BINARY_DIR})
TARGET_LINK_LIBRARIES(ROSSIO ROSS)
