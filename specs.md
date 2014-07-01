# ROSS IO Specifications

The purpose of this system is to add checkpointing to [ROSS](http://github.com/carothersc/ROSS).
A checkpoint consists of all the LP states and events in the system. 
These data structures are organized into partitions that can be read by MPI tasks. 

This system is derived from [phastaIO](http://github.com/fuj/phastaIO).

**ALERT:**
This system has only been tested with the [gonsie/SR](http://github.com/gonsie/SR) fork of ROSS. 
While ROSSIO is being developed as an independent module, there are some modifications that will need to be implemented within the ROSS core.
Note that while ROSSIO is under active development it **may not** function with regular ROSS.

## Example

A sample model can be seen at [gonsie/phold-io](http://github.com/gonsie/phold-io).
This model is being actively developed along side ROSSIO and will reflect the current API.


## API

*Note: please see the [wiki](http://github.com/gonsie/ROSSIO/wiki) for more detailed documentation.*

### User Implemented Functions

Each LP-type and event-type must be able to be serialize and de-serialize its data. This can/should be binary data. Therefore, for each LP and event type the model-developer must implement

- Serialize function
- De-serialize function

ROSS defines LP types with an array of structs function pointers (one tw_lptype struct per LP type).
Similarly, ROSSIO will use an array of io_lptype structs, which include function pointers to the serialize and deserialize functions.
Here is the code with the relevant type definitions:

```
// LP type map and function struct
typedef void (*serialize_f)(tw_lp *lp, void *store);
typedef void (*deserialize_f)(void *store, tw_lp *lp);

typedef struct {
    serialize_f serialize;
    deserialize_f deserialize;
    size_t model_size;
} io_lptype;

extern io_lptype * g_io_lp_types;
```

### System Functions

- Load checkpoint from files
  - block reading of data
  - de-serializing of data structures
  - setting ROSS variables (?)
- Save checkpoint to files
  - serializing of data structures 
  - block writing of data
  - recording ROSS variables (?)


## File Layout Specification

- Read Me file
- Master Header file
- 1 or more Data files

### Read Me File

This is a human readable file that will **not** be parsed by the ROSSIO system.
This file contains information about the checkpoint files, including:

- How many files are in the checkpoint
- The name of the checkpoint
- The version of ROSS and ROSSIO (in git hashes)
- Date the files were written
- Other details about the ROSS config and model


### Master Header File

The master header contains all of the meta-data for the partitions. 
This file is not human readable (binary data).

Each block of metadata contians the following information on a partition:
- Partition number
- Partition file
- Partition offset (within file)
- Partition size
- Data item count
- Data item size (static value is optional)

### Data Files

The data files contain only binary data. 
There are no per-file or per-partition headers. 

The model must provide a data serializer and deserializer for each LP and event type.

## Design Decisions and Assumptions

- The master header is separated into its own file
- Data files contain one or more whole parition descriptions
- Partitions are enumerated linearly
- LPs are sequentially enumerated within partitions
- Partitions cannot be divided
- The model is responsible for providing LP and event readers and writers
- Partitions are linearly combined to form larger blocks (see image)
- Each MPI Rank has the same number of partitions

<!---
![Linear Partition Combinations](partitions.pdf)
-->

## Coding Standards

This code loosely adheres to the ROSS coding standard:

- global variables begin with a `g`
- functions and variables within this module are prefaced with `io`
- underscores are used between words (in preference to camel case)

