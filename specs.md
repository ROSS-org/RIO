# ROSS IO Specifications

The purpose of this system is to add checkpointing to [ROSS](http://github.com/carothersc/ROSS). A checkpoint consists of all the LP states and events in the system. These data structures are organized into partitions that can be read by MPI tasks. 

This system is derived from [phastaIO](http://github.com/fuj/phastaIO).

## API

### User Implemented Functions

Each LP-type and event-type must be able to be serialize and de-serialize its data. This can/should be binary data. Therefore, for each LP and event type the model-developer must implement:

- Serialize function
- De-serialize function


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

- Master Header file
- 1 or more Data files

### Master Header File

The master header contains all of the meta-data for the partitions. 
This file is human readable, but is explicitly defined and contains only numbers.

The first line contains:
- Total Number of Files
- Total Number of Partitions
- Size of meta-data for a single partition (for block reading)

Each of the following lines contians the meta-data for one partition:
- Partition number
- Partition file
- Partition offset (within file)
- Partition size
- LP count
- Event count

### Data Files

The data files contain only binary data. 
There are no per-file or per-partition headers. 
However, each segment of data (LP or event) has its own header:

- Data type (LP or event type)
- Size

The model must provide a data reader and writer for each LP and event type.

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

## Use Cases and Tests
