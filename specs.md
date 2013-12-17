
# ROSS IO Specifications

December 13, 2013

## Design Decisions and Assumptions

- The master header is separated into its own file
- Data files contain one or more whole parition descriptions
- Partitions are enumerated linearly
- LPs are sequentially enumerated within partitions
- Partitions always represent max granularity
- The model is responsible for providing LP and event readers and writers
- Partitions are linearly combined to form larger blocks (see image)
- Each MPI Rank has the same number of partitions

![Linear Partition Combinations](partitions.pdf)

## Layout Specification

- [Master Header file](#Master_Header)
- 1 or more Data files

### Master Header

- Total Number of Files
- Total Number of Partitions
- Partition Details
    - Partition number
    - Partition file
    - Partition offset (within file)
    - Partition size
    - lp count
    - event count

### Partition Header

- Number of LPs in partition
- Number of Events in partition
- Total Size of Partition

### Data Header

- Data type (LP or event type)
- Size

## API Specification


## Coding Standards

This code loosely adheres to the ROSS coding standard:

- global variables begin with a `g`
- functions and variables within this module are prefaced with `io`
- underscores are used between words (in preference to camel case)

