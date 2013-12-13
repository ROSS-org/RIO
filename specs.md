
# ROSS IO Specifications

December 13, 2013

## Design Decisions and Assumptions

- The master header is separated into its own file
- Data files contain one or more whole parition descriptions
- Partitions are enumerated linearly
- LPs are sequentially enumerated within partitions
- Partitions are linearly combined to form larger blocks
- Partitions always represent max granularity
- The model is responsible for providing LP and event readers and writers

## Layout Specification

- [Master Header file](#Master_Header)
- 1 or more Data files

### Master Header

- Total Number of Files
- Total Number of Partitions
- Location and Offset List
    - (`partition number`, `file number`, `byte offset`)

### Partition Header

- Number of LPs in partition
- Number of Events in partition
- Total Size of Partition

### Data Header

- Data type (LP or event type)
- Size

## API Specification


