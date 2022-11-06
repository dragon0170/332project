# 332project

## Build
This command compiles protobuf file to scala class.
```shell
sbt compile
```

## Installation

```shell
sbt stage
```

## Milestones

### #1(~11/7)

- Generate unsorted data files
- Learn grpc
- Slave connects to Master with grpc

### #2(~11/13)

- Sample data from the file in each worker.
- Master determines and broadcasts sorting key ranges for each worker.

### #3(~11/20)

- Sort input files in each worker and save the sorted results into partitioned files with appropriate key ranges.

### #4(~12/4)

- Shuffle the sorted files with each other.
- Merge all sorted files in each worker and save into partitioned files with appropriate size.

## Weekly Progress

### Progress of week 1

- Kick off team meeting
- Planning for the project
    - List up the tasks
    - Set up the milestones
    - Arrange tasks thorugh 6 weeks

### Goal of week 2, 최규용

- Study how to use gensort, valsort
- Study how to use GRPC in scala
    - implement simple server, client program
- Study building a executable file with scala

### Goal of week 2, 김수빈

- Study how to use GRPC in scala
    - implement simple server, client program
- Study building a executable file with scala

### Goal of week 2, Mathis

- Study how to use GRPC in scala
    - implement simple server, client program
- Study building a executable file with scala

---

### Progress of week 2

- Study how to use gensort, valsort
- Study how to use GRPC in scala
    - implement simple server, client program
- Study building a executable file with scala

### Goal of week 3, 최규용

- set up the basic scala project
- implement master, slave app with grpc communication

### Goal of week 3, 김수빈

- print master’s ip and port
- connect slave to master with argv master address

### Goal of week 3, Mathis

- master waits and prints the list of slaves’ ip address
- send ip address from slaves to master

---

### Progress of week 3

- set up the basic scala project
- implement master, slave app with grpc communication
- send ip address from slaves to master
- master stores ip address of slaves

### Goal of week 4, 최규용

- master waits slaves’ connection until the number of established connections is same as argv number
  - If all connections are established, master prints the list of slaves’ ip address
- set up testing automation for master, slave

### Goal of week 4, 김수빈

- print master’s ip and port
- connect slave to master with argv master ip address and port
- master should partition the ranges and send back to slave for key ranges

### Goal of week 4, Mathis

- load the files in the directory of argv -I option from the disk
- each worker samples data from the files and send to master
