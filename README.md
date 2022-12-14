# 332project

## Requirements
- JDK v1.8.0
- Scala v2.13.10
- sbt v1.7.3

## Build
This command compiles protobuf file to scala class.
```shell
sbt compile
```

## Installation

```shell
sbt stage
```

## How to Run Applications
You have to install first.
Then, you need to move to installed directory.
```shell
cd target/universal/stage/bin/
```
Finally, you can run master and slave there.
```shell
# Master
./master <# of workers>

# Slave
./slave <master IP:port> -I <input directory> <input directory> ... <input directory> -O <output directory> 

```

## Test
### SBT Test
Not working now. we have to fix it.
```shell
sbt test
```

### Docker Integration Test
```shell
./docker.sh
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

---

### Progress of week 4

- Implement handshaking feature between master and slave
  - flow diagram - [https://drive.google.com/file/d/1NAOFlg2oOSw5aHK3nLS4VR87bHtnB8Ry/view?usp=sharing](https://drive.google.com/file/d/1NAOFlg2oOSw5aHK3nLS4VR87bHtnB8Ry/view?usp=sharing)
  - print master’s ip and port
  - connect slave to master with argv master ip address and port
  - master waits slaves’ connection until the number of established connections is same as argv number
    - If all connections are established, master prints the list of slaves’ ip address
- Get a file from the disk and send data of the file as string with grpc from slave to master

### Goal of week 5, 최규용

- Add logging system
- Design distributed sorting system and architecture of master, slave roughly
- Set up testing automation for master, slave

### Goal of week 5, 김수빈

### Goal of week 5, Mathis

- Load whole files in the directory with argv -I option from the disk
- Sort one file and save the sorted result into another file

---

### Progress of week 5

- Add logging system with log4j
- Design distributed sorting system and architecture of master, slave roughly
  - [https://app.diagrams.net/#G1SG7lDIkw_nj-3UxaPZ5yxJTotxETJOMu](https://app.diagrams.net/#G1SG7lDIkw_nj-3UxaPZ5yxJTotxETJOMu)
- Make a progress presentation
  - progress.pdf at root directory
- Slave extracts 1MB of data from a gensort file and send it to master. Master collects sample data from all slaves and send back the response with partitioned key ranges.
  - [https://github.com/dragon0170/332project/pull/5](https://github.com/dragon0170/332project/pull/5)
  - Implementing key range partitioning algorithm is in progress
- Set up test code for grpc service
  - [https://github.com/dragon0170/332project/pull/8](https://github.com/dragon0170/332project/pull/8)
- Parse input(I), output(O) argument of slave application. Get files from input directory and do external sorting with them and save the result to a file in output directory.
  - [https://github.com/dragon0170/332project/pull/7](https://github.com/dragon0170/332project/pull/7)
  - Reviewing and testing for the code is in progress

### Goal of week 6, 최규용

- Set up testing automation with docker
- Design detailed messages and procedure for handshaking, sampling, sorting
- Merge the partitioning part and test on docker
- Assignment NodeScala

### Goal of week 6, 김수빈

- Finish implementing external sorting
  - Add test code
  - Save partitioned output files in the output directory
- Assignment NodeScala

### Goal of week 6, Mathis

- Finish implementing the paritioning part in master application
- Add test code for partitioning job
- Assignment NodeScala

---

### Progress of week 6

Not much progress than previous week because of the last assignment.

- Partitioning job - [https://github.com/dragon0170/332project/pull/5](https://github.com/dragon0170/332project/pull/5)
  - change type from string to bytes in protobuf SendDataRequest
  - fix bugs of paritioning key ranges in master
- Add automated testing - [https://github.com/dragon0170/332project/pull/8](https://github.com/dragon0170/332project/pull/8)
  - set up master, slave testing automation with docker compose
  - set up CI(Continuous Integration) with Github Actions
  - fix bugs of grpc blocking stub at linux environments

### Goal of week 7, 최규용

- Design detailed messages and procedure for distributed sort
- Review and merge the partitioning pull request and test with gensort files on docker environments
- Add skeleton code for shuffling job(set up grpc server at slave, define sequence of function calls)

### Goal of week 7, 김수빈

- Finish implementing external sorting
  - Add test code
  - Save partitioned output files in the output directory
- Review the detailed design of distributed sort

### Goal of week 7, Mathis

- Add test code for partitioning job and merge the partitioning pull request
- Review the detailed design of distributed sort

---

### Progress of week 7

- Design and review the distributed sorting system
  - [https://github.com/dragon0170/332project/issues/9](https://github.com/dragon0170/332project/issues/9)
  - Add detailed message protocol and add skeleton code for entire process
- Add test code for partitioning task
  - [https://github.com/dragon0170/332project/pull/5](https://github.com/dragon0170/332project/pull/5)

### Goal of week 8, 최규용

- Resolve merge conflicts and merge the pull request of partitioning task
- Implement sorting stage as described in design document
- Implement merging stage as described in design document
- Last meeting at saturday afternoon
  - merge all pull requests
  - do integration tests with docker
  - fix bugs

### Goal of week 8, 김수빈

- Not attended at weekly meeting and no response from kakaotalk
- (TBD)Finish implementing external sorting
- (TBD)Last meeting at saturday afternoon

### Goal of week 8, Mathis

- Implement shuffling stage as described in design document
- Last meeting at saturday afternoon
  - merge all pull requests
  - do integration tests with docker
  - fix bugs
