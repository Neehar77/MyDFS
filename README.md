# Distributed File System

Developed a file system similar to HDFS (Hadoop Distributed File System) with the following key properties:

Replication: Data replication for fault tolerance and improved availability.
Locking Service: Support for file locking to ensure data consistency in concurrent access scenarios.
Location Transparency: Abstracts file access from underlying physical storage, providing a unified view of the distributed file system.
Fault Tolerance: Resilience to node failures and data loss through replication and redundancy mechanisms.

## Features
Implementation of a distributed file system architecture inspired by HDFS.
Integration of Vue.js for an enhanced user interface, facilitating easy file management and monitoring.
Testing conducted by deploying minion servers on Google Cloud and Amazon Web Services, with the master node running locally.
