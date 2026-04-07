# CSCE311Proj2
CSCE 311 project 2
This project implements a multi-threaded server using POSIX threads and Unix domain sockets to handle SHA-256 hash requests from clients concurrently.
The server parses each client datagram into a Request struct containing the client reply address, file paths, rows per file, and the number of readers and solvers needed. Requests are pushed onto a shared queue and worker threads are woken via semaphore to process them.
Each worker checks out solvers first then readers, processes the files into 64-byte ASCII hashes, flattens the results, and streams them back to the client over a stream socket. Resources are always released in reverse order. This consistent acquisition ordering prevents deadlock.
Signal handlers for SIGINT and SIGTERM set a shared flag that causes all threads to exit cleanly.