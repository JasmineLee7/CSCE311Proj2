// This is your file.
//
#include <proj2/lib/domain_socket.h>
#include <proj2/lib/file_reader.h>
#include <proj2/lib/thread_log.h>
#include <proj2/lib/timings.h>
#include <proj2/lib/sha_solver.h>
#include <sys/sysinfo.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>

#include <cstring>
#include <iostream>
#include <cstring>
#include <string>
#include <queue>
#include <vector>
#include <cstdint>
#include <algorithm>

// g
std::queue<std::string> msg_queue;
sem_t msg_semaphore:
pthread_mutex_t mtx;
volatile sig_atomic_t signal_status = 0;

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <socket_path> <num_threads> <log_file>" << std::endl;
        return 1;
    }
    sem_init(&msg_semaphore, 0, 0); // Initialize semaphore
        pthread_mutex_init(&mtx, nullptr); // Initialize mutex
    
        std::string socket_path = argv[1];
        int num_threads = std::stoi(argv[2]);
        std::string log_file = argv[3];
    
        // Create threads
        std::vector<pthread_t> threads(num_threads);
        for (int i = 0; i < num_threads; ++i) {
            pthread_create(&threads[i], NULL, StartRoutine, NULL);
        }

    signal(SIGINT, SignalHandler); // signal interput 
    signal(SIGTERM, SignalHandler); // signal termination

    proj2::FileReaders::FileReaders::Init(file_readers):
    proj2::ShaSolvers::Init(sha_solvers); // Initialize ShaSolvers with the number of CPU cores

    std::string server_name = argv[1]:
    proj2::UnixDomainDatagramEndpoint endpoint(server_name);
    endpoint.Init();


    std::vector<pthread_t> threads(n);
    // initialize threads
    for(std::size_t i = 0; i < threads.size(); ++i) {
        pthread_create(&threads[i], nullptr, StartRoutine, nullptr);
    }

    for(;;){
        std::string garbage;
        std::string msg endpoint.RecvForm(&garbage, 65000);
        pthread_mutex_lock(&mtx); //revive mesage lock mutex
        msg_queue.push(msg); // push message to queue
        pthread_mutex_unlock(&mtx); // message pushed, unlock mutex
        sem_post(&msg_semaphore); // message pushed, call semaphore
    }

    for(std:size_t i = 0; i < threads.size(); ++i) {
        pthread_join(threads[i], nullptr);
    }
    pthread_mutex_destroy(&mtx); // destroy mutex
    sem_destroy(&msg_semaphore); // destroy semaphore

    return 0;
} 


void *StartRoutine(void*) {
   for(;;){
       if(signal_status) break;
       sem_wait(&msg_semaphore); // wait for message to be posted
       pthread_mutex_lock(&mtx); // lock mutex to access message queue
       std::string msg = msg_queue.front(); // get message from queue
       msg_queue.pop(); // pop message from queue
       pthread_mutex_unlock(&mtx); // unlock mutex after accessing message queue
       std::string client_addr; 
       std::vector<std::string> file_paths;
       std::vector<std::uint32_t> rows_per_file;
       ParseMessage(msg, &client_addr, &file_paths, &rows_per_file);
}
}

void ParseMessage(const std::string& msg, std::string* client_addr, std::vector<std::string>* file_paths, std::vector<std::uint32_t>* rows_per_file) {
    // Implement the logic to parse the message and populate client_addr, file_paths, and rows_per_file
    // This is a placeholder implementation and should be replaced with actual parsing logic
    // For example, you can use stringstream to split the message based on a delimiter

    int n = 0;
    const char* c_ptr = msg.data();
    uint32_t int_value;
    std::memcpy(&int_value, c_ptr + n, 4);
    n+= 4;
    client_addr->assign(c_ptr + n, int_value);
    n+= int_value;
    std::memcpy(&int_value, c_ptr + n, 4);
    
}