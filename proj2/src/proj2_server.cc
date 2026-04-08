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
#include <string>
#include <queue>
#include <vector>
#include <cstdint>
#include <algorithm>

// holds everything parsed out of one client datagram
struct Request {
    std::string client_addr;
    std::vector<std::string> file_paths;
    std::vector<std::uint32_t> rows_per_file;
    std::uint32_t num_readers;  // number of files = readers needed
    std::uint32_t num_solvers;  // max rows across files = solvers needed
};

std::queue<Request> msg_queue;
sem_t msg_semaphore;
pthread_mutex_t mtx;
volatile sig_atomic_t signal_status = 0;

void SignalHandler(int);
void* StartRoutine(void*);
Request ParseMessage(const std::string& msg);

void SignalHandler(int) {
    // only async-signal-safe operations here
    signal_status = 1;
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        ThreadErr("Usage: %s <socket_path> <num_threads> <num_solvers>\n", argv[0]);
        return 1;
    }

    // install signal handlers before anything else
    signal(SIGINT,  SignalHandler);
    signal(SIGTERM, SignalHandler);

    sem_init(&msg_semaphore, 0, 0);
    pthread_mutex_init(&mtx, nullptr);

    std::string socket_path = argv[1];
    int num_threads = std::stoi(argv[2]);

    // use total CPU count so the pool can satisfy any single request
    proj2::ShaSolvers::Init(get_nprocs());
    proj2::FileReaders::Init(num_threads);

    proj2::UnixDomainDatagramEndpoint endpoint(socket_path);
    endpoint.Init();

    // spawn worker threads — they block on msg_semaphore until work arrives
    std::vector<pthread_t> threads(num_threads);
    for (int i = 0; i < num_threads; ++i)
        pthread_create(&threads[i], nullptr, StartRoutine, nullptr);

    // main loop: receive datagrams, parse, enqueue
    for (;;) {
        if (signal_status) break;
        std::string garbage;
        std::string msg = endpoint.RecvFrom(&garbage, 65000);
        Request req = ParseMessage(msg);
        pthread_mutex_lock(&mtx);
        msg_queue.push(req);
        pthread_mutex_unlock(&mtx);
        sem_post(&msg_semaphore); // wake one worker
    }

    // wake all sleeping workers so they can see signal_status and exit
    for (int i = 0; i < num_threads; ++i)
        sem_post(&msg_semaphore);

    for (int i = 0; i < num_threads; ++i)
        pthread_join(threads[i], nullptr);

    pthread_mutex_destroy(&mtx);
    sem_destroy(&msg_semaphore);

    return 0;
}

Request ParseMessage(const std::string& msg) {
    Request req;
    int n = 0;
    const char* c_ptr = msg.data();
    std::uint32_t int_value;

    // read reply endpoint
    std::memcpy(&int_value, c_ptr + n, 4);
    n += 4;
    req.client_addr.assign(c_ptr + n, int_value);
    n += int_value;

    // read file count
    std::uint32_t file_count;
    std::memcpy(&file_count, c_ptr + n, 4);
    n += 4;

    // read each file: path then row count
    for (std::uint32_t i = 0; i < file_count; ++i) {
        std::memcpy(&int_value, c_ptr + n, 4); // path length
        n += 4;
        req.file_paths.push_back(std::string(c_ptr + n, int_value));
        n += int_value;

        std::uint32_t row_count;
        std::memcpy(&row_count, c_ptr + n, 4);
        n += 4;
        req.rows_per_file.push_back(row_count);
    }

    // compute resource needs so StartRoutine has them ready
    req.num_readers = file_count;
    req.num_solvers = *std::max_element(req.rows_per_file.begin(),
                                        req.rows_per_file.end());
    return req;
}

void* StartRoutine(void*) {
    for (;;) {
        if (signal_status) break;

        sem_wait(&msg_semaphore); // wait for message to be posted

        if (signal_status) break; // re-check after waking

        pthread_mutex_lock(&mtx); // lock mutex to access message queue
        Request req = msg_queue.front();
        msg_queue.pop();
        pthread_mutex_unlock(&mtx); // unlock after accessing message queue

        // acquire solvers first, then readers — consistent ordering prevents deadlock
        proj2::SolverHandle solver = proj2::ShaSolvers::Checkout(req.num_solvers);
        proj2::ReaderHandle reader = proj2::FileReaders::Checkout(req.num_readers, &solver);

        // process files into hashes
        std::vector<std::vector<proj2::ReaderHandle::HashType>> file_hashes(req.num_readers);
        reader.Process(req.file_paths, req.rows_per_file, &file_hashes);

        // flatten all hashes into one response string, in file order
        std::string response;
        for (auto& hashes : file_hashes)
            for (auto& h : hashes)
                response.append(h.data(), 64);

        // release in reverse order — readers first, then solvers
        proj2::FileReaders::Checkin(std::move(reader));
        proj2::ShaSolvers::Checkin(std::move(solver));

        // connect back to client and stream the result
        proj2::UnixDomainStreamClient stream(req.client_addr);
        stream.Init();
        stream.Write(response.data(), response.size());
    }

    return nullptr;
}