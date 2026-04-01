// proj2_server.cc
#include <proj2/lib/domain_socket.h>
#include <proj2/lib/file_reader.h>
#include <proj2/lib/thread_log.h>
#include <proj2/lib/sha_solver.h>

#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <sys/sysinfo.h>

#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include <iostream>

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------

// Message queue shared between the main receiver loop and worker threads
static std::queue<std::string> msg_queue;
static pthread_mutex_t queue_mtx;
static sem_t queue_sem;

// The bound datagram endpoint — workers need it to connect back to clients
static proj2::UnixDomainDatagramEndpoint* g_endpoint = nullptr;

// Set to 1 by signal handler; workers check this to exit cleanly
static volatile sig_atomic_t g_stop = 0;

// ---------------------------------------------------------------------------
// Signal handler — only sets the flag, nothing else (async-signal-safe)
// ---------------------------------------------------------------------------
static void SignalHandler(int) {
    g_stop = 1;
}

// ---------------------------------------------------------------------------
// ParseMessage
//   Parses the binary datagram described in the spec:
//     uint32_t  reply_path_len
//     char[]    reply_path          (reply_path_len bytes, not null-terminated)
//     uint32_t  file_count
//     for each file:
//       uint32_t  path_len
//       char[]    path              (path_len bytes)
//       uint32_t  row_count
// ---------------------------------------------------------------------------
static void ParseMessage(const std::string& msg,
                         std::string* client_addr,
                         std::vector<std::string>* file_paths,
                         std::vector<std::uint32_t>* rows_per_file) {
    const char* p = msg.data();
    int n = 0;
    std::uint32_t int_value;

    // --- reply endpoint ---
    std::memcpy(&int_value, p + n, 4);
    n += 4;
    client_addr->assign(p + n, int_value);
    n += int_value;

    // --- file count ---
    std::memcpy(&int_value, p + n, 4);
    n += 4;
    std::uint32_t file_count = int_value;

    // --- per-file fields ---
    for (std::uint32_t i = 0; i < file_count; ++i) {
        // path length + path
        std::memcpy(&int_value, p + n, 4);
        n += 4;
        std::string path(p + n, int_value);
        n += int_value;
        file_paths->push_back(path);

        // row count
        std::memcpy(&int_value, p + n, 4);
        n += 4;
        rows_per_file->push_back(int_value);
    }
}

// ---------------------------------------------------------------------------
// Worker thread routine
//   Each thread:
//     1. Waits on the semaphore for a message
//     2. Pops the message under the mutex
//     3. Parses it
//     4. Checks out solvers FIRST (deadlock prevention — consistent ordering)
//     5. Checks out readers (holding solvers already)
//     6. Calls ReaderHandle::Process to get hashes
//     7. Checks in readers, then solvers
//     8. Connects back to client via stream socket and sends all hashes
// ---------------------------------------------------------------------------
static void* WorkerRoutine(void*) {
    for (;;) {
        // Wait for work; sem_wait may return EINTR — loop until we get it
        while (sem_wait(&queue_sem) != 0) { /* retry on EINTR */ }

        // Check termination flag after waking
        if (g_stop) break;

        // Pop one message from the queue
        pthread_mutex_lock(&queue_mtx);
        std::string msg = msg_queue.front();
        msg_queue.pop();
        pthread_mutex_unlock(&queue_mtx);

        // Parse the binary message
        std::string client_addr;
        std::vector<std::string> file_paths;
        std::vector<std::uint32_t> rows_per_file;
        ParseMessage(msg, &client_addr, &file_paths, &rows_per_file);

        std::uint32_t file_count = file_paths.size();

        // Determine how many solvers and readers to request.
        // The spec says rows_per_file[i] is the number of rows (seeds) per
        // file, and we need one solver per row across all files (or at least
        // one per file). We request one solver per file as the parallelism
        // unit since each ReaderHandle uses its solver set per file.
        // We checkout solvers FIRST to enforce consistent lock ordering and
        // prevent deadlock.
        std::uint32_t num_solvers = (file_count > 0) ? file_count : 1;
        std::uint32_t num_readers = file_count;

        // --- Checkout solvers first (deadlock prevention) ---
        proj2::SolverHandle solver = proj2::ShaSolvers::Checkout(num_solvers);

        // --- Then checkout readers ---
        proj2::ReaderHandle reader = proj2::FileReaders::Checkout(num_readers, &solver);

        // --- Process files into hashes ---
        // Library requires file_hashes to be pre-sized to num_files
        std::vector<std::vector<proj2::ReaderHandle::HashType>> file_hashes(file_count);
        reader.Process(file_paths, rows_per_file, &file_hashes);

        // --- Release resources in reverse order (readers first, then solvers) ---
        proj2::FileReaders::Checkin(std::move(reader));
        proj2::ShaSolvers::Checkin(std::move(solver));

        // --- Concatenate all hashes in file order ---
        std::string result;
        for (std::uint32_t i = 0; i < file_count; ++i) {
            for (const auto& hash : file_hashes[i]) {
                result.append(hash.data(), 64);
            }
        }

        // --- Connect back to the client via a stream socket and send result ---
        proj2::UnixDomainStreamClient stream_client(client_addr);
        stream_client.Init();  // connect
        stream_client.Write(result.data(), result.size());
    }

    return nullptr;
}

// ---------------------------------------------------------------------------
// main
//   argv[1] = socket path
//   argv[2] = number of file reader threads
//   argv[3] = log file (unused beyond satisfying the interface)
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <socket_path> <num_threads> <log_file>" << std::endl;
        return 1;
    }

    std::string socket_path = argv[1];
    int num_threads = std::stoi(argv[2]);

    // Install signal handlers before anything else
    signal(SIGINT,  SignalHandler);
    signal(SIGTERM, SignalHandler);

    // Initialize synchronization primitives
    pthread_mutex_init(&queue_mtx, nullptr);
    sem_init(&queue_sem, 0, 0);

    // Get the number of CPU cores for solver pool sizing
    int num_cores = get_nprocs();

    // Initialize resource pools
    // ShaSolvers: one pool sized to the number of CPU cores
    proj2::ShaSolvers::Init(num_cores);
    // FileReaders: sized to the requested number of threads
    proj2::FileReaders::Init(num_threads);

    // Bind the datagram endpoint
    proj2::UnixDomainDatagramEndpoint endpoint(socket_path);
    endpoint.Init();
    g_endpoint = &endpoint;

    // Spawn worker threads
    std::vector<pthread_t> threads(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        pthread_create(&threads[i], nullptr, WorkerRoutine, nullptr);
    }

    // Main receive loop: block waiting for datagrams, push to queue
    for (;;) {
        if (g_stop) break;

        std::string peer;
        // RecvFrom blocks; returns the raw datagram bytes
        std::string msg = endpoint.RecvFrom(&peer, 65000);

        pthread_mutex_lock(&queue_mtx);
        msg_queue.push(msg);
        pthread_mutex_unlock(&queue_mtx);
        sem_post(&queue_sem);
    }

    // Wake all threads so they see g_stop and exit
    for (int i = 0; i < num_threads; ++i) {
        sem_post(&queue_sem);
    }

    // Join all threads
    for (int i = 0; i < num_threads; ++i) {
        pthread_join(threads[i], nullptr);
    }

    // Clean up
    pthread_mutex_destroy(&queue_mtx);
    sem_destroy(&queue_sem);

    return 0;
}