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

// Forward declarations so main can see these before their definitions
void *StartRoutine(void*);
void ParseMessage(const std::string& msg,
                  std::string* client_addr,
                  std::vector<std::string>* file_paths,
                  std::vector<std::uint32_t>* rows_per_file);
void SignalHandler(int);

// Globals shared between main (producer) and worker threads (consumers)
std::queue<std::string> msg_queue;
sem_t msg_semaphore;          // counts how many messages are waiting
pthread_mutex_t mtx;          // protects msg_queue
volatile sig_atomic_t signal_status = 0;  // set by signal handler to request shutdown

void SignalHandler(int) {
    // Only async-signal-safe operations allowed here.
    // Setting a sig_atomic_t flag is safe; everything else is not.
    signal_status = 1;
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        ThreadErr("Usage: %s <socket_path> <num_threads> <num_solvers>\n", argv[0]);
        return 1;
    }

    // Install signal handlers before anything else so no signal is missed
    signal(SIGINT,  SignalHandler);
    signal(SIGTERM, SignalHandler);

    sem_init(&msg_semaphore, 0, 0);   // starts at 0; incremented each time a message arrives
    pthread_mutex_init(&mtx, nullptr);

    std::string socket_path = argv[1];
    int num_threads = std::stoi(argv[2]);
    int num_solvers = std::stoi(argv[3]);

    // Init resource pools before threads start so Checkout calls work immediately.
    // Solvers: use the count passed on the command line.
    // Readers: one per thread is a reasonable bound — each thread needs at most one reader set.
    proj2::ShaSolvers::Init(num_solvers);
    proj2::FileReaders::Init(num_threads);

    // Bind the datagram endpoint so the socket exists before threads or clients run
    proj2::UnixDomainDatagramEndpoint endpoint(socket_path);
    endpoint.Init();

    // Spawn worker threads. They block on sem_wait until messages arrive.
    std::vector<pthread_t> threads(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        pthread_create(&threads[i], nullptr, StartRoutine, nullptr);
    }

    // Main loop: receive datagrams and enqueue them for workers.
    // RecvFrom gives us the sender path (we don't need it here; client embeds reply addr in payload).
    for (;;) {
        if (signal_status) break;
        std::string sender_path;
        std::string msg = endpoint.RecvFrom(&sender_path, 65000);
        pthread_mutex_lock(&mtx);
        msg_queue.push(msg);
        pthread_mutex_unlock(&mtx);
        sem_post(&msg_semaphore);  // wake one worker
    }

    // Signal all sleeping workers to wake and check signal_status so they exit
    for (int i = 0; i < num_threads; ++i)
        sem_post(&msg_semaphore);

    for (int i = 0; i < num_threads; ++i)
        pthread_join(threads[i], nullptr);

    pthread_mutex_destroy(&mtx);
    sem_destroy(&msg_semaphore);

    return 0;
}

void ParseMessage(const std::string& msg,
                  std::string* client_addr,
                  std::vector<std::string>* file_paths,
                  std::vector<std::uint32_t>* rows_per_file) {
    // Walk the binary payload byte by byte using an offset n.
    // All integers are 4-byte uint32_t in host byte order (per the spec).
    // Strings are NOT null terminated — length always comes first.

    int n = 0;
    const char* c_ptr = msg.data();
    std::uint32_t int_value;

    // Read reply endpoint length, then the string itself
    std::memcpy(&int_value, c_ptr + n, 4);
    n += 4;
    client_addr->assign(c_ptr + n, int_value);
    n += int_value;

    // Read file count — tells us how many (path, row_count) pairs follow
    std::uint32_t file_count;
    std::memcpy(&file_count, c_ptr + n, 4);
    n += 4;

    // Read each file entry: path length, path string, row count
    for (std::uint32_t i = 0; i < file_count; ++i) {
        std::memcpy(&int_value, c_ptr + n, 4);  // path length
        n += 4;
        file_paths->push_back(std::string(c_ptr + n, int_value));  // path string
        n += int_value;

        std::uint32_t row_count;
        std::memcpy(&row_count, c_ptr + n, 4);  // row count for this file
        n += 4;
        rows_per_file->push_back(row_count);
    }
}

void *StartRoutine(void*) {
    for (;;) {
        if (signal_status) break;

        sem_wait(&msg_semaphore);  // block until a message is available

        if (signal_status) break;  // re-check after waking; could be shutdown sem_post

        pthread_mutex_lock(&mtx);
        std::string msg = msg_queue.front();
        msg_queue.pop();
        pthread_mutex_unlock(&mtx);

        std::string client_addr;
        std::vector<std::string> file_paths;
        std::vector<std::uint32_t> rows_per_file;
        ParseMessage(msg, &client_addr, &file_paths, &rows_per_file);

        // --- Resource acquisition: solvers first, then readers (prevents deadlock) ---

        // Find the largest row count across all files.
        // That is how many solvers we need: the reader for the biggest file
        // will dispatch that many SHA computations in parallel.
        std::uint32_t max_rows = *std::max_element(rows_per_file.begin(), rows_per_file.end());

        // 1. Checkout solvers first — blocks if not enough are free
        proj2::SolverHandle solver = proj2::ShaSolvers::Checkout(max_rows);

        // 2. Checkout readers second — blocks if not enough are free
        //    We need one reader slot per file
        std::uint32_t num_files = static_cast<std::uint32_t>(file_paths.size());
        proj2::ReaderHandle reader = proj2::FileReaders::Checkout(num_files, &solver);

        // --- Do the work ---

        // Process all files; result is a 2D vector: one inner vector of hashes per file
        std::vector<std::vector<proj2::ReaderHandle::HashType>> file_hashes;
        reader.Process(file_paths, rows_per_file, &file_hashes);

        // Flatten: concatenate all per-file hash vectors in file order into one buffer.
        // Each hash is exactly 64 ASCII bytes (no null terminator).
        std::string response;
        for (auto& hashes : file_hashes)
            for (auto& h : hashes)
                response.append(h.data(), 64);

        // --- Release resources: readers first, then solvers (reverse of acquisition) ---
        proj2::FileReaders::Checkin(std::move(reader));
        proj2::ShaSolvers::Checkin(std::move(solver));

        // --- Send result back to client via stream socket ---
        proj2::UnixDomainStreamClient stream(client_addr);
        stream.Init();  // connect to client's listening stream endpoint
        stream.Write(response.data(), response.size());
    }

    return nullptr;
}