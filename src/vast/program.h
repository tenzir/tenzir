#ifndef VAST_PROGRAM_H
#define VAST_PROGRAM_H

#include "config.h"

#include "vast/configuration.h"
#include "vast/comm/io.h"
#include "vast/meta/taxonomy_manager.h"
#include "vast/util/profiler.h"
#include "vast/util/queue.h"

namespace vast {

/// The main program.
class program
{
    program(program const&) = delete;
    program& operator=(program const&) = delete;

public:
    /// Constructs the program.
    program();

    /// Destroys the program.
    ~program();

    /// Initializes the program with arguments from @c main.
    /// @param argc argument counter from @c main.
    /// @param argv argument vector from @c main.
    bool init(int argc, char *argv[]);

    /// Initializes the program from a configuration file.
    /// @param filename The name of the configuration file.
    bool init(std::string const& filename);

    /// Starts VAST and block. 
    void start();

    /// Terminates VAST. If the function has not yet been called, we try to shut
    /// down all components cleanly. If already called once and VAST is still
    /// shutting down, force an exit by canceling all outstanding operations
    /// immediately.
    void stop();

    /// The last words.
    /// @return The program's exit status.
    int end();

private:
    void do_init();
    
    bool terminating_;
    int return_;

    configuration config_;
    comm::io io_;
    util::profiler profiler_;
    meta::taxonomy_manager tax_manager_;

    util::queue<std::exception_ptr> errors_;
};

} // namespace vast

#endif
