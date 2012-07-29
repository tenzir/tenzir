#ifndef VAST_PROGRAM_H
#define VAST_PROGRAM_H

#include <cppa/cppa.hpp>
#include "vast/configuration.h"

namespace vast {

/// The main program.
class program
{
    program(program const&) = delete;
    program& operator=(program const&) = delete;

public:
    /// Constructs the program.
    program();

    /// Initializes the program with arguments from @c main.
    /// @param argc argument counter from @c main.
    /// @param argv argument vector from @c main.
    /// @return `true` if initialization was successful.
    bool init(int argc, char *argv[]);

    /// Initializes the program from a configuration file.
    /// @param filename The name of the configuration file.
    /// @return `true` if initialization was successful.
    bool init(std::string const& filename = "");

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
    cppa::actor_ptr archive_;
    cppa::actor_ptr index_;
    cppa::actor_ptr tracker_;
    cppa::actor_ptr ingestor_;
    cppa::actor_ptr search_;
    cppa::actor_ptr query_client_;
    cppa::actor_ptr schema_manager_;
    cppa::actor_ptr profiler_;
};

} // namespace vast

#endif
