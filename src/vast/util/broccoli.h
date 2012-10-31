#ifndef VAST_UTIL_BROCCOLI_H
#define VAST_UTIL_BROCCOLI_H

#include <ze/fwd.h>
#include "vast/util/server.h"

// Forward declaration.
struct bro_conn;

namespace vast {
namespace util {
namespace broccoli {

struct connection;
typedef util::server<connection> server;
typedef std::function<void(ze::event)> event_handler;

/// Initializes Broccoli. This function must be called before any other call
/// into the Broccoli library.
/// @param messages If `true`, show message contents and protocol details.
/// @param calltrace If `true`, enable call tracing.
void init(bool messages = false, bool calltrace = false);

/// A Broccoli connection actor.
struct connection : cppa::sb_actor<connection>
{
  /// Spawns a new Broccoli connection.
  /// @param The input stream to read data from.
  /// @param The output stream to read data from.
  connection(
      cppa::network::input_stream_ptr in,
      cppa::network::output_stream_ptr out);

  struct bro_conn* bc_;
  cppa::network::input_stream_ptr in_;
  cppa::network::output_stream_ptr out_;
  event_handler event_handler_;
  //std::vector<ze::event> events_;
  cppa::behavior init_state;
};

} // namespace broccoli
} // namespace vast
} // namespace util

#endif
