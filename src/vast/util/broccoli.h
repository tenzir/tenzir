#ifndef VAST_UTIL_BROCCOLI_H
#define VAST_UTIL_BROCCOLI_H

#include "vast/actor.h"
#include "vast/util/server.h"

namespace vast {

class event;

namespace util {
namespace broccoli {

struct bro_conn;
struct connection;

typedef util::server<connection> server;
typedef std::function<void(event)> event_handler;

/// Initializes Broccoli. This function must be called before any other call
/// into the Broccoli library.
/// @param messages If `true`, show message contents and protocol details.
/// @param calltrace If `true`, enable call tracing.
void init(bool messages = false, bool calltrace = false);

/// A Broccoli connection actor.
class connection : actor<connection>
{
public:
  /// Spawns a new Broccoli connection.
  /// @param The input stream to read data from.
  /// @param The output stream to read data from.
  connection(cppa::io::input_stream_ptr in,
             cppa::io::output_stream_ptr out);

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

  void act();
  char const* description() const;

private:
  struct bro_conn* bc_;
  cppa::io::input_stream_ptr in_;
  cppa::io::output_stream_ptr out_;
  event_handler event_handler_;
};

} // namespace broccoli
} // namespace vast
} // namespace util

#endif
