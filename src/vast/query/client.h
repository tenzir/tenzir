#ifndef VAST_QUERY_CLIENT_H
#define VAST_QUERY_CLIENT_H

#include <cppa/cppa.hpp>
#include <ze/event.h>
#include <ze/util/queue.h>

namespace vast {
namespace query {

/// A simple query client.
class client : cppa::sb_actor<client>
{
public:
  /// Sets the initial behavior.
  client();
  cppa::behavior init_state;

private:
  /// Waits for console input on STDIN.
  void wait_for_input();

  /// Tries to pop and print an event from the result buffer.
  bool try_print();

  cppa::actor_ptr remote_;
  std::string query_;
  ze::util::queue<ze::event> buffer_;
  unsigned batch_size_ = 10;
  unsigned printed_ = 0;
  bool asking_ = true;
};

} // namespace query
} // namespace vast

#endif
