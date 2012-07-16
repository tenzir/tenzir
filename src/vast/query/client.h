#ifndef VAST_QUERY_CLIENT_H
#define VAST_QUERY_CLIENT_H

#include <cppa/cppa.hpp>
#include <ze/event.h>

namespace vast {
namespace query {

/// A simple query client.
class client : public cppa::sb_actor<client>
{
  friend class cppa::sb_actor<client>;

public:
  /// Spawns a query client.
  ///
  /// @param search The search actor.
  ///
  /// @param batch_size The number of results to display until requiring user
  /// input.
  client(cppa::actor_ptr search, unsigned batch_size);

private:
  /// Waits for console input on STDIN.
  void wait_for_user_input();

  unsigned batch_size_;
  unsigned printed_ = 0;
  bool asking_ = true;

  cppa::actor_ptr search_;
  cppa::actor_ptr query_;
  cppa::behavior operating_;
  cppa::behavior init_state;
};

} // namespace query
} // namespace vast

#endif
