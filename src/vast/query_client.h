#ifndef VAST_QUERY_CLIENT_H
#define VAST_QUERY_CLIENT_H

#include <deque>
#include <cppa/cppa.hpp>
#include <ze/fwd.h>

namespace vast {

/// A simple query client.
class query_client : public cppa::sb_actor<query_client>
{
  friend class cppa::sb_actor<query_client>;

public:
  /// Spawns a query client.
  ///
  /// @param search The search actor.
  ///
  /// @param expression The query expression.
  ///
  /// @param batch_size The number of results to display until requiring user
  /// input.
  query_client(cppa::actor_ptr search,
               std::string const& expression,
               uint32_t batch_size);

private:
  bool running_ = true;
  size_t buffer_size_ = 50; // FIXME: make configurable.
  std::deque<cppa::cow_tuple<ze::event>> results_;
  cppa::actor_ptr search_;
  cppa::actor_ptr query_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
