#ifndef VAST_QUERY_CLIENT_H
#define VAST_QUERY_CLIENT_H

#include <cppa/cppa.hpp>

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
  cppa::actor_ptr search_;
  cppa::actor_ptr query_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
