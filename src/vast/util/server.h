#ifndef VAST_UTIL_SERVER_H
#define VAST_UTIL_SERVER_H

#include <cppa/cppa.hpp>
#include <cppa/network/ipv4_acceptor.hpp>
#include "vast/util/poll.h"

namespace vast {
namespace util {

template <typename Connection>
struct server : cppa::sb_actor<server<Connection>>
{
  /// Spawns a new server and redirects new connections to a given actor.
  /// @param port The port to listen on.
  /// @param handler The actor handling freshly accepted connections.
  server(uint16_t port, cppa::actor_ptr handler)
    : acceptor(cppa::network::ipv4_acceptor::create(port, nullptr))
  {
    using namespace cppa;
    init_state = (
        on(atom("accept")) >> [=]
        {
          if (poll(acceptor->file_handle()) &&
            (auto opt = acceptor->try_accept_connection()))
          {
            auto& stream = *opt;
            auto conn = spawn<Connection>(stream.first, stream.second);
            send(handler, atom("connection"), conn);
          }
          self << self->last_dequeued();
        },
        on(atom("kill")) >> [=]
        {
          self->quit();
        });
  }

  std::unique_ptr<cppa::network::acceptor> acceptor;
  cppa::behavior init_state;
};

} // namespace util
} // namespace vast

#endif
