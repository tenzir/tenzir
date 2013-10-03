#ifndef VAST_UTIL_SERVER_H
#define VAST_UTIL_SERVER_H

#include <cppa/cppa.hpp>
#include <cppa/io/ipv4_acceptor.hpp>
#include "vast/actor.h"
#include "vast/util/poll.h"

namespace vast {
namespace util {

template <typename Connection>
class server : actor<server<Connection>>
{
public:
  /// Spawns a new server and redirects new connections to a given actor.
  /// @param port The port to listen on.
  /// @param handler The actor handling freshly accepted connections.
  server(uint16_t port, cppa::actor_ptr handler)
    : acceptor_{cppa::io::ipv4_acceptor::create(port, nullptr)},
      handler_{std::move(handler)}
  {
  }

  void act()
  {
    using namespace cppa;
    become(
        on(atom("accept")) >> [=]
        {
          if (poll(acceptor_->file_handle()))
          {
            if (auto opt = acceptor_->try_accept_connection())
              send(handler_,
                   atom("connection"),
                   spawn<Connection>(opt->first, opt->second));
          }
          self << self->last_dequeued();
        });
  }

  char const* description() const
  {
    return "server";
  }

private:
  std::unique_ptr<cppa::io::acceptor> acceptor_;
  cppa::actor_ptr handler_;
};

} // namespace util
} // namespace vast

#endif
