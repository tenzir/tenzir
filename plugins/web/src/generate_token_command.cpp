#include "web/generate_token_command.hpp"

#include "web/authenticator.hpp"

#include <tenzir/connect_to_node.hpp>

#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::web {

auto generate_token_command(const tenzir::invocation& inv,
                            caf::actor_system& system) -> caf::message {
  (void)inv;
  auto self = caf::scoped_actor{system};
  auto node_opt = tenzir::connect_to_node(self);
  if (not node_opt) {
    return caf::make_message(std::move(node_opt.error()));
  }
  const auto node = std::move(*node_opt);
  // The typed `get_node_components()` only works for actors whose type id is
  // defined in the main tenzir namespace, so we have to work around manually.
  auto timeout = caf::infinite;
  auto authenticator = get_authenticator(self, node, timeout);
  if (!authenticator)
    return caf::make_message(authenticator.error());
  auto result = caf::message{};
  self->request(*authenticator, caf::infinite, atom::generate_v)
    .receive(
      [](token_t token) {
        fmt::print("{}\n", token);
      },
      [&](const caf::error& e) {
        result = caf::make_message(e);
      });
  return result;
}

} // namespace tenzir::plugins::web
