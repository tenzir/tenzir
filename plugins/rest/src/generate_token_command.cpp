#include "rest/generate_token_command.hpp"

#include "rest/authenticator.hpp"

#include <vast/system/spawn_or_connect_to_node.hpp>

#include <caf/scoped_actor.hpp>

namespace vast::plugins::rest {

auto generate_token_command(const vast::invocation& inv,
                            caf::actor_system& system) -> caf::message {
  auto self = caf::scoped_actor{system};
  auto node_opt = vast::system::spawn_or_connect_to_node(
    self, inv.options, content(system.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node
    = std::holds_alternative<system::node_actor>(node_opt)
        ? std::get<system::node_actor>(node_opt)
        : std::get<scope_linked<system::node_actor>>(node_opt).get();
  // The typed `get_node_components()` only works for actors whose type id is
  // defined in the main vast namespace, so we have to work around manually.
  // TODO: Introduce a `component_plugin::component_name()` function so we
  // can name the component "authenticator" instead of "rest".
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

} // namespace vast::plugins::rest
