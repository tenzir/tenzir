#include "vast/api/vast_api.h"

#include "vast/config.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/connect_to_node.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/fwd.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

struct VAST {
  caf::actor_system sys;
  vast::system::node_actor node;
  std::string endpoint;
};

// FIXME: catch exceptions and return nullptr
extern "C" VAST* vast_open(const char* endpoint) {
  auto result = static_cast<VAST*>(malloc(sizeof(VAST)));

  caf::actor_system_config cfg;
  new (&result->sys) caf::actor_system{cfg};

  auto self = caf::scoped_actor{result->sys};

  caf::settings vast_cfg;
  caf::put(vast_cfg, "vast.endpoint", endpoint);
  vast::system::connect_to_node(self, vast_cfg);

  return result;
}

// Return 0 on success
extern "C" int vast_metrics(VAST*, struct vast_metrics* out) {
  out->version = vast::version::version;
  return 0;
}

// Closes the connection
extern "C" void vast_close(VAST*) {
  // FIXME: Close connection
}