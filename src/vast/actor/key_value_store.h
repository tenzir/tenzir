#ifndef VAST_ACTOR_KEY_VALUE_STORE_H
#define VAST_ACTOR_KEY_VALUE_STORE_H

#include <set>
#include <string>

#include "vast/actor/actor.h"
#include "vast/util/radix_tree.h"

namespace vast {

/// A replicated hierarchical key-value store.
struct key_value_store : default_actor
{
  /// Spawns a backend.
  key_value_store(std::string const& seperator = "/");

  void on_exit();
  caf::behavior make_behavior() override;

  std::string const seperator_;
  util::radix_tree<caf::message> data_;
  std::set<caf::actor> peers_;
};

} // namespace vast

#endif
