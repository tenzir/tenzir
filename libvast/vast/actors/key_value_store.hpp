#ifndef VAST_ACTOR_KEY_VALUE_STORE_HPP
#define VAST_ACTOR_KEY_VALUE_STORE_HPP

#include <set>

#include "vast/filesystem.hpp"
#include "vast/none.hpp"
#include "vast/actor/basic_state.hpp"
#include "vast/util/radix_tree.hpp"

namespace vast {

/// A replicated hierarchical key-value store.
struct key_value_store {
  using storage = util::radix_tree<message>;

  struct state : basic_state {
    state(local_actor* self);

    storage data;
    util::radix_tree<none> persistent;
    actor leader;
    std::set<actor> followers;
  };

  /// Spawns a key-value store.
  /// @param self The actor handle.
  /// @param dir The directory used for persistence. If empty, the instance
  ///            operates in-memory only.
  static behavior make(stateful_actor<state>* self, path dir);
};

} // namespace vast

#endif
