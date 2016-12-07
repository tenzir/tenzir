#ifndef VAST_SYSTEM_KEY_VALUE_STORE_HPP
#define VAST_SYSTEM_KEY_VALUE_STORE_HPP

#include <set>

#include "vast/filesystem.hpp"
#include "vast/none.hpp"
#include "vast/detail/radix_tree.hpp"

namespace vast {
namespace system {

using storage = detail::radix_tree<message>;

/// A replicated hierarchical key-value store.
struct key_value_store_state {
  storage data;
  util::radix_tree<none> persistent;
  caf::actor leader;
  std::set<caf::actor> followers;
  const char* name = "key-value-store";
};

/// Spawns a key-value store.
/// @param self The actor handle.
/// @param dir The directory used for persistence. If empty, the instance
///            operates in-memory only.
caf::behavior key_value_store(caf::stateful_actor<key_value_store_state>* self,
                              path dir);

} // namespace system
} // namespace vast

#endif
