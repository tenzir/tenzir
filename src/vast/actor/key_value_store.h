#ifndef VAST_ACTOR_KEY_VALUE_STORE_H
#define VAST_ACTOR_KEY_VALUE_STORE_H

#include <set>
#include <string>

#include <caf/scoped_actor.hpp>

#include "vast/filesystem.h"
#include "vast/none.h"
#include "vast/actor/actor.h"
#include "vast/util/radix_tree.h"

namespace vast {

/// A replicated hierarchical key-value store.
class key_value_store : public default_actor {
public:
  using storage = util::radix_tree<message>;

  /// Spawns a key-value store.
  /// @param dir The directory used for persistence. If empty, the instance
  ///            operates in-memory only.
  key_value_store(path dir = {});

private:
  void on_exit() override;
  behavior make_behavior() override;

  bool update(std::string const& key, message const& value);

  path dir_;
  storage data_;
  util::radix_tree<none> persistent_;
  actor leader_;
  std::set<actor> followers_;
  behavior following_;
  behavior candidating_;
  behavior leading_;
};

} // namespace vast

#endif
