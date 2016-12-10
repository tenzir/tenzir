#include <caf/all.hpp>

#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/key_value_store.hpp"

using namespace caf;

namespace vast {
namespace system {

data_store_type::behavior_type
data_store(data_store_type::stateful_pointer<data_store_state> self) {
  return {
    [=](put_atom, const data& key, data& value) {
      self->state.store[key] = std::move(value);
      return ok_atom::value;
    },
    [=](delete_atom, const data& key) {
      self->state.store.erase(key);
      return ok_atom::value;
    },
    [=](get_atom, const data& key) -> result<data> {
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return make_error(ec::unspecified, "no such key");
      return i->second;
    }
  };
}

} // namespace system
} // namespace vast
