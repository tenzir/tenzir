#include "vast/event_types.hpp"

namespace vast {

bool event_types::initialized = false;

bool event_types::init(schema s) {
  if (initialized)
    return false;
  get_impl() = std::move(s);
  initialized = true;
  return true;
}

const schema* event_types::get() {
  if (!initialized)
    return nullptr;
  return &get_impl();
}

schema& event_types::get_impl() {
  static schema data;
  return data;
}

} // namespace vast
