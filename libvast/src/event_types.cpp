#include "vast/event_types.hpp"

namespace vast::event_types {

namespace {

bool initialized = false;

schema& get_impl() {
  static schema data;
  return data;
}

} // namespace

bool init(schema s) {
  if (initialized)
    return false;
  get_impl() = std::move(s);
  initialized = true;
  return true;
}

const schema* get() {
  if (!initialized)
    return nullptr;
  return &get_impl();
}

} // namespace vast::event_types
