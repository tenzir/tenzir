#include "vast/event_types.hpp"

namespace vast::event_types {

namespace {

bool initialized = false;

module& get_impl() {
  static module data;
  return data;
}

} // namespace

bool init(module s) {
  if (initialized)
    return false;
  get_impl() = std::move(s);
  initialized = true;
  return true;
}

const module* get() {
  if (!initialized)
    return nullptr;
  return &get_impl();
}

} // namespace vast::event_types
