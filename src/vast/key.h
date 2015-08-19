#ifndef VAST_KEY_H
#define VAST_KEY_H

#include <string>

#include "vast/concept/printable/string.h"
#include "vast/concept/printable/to_string.h"
#include "vast/util/stack/vector.h"

namespace vast {

/// A sequence of names identifying a resource.
struct key : util::stack::vector<4, std::string> {
  using super = util::stack::vector<4, std::string>;
  using super::vector;

  /// Creates a key string reprentation of an arbitrary sequence.
  template <typename... Ts>
  static std::string str(Ts&&... xs);
};

} // namespace vast

#include "vast/concept/printable/vast/key.h"

template <typename... Ts>
std::string vast::key::str(Ts&&... xs) {
  using vast::to_string;
  using std::to_string;
  return to_string(key{to_string(xs)...});
}

#endif
