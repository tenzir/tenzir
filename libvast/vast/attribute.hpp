#ifndef VAST_ATTRIBUTE_HPP
#define VAST_ATTRIBUTE_HPP

#include <string>

#include "vast/detail/operators.hpp"
#include "vast/optional.hpp"

namespace vast {

/// A qualifier.
struct attribute : detail::totally_ordered<attribute> {
  attribute(std::string key = {});
  attribute(std::string key, std::string value);

  std::string key;
  optional<std::string> value;

  friend bool operator==(attribute const& x, attribute const& y);
  friend bool operator<(attribute const& x, attribute const& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, attribute& a) {
    return f(a.key, a.value);
  }
};

} // namespace vast

#endif
