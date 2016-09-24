#include <tuple>

#include "vast/attribute.hpp"

namespace vast {

attribute::attribute(std::string key) : key{std::move(key)} {
}

attribute::attribute(std::string key, std::string value)
  : key{std::move(key)},
    value{std::move(value)} {
}

bool operator==(attribute const& x, attribute const& y) {
  return x.key == y.key && x.value == y.value;
};

bool operator<(attribute const& x, attribute const& y) {
  return std::tie(x.key, x.value) < std::tie(y.key, y.value);
}

} // namespace vast
