#ifndef VAST_UUID_HPP
#define VAST_UUID_HPP

#include <array>

#include "vast/detail/operators.hpp"

namespace vast {

struct access;

/// A universally unique identifier (UUID).
class uuid : detail::totally_ordered<uuid> {
  friend access;

public:
  using value_type = uint8_t;
  using reference = value_type&;
  using const_reference = value_type const&;
  using iterator = value_type*;
  using const_iterator = value_type const*;
  using size_type = size_t;

  static uuid random();
  static uuid nil();

  uuid() = default;

  reference operator[](size_t i);
  const_reference operator[](size_t i) const;

  iterator begin();
  iterator end();
  const_iterator begin() const;
  const_iterator end() const;
  size_type size() const;

  void swap(uuid& other);

  friend bool operator==(uuid const& x, uuid const& y);
  friend bool operator<(uuid const& x, uuid const& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, uuid& u) {
    return f(u.id_);
  }

private:
  std::array<value_type, 16> id_;
};

} // namespace vast

// TODO: express in terms of hashable concept. This means simply hashing the
// bytes of the entire std::array.
namespace std {

template <>
struct hash<vast::uuid> {
  size_t operator()(vast::uuid const& u) const {
    size_t x = 0;
    for (auto& byte : u)
      x ^= static_cast<size_t>(byte) + 0x9e3779b9 + (x << 6) + (x >> 2);
    return x;
  }
};

} // namespace std

#endif
