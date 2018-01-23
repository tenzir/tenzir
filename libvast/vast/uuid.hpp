/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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
  using const_reference = const value_type&;
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

  friend bool operator==(const uuid& x, const uuid& y);
  friend bool operator<(const uuid& x, const uuid& y);

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
  size_t operator()(const vast::uuid& u) const {
    size_t x = 0;
    for (auto& byte : u)
      x ^= static_cast<size_t>(byte) + 0x9e3779b9 + (x << 6) + (x >> 2);
    return x;
  }
};

} // namespace std

#endif
