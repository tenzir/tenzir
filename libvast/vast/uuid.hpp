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

#pragma once

#include "vast/byte.hpp"
#include "vast/detail/operators.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/span.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/meta/hex_formatted.hpp>

#include <array>

namespace vast {

/// A universally unique identifier (UUID).
class uuid : detail::totally_ordered<uuid> {
public:
  /// The number of bytes in a UUID;
  static constexpr size_t num_bytes = 16;

  using value_type = byte;
  using reference = value_type&;
  using const_reference = const value_type&;
  using iterator = value_type*;
  using const_iterator = const value_type*;
  using size_type = size_t;

  static uuid random();
  static uuid nil();

  /// Constructs an uninitialized UUID.
  uuid() = default;

  /// Constructs a UUID from 16 bytes.
  /// @param bytes The data to interpret as UUID.
  explicit uuid(span<const byte, num_bytes> bytes);

  /// Accesses a specific byte.
  reference operator[](size_t i);
  const_reference operator[](size_t i) const;

  // Container interface.
  iterator begin();
  iterator end();
  const_iterator begin() const;
  const_iterator end() const;
  size_type size() const;

  friend bool operator==(const uuid& x, const uuid& y);
  friend bool operator<(const uuid& x, const uuid& y);

  /// @returns the binary data.
  friend span<const byte, num_bytes> as_bytes(const uuid& x) {
    return span<const byte, num_bytes>{x.id_};
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, uuid& x) {
    return f(caf::meta::hex_formatted(), x.id_);
  }

private:
  std::array<value_type, num_bytes> id_;
};

// flatbuffer support

caf::expected<flatbuffers::Offset<fbs::UUID>>
pack(flatbuffers::FlatBufferBuilder& builder, const uuid& x);

caf::error unpack(const fbs::UUID& x, uuid& y);

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
