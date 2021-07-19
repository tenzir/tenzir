//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/operators.hpp"
#include "vast/fbs/uuid.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/meta/hex_formatted.hpp>

#include <array>
#include <cstddef>
#include <span>

namespace vast {

/// A universally unique identifier (UUID).
class uuid : detail::totally_ordered<uuid> {
public:
  /// The number of bytes in a UUID;
  static constexpr size_t num_bytes = 16;

  using value_type = std::byte;
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
  explicit uuid(std::span<const std::byte, num_bytes> bytes);

  /// Accesses a specific byte.
  reference operator[](size_t i);
  const_reference operator[](size_t i) const;

  // Container interface.
  iterator begin();
  iterator end();
  [[nodiscard]] const_iterator begin() const;
  [[nodiscard]] const_iterator end() const;
  [[nodiscard]] size_type size() const;

  friend bool operator==(const uuid& x, const uuid& y);
  friend bool operator<(const uuid& x, const uuid& y);

  /// @returns the binary data.
  friend std::span<const std::byte, num_bytes> as_bytes(const uuid& x) {
    return std::span<const std::byte, num_bytes>{x.id_};
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, uuid& x) {
    return f(caf::meta::hex_formatted(), x.id_);
  }

private:
  std::array<value_type, num_bytes> id_;
};

// flatbuffer support

caf::expected<flatbuffers::Offset<fbs::uuid::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const uuid& x);

caf::error unpack(const fbs::uuid::v0& x, uuid& y);

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
