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
#include "vast/hash/hash.hpp"
#include "vast/hash/uniquely_represented.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/meta/hex_formatted.hpp>
#include <fmt/format.h>

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

  /// @returns The binary data as a pair of 64 bit integers.
  [[nodiscard]] std::pair<uint64_t, uint64_t> as_u64() const;

  template <class Inspector>
  friend auto inspect(Inspector& f, uuid& x) {
    return f(caf::meta::hex_formatted(), x.id_);
  }

private:
  std::array<value_type, num_bytes> id_;
};

template <>
struct is_uniquely_represented<uuid>
  : std::bool_constant<sizeof(uuid) == uuid::num_bytes> {};

// flatbuffer support
[[nodiscard]] caf::expected<flatbuffers::Offset<fbs::uuid::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const uuid& x);

[[nodiscard]] caf::error unpack(const fbs::uuid::v0& x, uuid& y);

} // namespace vast

namespace std {

template <>
struct hash<vast::uuid> {
  size_t operator()(const vast::uuid& x) const {
    return vast::hash(x);
  }
};

} // namespace std

namespace fmt {

template <>
struct formatter<vast::uuid> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::uuid& x, FormatContext& ctx) {
    // e.g. 96107185-1838-48fb-906c-d1a9941ff407
    static_assert(sizeof(vast::uuid) == 16, "id format changed, please update "
                                            "formatter");
    const auto args
      = std::span{reinterpret_cast<const unsigned char*>(x.begin()), x.size()};
    return format_to(ctx.out(), "{:02X}-{:02X}-{:02X}-{:02X}-{:02X}",
                     join(args.subspan(0, 4), ""), join(args.subspan(4, 2), ""),
                     join(args.subspan(6, 2), ""), join(args.subspan(8, 2), ""),
                     join(args.subspan(10, 6), ""));
  }
};

} // namespace fmt
