//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/operators.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/uniquely_represented.hpp"

#include <caf/detail/stringification_inspector.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <array>
#include <cstddef>
#include <span>

namespace tenzir {

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
  static uuid null();
  static uuid from_flatbuffer(const fbs::UUID&);

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

  friend std::strong_ordering
  operator<=>(const uuid& lhs, const uuid& rhs) noexcept;

  friend bool operator==(const uuid& lhs, const uuid& rhs) noexcept;

  /// @returns the binary data.
  friend std::span<const std::byte, num_bytes> as_bytes(const uuid& x) {
    return std::span<const std::byte, num_bytes>{x.id_};
  }

  /// @returns The binary data as a pair of 64 bit integers.
  [[nodiscard]] std::pair<uint64_t, uint64_t> as_u64() const;

  template <class Inspector>
  friend auto inspect(Inspector& f, uuid& x) {
    return f.apply(x.id_);
  }

  friend auto inspect(caf::detail::stringification_inspector& f, uuid& x) {
    return f.apply(fmt::to_string(x));
  }

private:
  std::array<value_type, num_bytes> id_;
};

template <>
struct is_uniquely_represented<uuid>
  : std::bool_constant<sizeof(uuid) == uuid::num_bytes> {};

// flatbuffer support
[[nodiscard]] caf::expected<flatbuffers::Offset<fbs::LegacyUUID>>
pack(flatbuffers::FlatBufferBuilder& builder, const uuid& x);

[[nodiscard]] caf::error unpack(const fbs::LegacyUUID& x, uuid& y);

} // namespace tenzir

namespace std {

template <>
struct hash<tenzir::uuid> {
  size_t operator()(const tenzir::uuid& x) const {
    return tenzir::hash(x);
  }
};

} // namespace std

namespace fmt {

template <>
struct formatter<tenzir::uuid> {
  bool uppercase = false;

  // Callers may use '{:u}' to force upper-case rendering.
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    auto it = ctx.begin();
    auto end = ctx.end();
    if (it != end && (*it++ == 'u')) {
      uppercase = true;
    }
    // continue until end of range
    while (it != end && *it != '}')
      ++it;
    return it;
  }

  template <typename FormatContext>
  auto format(const tenzir::uuid& x, FormatContext& ctx) const {
    // e.g. 96107185-1838-48fb-906c-d1a9941ff407
    static_assert(sizeof(tenzir::uuid) == 16,
                  "id format changed, please update "
                  "formatter");
    const auto args
      = std::span{reinterpret_cast<const unsigned char*>(x.begin()), x.size()};
    if (uppercase)
      return fmt::format_to(ctx.out(), "{:02X}-{:02X}-{:02X}-{:02X}-{:02X}",
                            join(args.subspan(0, 4), ""),
                            join(args.subspan(4, 2), ""),
                            join(args.subspan(6, 2), ""),
                            join(args.subspan(8, 2), ""),
                            join(args.subspan(10, 6), ""));
    return fmt::format_to(ctx.out(), "{:02x}-{:02x}-{:02x}-{:02x}-{:02x}",
                          join(args.subspan(0, 4), ""),
                          join(args.subspan(4, 2), ""),
                          join(args.subspan(6, 2), ""),
                          join(args.subspan(8, 2), ""),
                          join(args.subspan(10, 6), ""));
  }
};

} // namespace fmt
