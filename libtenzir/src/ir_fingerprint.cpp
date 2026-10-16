//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/hash/sha.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/plugin.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>
#include <caf/error.hpp>
#include <caf/serializer.hpp>
#include <fmt/format.h>

#include <cstddef>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace tenzir {

namespace {

/// Serializes like `caf::binary_serializer`, except for what is inside a
/// `location`, which it leaves out.
///
/// A location names its source by an id from a process-wide counter, so two
/// compiles of one definition never agree on it, and what it points at says
/// where an entity was written, not what it does.
class canonical_serializer final : public caf::serializer {
public:
  explicit canonical_serializer(caf::byte_buffer& buffer) : inner_{buffer} {
  }

  auto has_human_readable_format() const noexcept -> bool override {
    return false;
  }

  auto set_error(caf::error stop_reason) -> void override {
    error_ = std::move(stop_reason);
  }

  auto get_error() noexcept -> caf::error& override {
    return error_;
  }

  auto begin_object(caf::type_id_t type, std::string_view name)
    -> bool override {
    if (skipped_ > 0 or name == "location") {
      ++skipped_;
      return true;
    }
    return inner_.begin_object(type, name);
  }

  auto end_object() -> bool override {
    if (skipped_ > 0) {
      --skipped_;
      return true;
    }
    return inner_.end_object();
  }

  auto begin_field(std::string_view name) -> bool override {
    return skipped_ > 0 or inner_.begin_field(name);
  }

  auto begin_field(std::string_view name, bool is_present) -> bool override {
    return skipped_ > 0 or inner_.begin_field(name, is_present);
  }

  auto begin_field(std::string_view name, std::span<caf::type_id_t const> types,
                   size_t index) -> bool override {
    return skipped_ > 0 or inner_.begin_field(name, types, index);
  }

  auto begin_field(std::string_view name, bool is_present,
                   std::span<caf::type_id_t const> types, size_t index)
    -> bool override {
    return skipped_ > 0 or inner_.begin_field(name, is_present, types, index);
  }

  auto end_field() -> bool override {
    return skipped_ > 0 or inner_.end_field();
  }

  auto begin_tuple(size_t size) -> bool override {
    return skipped_ > 0 or inner_.begin_tuple(size);
  }

  auto end_tuple() -> bool override {
    return skipped_ > 0 or inner_.end_tuple();
  }

  auto begin_key_value_pair() -> bool override {
    return skipped_ > 0 or inner_.begin_key_value_pair();
  }

  auto end_key_value_pair() -> bool override {
    return skipped_ > 0 or inner_.end_key_value_pair();
  }

  auto begin_sequence(size_t size) -> bool override {
    return skipped_ > 0 or inner_.begin_sequence(size);
  }

  auto end_sequence() -> bool override {
    return skipped_ > 0 or inner_.end_sequence();
  }

  auto begin_associative_array(size_t size) -> bool override {
    return skipped_ > 0 or inner_.begin_associative_array(size);
  }

  auto end_associative_array() -> bool override {
    return skipped_ > 0 or inner_.end_associative_array();
  }

  auto value(std::byte x) -> bool override {
    return forward(x);
  }

  auto value(bool x) -> bool override {
    return forward(x);
  }

  auto value(int8_t x) -> bool override {
    return forward(x);
  }

  auto value(uint8_t x) -> bool override {
    return forward(x);
  }

  auto value(int16_t x) -> bool override {
    return forward(x);
  }

  auto value(uint16_t x) -> bool override {
    return forward(x);
  }

  auto value(int32_t x) -> bool override {
    return forward(x);
  }

  auto value(uint32_t x) -> bool override {
    return forward(x);
  }

  auto value(int64_t x) -> bool override {
    return forward(x);
  }

  auto value(uint64_t x) -> bool override {
    return forward(x);
  }

  auto value(float x) -> bool override {
    return forward(x);
  }

  auto value(double x) -> bool override {
    return forward(x);
  }

  auto value(long double x) -> bool override {
    return forward(x);
  }

  auto value(std::string_view x) -> bool override {
    return forward(x);
  }

  auto value(std::u16string const& x) -> bool override {
    return forward(x);
  }

  auto value(std::u32string const& x) -> bool override {
    return forward(x);
  }

  auto value(caf::const_byte_span x) -> bool override {
    return forward(x);
  }

  auto value(std::vector<bool> const& x) -> bool override {
    return forward(x);
  }

  auto actor_handle_codec() -> caf::actor_handle_codec* override {
    // A compiled pipeline holds no actor handles.
    return nullptr;
  }

private:
  template <class T>
  auto forward(T const& x) -> bool {
    return skipped_ > 0 or inner_.value(x);
  }

  caf::binary_serializer inner_;
  caf::error error_;
  size_t skipped_ = 0;
};

} // namespace

auto ir::fingerprint(pipeline const& pipe) -> std::string {
  auto buffer = caf::byte_buffer{};
  auto serializer = canonical_serializer{buffer};
  auto& base = static_cast<caf::serializer&>(serializer);
  // Saving does not change what it inspects.
  auto ok = inspect(base, const_cast<pipeline&>(pipe));
  TENZIR_ASSERT(ok, "a compiled pipeline must serialize");
  auto hash = sha256{};
  hash.add(std::as_bytes(std::span{buffer}));
  auto result = std::string{};
  for (auto byte : hash.finish()) {
    fmt::format_to(std::back_inserter(result), "{:02x}",
                   std::to_integer<unsigned>(byte));
  }
  return result;
}

} // namespace tenzir
