//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/narrow.hpp"
#include "tenzir/hash/sha.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/location.hpp"
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
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir {

namespace {

/// Serializes like `caf::binary_serializer`, except that a `location` names
/// its source by the order in which the sources first appear rather than by
/// its id.
///
/// The id comes from a process-wide counter, so two compiles of one definition
/// never agree on it. Where in its source a location points does enter: a
/// diagnostic of a run points there, and has to be right for the text the run
/// is shown with.
class canonical_serializer final : public caf::serializer {
public:
  explicit canonical_serializer(caf::byte_buffer& buffer) : inner_{buffer} {
  }

  /// Writes `x` the way a location inside the pipeline is written.
  auto write(location const& x) -> bool {
    return inner_.value(x.begin) and inner_.value(x.end)
           and inner_.value(ordinal(x.source_index))
           and inner_.value(x.callsite_index);
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
      if (skipped_ == 0) {
        fields_.clear();
      }
      ++skipped_;
      return true;
    }
    return inner_.begin_object(type, name);
  }

  auto end_object() -> bool override {
    if (skipped_ > 0) {
      if (--skipped_ > 0) {
        return true;
      }
      // Its fields, in the order `inspect` gives them.
      if (fields_.size() != 4) {
        set_error(caf::make_error(caf::sec::invalid_argument,
                                  "a location has four fields"));
        return false;
      }
      return write(location{fields_[0], fields_[1], fields_[2], fields_[3]});
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
    if (skipped_ == 1) {
      fields_.push_back(x);
      return true;
    }
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

  /// The position of `source` among the sources seen so far, from 1, or 0 for
  /// no source.
  auto ordinal(SourceId source) -> uint32_t {
    if (source == 0) {
      return 0;
    }
    auto [it, inserted] = ordinals_.try_emplace(
      source, detail::narrow_cast<uint32_t>(ordinals_.size() + 1));
    return it->second;
  }

  caf::binary_serializer inner_;
  caf::error error_;
  /// How deep inside a location the serializer is.
  size_t skipped_ = 0;
  /// The fields of the location being written.
  std::vector<uint32_t> fields_;
  std::unordered_map<SourceId, uint32_t> ordinals_;
};

} // namespace

auto ir::fingerprint(pipeline const& pipe, SourceMap const& sources)
  -> std::string {
  auto buffer = caf::byte_buffer{};
  auto serializer = canonical_serializer{buffer};
  auto& base = static_cast<caf::serializer&>(serializer);
  // Saving does not change what it inspects.
  auto ok = inspect(base, const_cast<pipeline&>(pipe));
  TENZIR_ASSERT(ok, "a compiled pipeline must serialize");
  // The calls of user-defined operators, which a diagnostic from inside one
  // points at too, and which the pipeline itself need not hold.
  for (auto const& call_site : sources.call_sites()) {
    ok = serializer.write(call_site);
    TENZIR_ASSERT(ok);
  }
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
