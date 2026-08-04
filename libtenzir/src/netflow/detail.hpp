//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/checked_math.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/netflow.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <span>
#include <string>
#include <vector>

#include "iana.hpp"

namespace tenzir::netflow::detail {

constexpr auto max_exporters = size_t{1024};
constexpr auto max_templates_per_exporter = size_t{4096};
constexpr auto max_templates = size_t{16 * 1024};
constexpr auto max_template_fields = size_t{256 * 1024};
constexpr auto max_sets_per_message = size_t{1024};
constexpr auto max_buffered_sets = size_t{1024};
constexpr auto max_buffered_bytes = size_t{16 * 1024 * 1024};
// Bound the transient row-wise representation before the executor can move
// decoded records into columnar builders. Track records and fields separately
// so both narrow and wide templates remain bounded.
constexpr auto max_decoded_records = size_t{64 * 1024};
constexpr auto max_decoded_fields = size_t{64 * 1024};
constexpr auto max_buffer_generations = uint64_t{1024};
constexpr auto buffered_set_ttl = std::chrono::minutes{1};
constexpr auto variable_length = uint16_t{std::numeric_limits<uint16_t>::max()};
constexpr auto ntp_to_unix_seconds = int64_t{2'208'988'800};

class Cursor {
public:
  explicit Cursor(std::span<const std::byte> bytes) : bytes_{bytes} {
  }

  auto remaining() const -> size_t {
    return bytes_.size() - offset_;
  }

  auto offset() const -> size_t {
    return offset_;
  }

  auto read_u8(uint8_t& result) -> bool {
    if (remaining() < 1) {
      return false;
    }
    result = std::to_integer<uint8_t>(bytes_[offset_]);
    ++offset_;
    return true;
  }

  auto read_u16(uint16_t& result) -> bool {
    if (remaining() < 2) {
      return false;
    }
    result = uint16_t{std::to_integer<uint8_t>(bytes_[offset_])} << 8;
    result |= std::to_integer<uint8_t>(bytes_[offset_ + 1]);
    offset_ += 2;
    return true;
  }

  auto read_u32(uint32_t& result) -> bool {
    if (remaining() < 4) {
      return false;
    }
    result = uint32_t{std::to_integer<uint8_t>(bytes_[offset_])} << 24;
    result |= uint32_t{std::to_integer<uint8_t>(bytes_[offset_ + 1])} << 16;
    result |= uint32_t{std::to_integer<uint8_t>(bytes_[offset_ + 2])} << 8;
    result |= std::to_integer<uint8_t>(bytes_[offset_ + 3]);
    offset_ += 4;
    return true;
  }

  auto read_u64(uint64_t& result, size_t length) -> bool {
    if (length > 8 or remaining() < length) {
      return false;
    }
    result = 0;
    for (auto index = size_t{0}; index < length; ++index) {
      result <<= 8;
      result |= std::to_integer<uint8_t>(bytes_[offset_ + index]);
    }
    offset_ += length;
    return true;
  }

  auto take(size_t size, std::span<const std::byte>& result) -> bool {
    if (remaining() < size) {
      return false;
    }
    result = bytes_.subspan(offset_, size);
    offset_ += size;
    return true;
  }

  auto rest() const -> std::span<const std::byte> {
    return bytes_.subspan(offset_);
  }

private:
  std::span<const std::byte> bytes_;
  size_t offset_ = 0;
};

inline auto read_u16_at(std::span<const std::byte> bytes, size_t offset)
  -> uint16_t {
  TENZIR_ASSERT(offset + 2 <= bytes.size());
  return uint16_t{std::to_integer<uint8_t>(bytes[offset])} << 8
         | std::to_integer<uint8_t>(bytes[offset + 1]);
}

inline auto modular_delta(uint32_t current, uint32_t reference) -> int64_t {
  auto delta = int64_t{current} - reference;
  constexpr auto rollover = int64_t{1} << 32;
  if (delta > std::numeric_limits<int32_t>::max()) {
    delta -= rollover;
  } else if (delta < std::numeric_limits<int32_t>::min()) {
    delta += rollover;
  }
  return delta;
}

inline auto unfold_seconds(uint32_t seconds, int64_t reference_seconds)
  -> int64_t {
  return reference_seconds
         + modular_delta(seconds, static_cast<uint32_t>(reference_seconds));
}

inline auto make_time(int64_t seconds, int64_t nanoseconds = 0)
  -> Option<time> {
  auto value = checked_mul(seconds, int64_t{1'000'000'000});
  if (not value) {
    return None{};
  }
  value = checked_add(*value, nanoseconds);
  if (not value) {
    return None{};
  }
  return time{duration{*value}};
}

inline auto all_zero(std::span<const std::byte> bytes) -> bool {
  return std::ranges::all_of(bytes, [](auto byte) {
    return byte == std::byte{0};
  });
}

inline auto is_supported_version(uint16_t version) -> bool {
  return version == static_cast<uint16_t>(Version::v5)
         or version == static_cast<uint16_t>(Version::v9)
         or version == static_cast<uint16_t>(Version::ipfix);
}

struct EnterpriseElement {
  uint16_t id;
  std::string_view name;
  InformationElementType type;
};

/// Looks up a standard IANA information element by ID.
auto standard_element(uint16_t id) -> InformationElement const*;

/// Looks up an Ixia enterprise information element by ID.
auto ixia_element(uint16_t id) -> EnterpriseElement const*;

/// Decodes a single information element value into native data, preserving
/// undecodable values as blobs.
auto decode_value(InformationElementType type, std::span<const std::byte> bytes,
                  int64_t reference_unix_seconds) -> data;

using FramingTemplate = std::vector<uint16_t>;
using FramingTemplates = std::map<uint16_t, FramingTemplate>;

enum class RecordCountSelection : uint8_t {
  no_match,
  unique,
  ambiguous,
};

struct V9FramingState {
  bool active = false;
  uint16_t expected_records = 0;
  uint32_t sys_uptime = 0;
  uint32_t export_time_seconds = 0;
  uint32_t sequence_number = 0;
  uint32_t domain_id = 0;
  size_t offset = 20;
  size_t minimum_record_count = 0;
  size_t maximum_record_count = 0;
  bool record_count_known = true;
  size_t set_count = 0;
  size_t template_field_count = 0;
  FramingTemplates local_templates;

  auto matches(uint16_t expected_records_value, uint32_t sys_uptime_value,
               uint32_t export_time_seconds_value,
               uint32_t sequence_number_value, uint32_t domain_id_value) const
    -> bool {
    return active and expected_records == expected_records_value
           and sys_uptime == sys_uptime_value
           and export_time_seconds == export_time_seconds_value
           and sequence_number == sequence_number_value
           and domain_id == domain_id_value;
  }
};

/// Enumerates the possible per-set record counts for a framed v9 data set.
auto framed_data_record_counts(std::span<const std::byte> payload,
                               FramingTemplate const& fields,
                               std::vector<size_t>& candidates)
  -> Option<std::string>;

/// Distributes the expected record count over the candidate sets.
auto select_data_record_counts(
  std::span<const std::span<const size_t>> candidate_sets,
  size_t expected_record_count, std::vector<size_t>& record_counts)
  -> RecordCountSelection;

/// Resolves a known v9 template's field lengths from decoder state during
/// framing. Returns false when the exporter, template, or message order is
/// unknown, forcing the framer to parse templates from the message itself.
using FramingLookup
  = std::function<bool(uint32_t domain_id, uint32_t sys_uptime,
                       uint32_t export_time_seconds, uint32_t sequence_number,
                       uint16_t template_id, FramingTemplate& fields)>;

/// Determines the size of the first message in an unframed byte stream.
auto frame_message(std::span<const std::byte> bytes, bool end_of_input,
                   FramingLookup const& lookup, V9FramingState& v9_state)
  -> FrameResult;

} // namespace tenzir::netflow::detail
