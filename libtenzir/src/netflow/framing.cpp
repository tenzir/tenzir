//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "detail.hpp"

namespace tenzir::netflow::detail {

namespace {

auto parse_framing_field(Cursor& cursor, FramingTemplate& fields,
                         size_t& message_field_count) -> Option<std::string> {
  auto id = uint16_t{0};
  auto length = uint16_t{0};
  if (not cursor.read_u16(id) or not cursor.read_u16(length)) {
    return std::string{"truncated NetFlow v9 template field specifier"};
  }
  if ((id & uint16_t{0x8000}) != 0) {
    auto enterprise_number = uint32_t{0};
    if (not cursor.read_u32(enterprise_number)) {
      return std::string{"truncated NetFlow v9 enterprise field specifier"};
    }
  }
  if (length == 0) {
    return std::string{"NetFlow v9 template field has zero length"};
  }
  if (message_field_count == max_template_fields) {
    return fmt::format("NetFlow message contains more than {} template fields",
                       max_template_fields);
  }
  ++message_field_count;
  fields.push_back(length);
  return None{};
}

auto parse_framing_fields(Cursor& cursor, size_t count, FramingTemplate& fields,
                          size_t& message_field_count) -> Option<std::string> {
  if (count > 1024) {
    return fmt::format("NetFlow v9 template declares too many fields ({})",
                       count);
  }
  if (count > max_template_fields - message_field_count) {
    return fmt::format("NetFlow message contains more than {} template fields",
                       max_template_fields);
  }
  fields.reserve(fields.size() + count);
  for (auto index = size_t{0}; index < count; ++index) {
    if (auto error = parse_framing_field(cursor, fields, message_field_count)) {
      return error;
    }
  }
  return None{};
}

auto parse_framing_fields_by_length(Cursor& cursor, size_t length,
                                    FramingTemplate& fields,
                                    size_t& message_field_count)
  -> Option<std::string> {
  auto bytes = std::span<const std::byte>{};
  if (not cursor.take(length, bytes)) {
    return std::string{"truncated NetFlow v9 options template fields"};
  }
  auto fields_cursor = Cursor{bytes};
  while (fields_cursor.remaining() > 0) {
    if (fields.size() >= 1024) {
      return std::string{"NetFlow v9 template declares too many fields"};
    }
    if (auto error
        = parse_framing_field(fields_cursor, fields, message_field_count)) {
      return error;
    }
  }
  return None{};
}

auto parse_framing_templates(std::span<const std::byte> payload, bool options,
                             FramingTemplates& templates, size_t& record_count,
                             size_t& message_field_count)
  -> Option<std::string> {
  auto cursor = Cursor{payload};
  while (cursor.remaining() > 3) {
    auto template_id = uint16_t{0};
    if (not cursor.read_u16(template_id)) {
      return std::string{"truncated NetFlow v9 template record"};
    }
    if (template_id < 256) {
      return fmt::format("invalid NetFlow v9 template ID {}", template_id);
    }
    auto fields = FramingTemplate{};
    if (options) {
      auto scope_length = uint16_t{0};
      auto option_length = uint16_t{0};
      if (not cursor.read_u16(scope_length)
          or not cursor.read_u16(option_length)) {
        return std::string{"truncated NetFlow v9 options template header"};
      }
      if (auto error = parse_framing_fields_by_length(
            cursor, scope_length, fields, message_field_count)) {
        return error;
      }
      if (auto error = parse_framing_fields_by_length(
            cursor, option_length, fields, message_field_count)) {
        return error;
      }
    } else {
      auto field_count = uint16_t{0};
      if (not cursor.read_u16(field_count)) {
        return std::string{"truncated NetFlow v9 template record header"};
      }
      if (field_count == 0) {
        return std::string{"NetFlow v9 template has no fields"};
      }
      if (auto error = parse_framing_fields(cursor, field_count, fields,
                                            message_field_count)) {
        return error;
      }
    }
    if (fields.empty()) {
      return std::string{"NetFlow v9 template has no fields"};
    }
    templates.insert_or_assign(template_id, std::move(fields));
    ++record_count;
  }
  return None{};
}

auto framing_error(uint16_t version, std::string message) -> FrameResult {
  return FrameResult{
    .status = FrameStatus::error,
    .size = 0,
    .version = version,
    .message = std::move(message),
  };
}

} // namespace

auto framed_data_record_counts(std::span<const std::byte> payload,
                               FramingTemplate const& fields,
                               std::vector<size_t>& candidates)
  -> Option<std::string> {
  auto minimum_record_size = size_t{0};
  for (auto length : fields) {
    minimum_record_size += length == variable_length ? 1 : length;
  }
  if (minimum_record_size == 0) {
    return std::string{"NetFlow v9 template consumes no input"};
  }
  auto cursor = Cursor{payload};
  auto record_count = size_t{0};
  if (cursor.remaining() <= 3) {
    candidates.push_back(0);
  }
  while (cursor.remaining() >= minimum_record_size) {
    auto error = Option<std::string>{None{}};
    for (auto field_length : fields) {
      auto length = size_t{field_length};
      if (field_length == variable_length) {
        auto short_length = uint8_t{0};
        if (not cursor.read_u8(short_length)) {
          error = std::string{"truncated variable-length field prefix"};
          break;
        }
        if (short_length < 255) {
          length = short_length;
        } else {
          auto long_length = uint16_t{0};
          if (not cursor.read_u16(long_length)) {
            error = std::string{"truncated variable-length field prefix"};
            break;
          }
          length = long_length;
        }
      }
      auto ignored = std::span<const std::byte>{};
      if (not cursor.take(length, ignored)) {
        error = std::string{"truncated NetFlow v9 data record"};
        break;
      }
    }
    if (error) {
      if (candidates.empty()) {
        return error;
      }
      break;
    }
    ++record_count;
    if (cursor.remaining() <= 3) {
      candidates.push_back(record_count);
    }
  }
  if (candidates.empty()) {
    return std::string{"NetFlow v9 data set has invalid alignment padding"};
  }
  return None{};
}

auto select_data_record_counts(
  std::span<const std::span<const size_t>> candidate_sets,
  size_t expected_record_count, std::vector<size_t>& record_counts)
  -> RecordCountSelection {
  auto minimum_record_count = size_t{0};
  auto maximum_record_count = size_t{0};
  for (auto candidates : candidate_sets) {
    TENZIR_ASSERT(not candidates.empty());
    for (auto index = size_t{1}; index < candidates.size(); ++index) {
      TENZIR_ASSERT(candidates[index] == candidates[index - 1] + 1);
    }
    minimum_record_count += candidates.front();
    maximum_record_count += candidates.back();
  }
  if (expected_record_count < minimum_record_count
      or expected_record_count > maximum_record_count) {
    return RecordCountSelection::no_match;
  }
  auto remaining = expected_record_count - minimum_record_count;
  auto donor_count = size_t{0};
  auto receiver_count = size_t{0};
  auto donor_index = size_t{0};
  auto receiver_index = size_t{0};
  record_counts.reserve(candidate_sets.size());
  for (auto index = size_t{0}; index < candidate_sets.size(); ++index) {
    auto candidates = candidate_sets[index];
    auto const capacity = candidates.back() - candidates.front();
    auto const increment = std::min(remaining, capacity);
    record_counts.push_back(candidates.front() + increment);
    remaining -= increment;
    if (increment > 0) {
      ++donor_count;
      donor_index = index;
    }
    if (increment < capacity) {
      ++receiver_count;
      receiver_index = index;
    }
  }
  TENZIR_ASSERT(remaining == 0);
  if (donor_count > 0 and receiver_count > 0
      and (donor_count > 1 or receiver_count > 1
           or donor_index != receiver_index)) {
    return RecordCountSelection::ambiguous;
  }
  return RecordCountSelection::unique;
}

auto frame_message(std::span<const std::byte> bytes, bool end_of_input,
                   FramingLookup const& lookup, V9FramingState& v9_state)
  -> FrameResult {
  if (bytes.size() < 2) {
    v9_state = {};
    if (end_of_input and not bytes.empty()) {
      return framing_error(0, "truncated NetFlow version field");
    }
    return {};
  }
  auto const version = read_u16_at(bytes, 0);
  if (not is_supported_version(version)) {
    v9_state = {};
    return framing_error(version, fmt::format("unsupported NetFlow version {}",
                                              version));
  }
  if (version == static_cast<uint16_t>(Version::v5)) {
    v9_state = {};
    if (bytes.size() < 4) {
      if (end_of_input) {
        return framing_error(version, "truncated NetFlow v5 header");
      }
      return FrameResult{.version = version};
    }
    auto const count = read_u16_at(bytes, 2);
    if (count > 30) {
      return framing_error(
        version, fmt::format("invalid NetFlow v5 record count {}", count));
    }
    auto const size = size_t{24} + size_t{count} * 48;
    if (bytes.size() < size) {
      if (end_of_input) {
        return framing_error(version,
                             fmt::format("truncated NetFlow v5 message: "
                                         "expected {} bytes, got {}",
                                         size, bytes.size()));
      }
      return FrameResult{.version = version};
    }
    return FrameResult{FrameStatus::ready, size, version, {}};
  }
  if (version == static_cast<uint16_t>(Version::ipfix)) {
    v9_state = {};
    if (bytes.size() < 4) {
      if (end_of_input) {
        return framing_error(version, "truncated IPFIX header");
      }
      return FrameResult{.version = version};
    }
    auto const size = size_t{read_u16_at(bytes, 2)};
    if (size < 16) {
      return framing_error(
        version, fmt::format("invalid IPFIX message length {}", size));
    }
    if (bytes.size() < size) {
      if (end_of_input) {
        return framing_error(version, fmt::format("truncated IPFIX message: "
                                                  "expected {} bytes, got {}",
                                                  size, bytes.size()));
      }
      return FrameResult{.version = version};
    }
    return FrameResult{FrameStatus::ready, size, version, {}};
  }
  if (bytes.size() < 20) {
    v9_state = {};
    if (end_of_input) {
      return framing_error(version, "truncated NetFlow v9 header");
    }
    return FrameResult{.version = version};
  }
  auto header_cursor = Cursor{bytes.subspan(2, 18)};
  auto expected_records = uint16_t{0};
  auto sys_uptime = uint32_t{0};
  auto export_time_seconds = uint32_t{0};
  auto sequence_number = uint32_t{0};
  auto domain_id = uint32_t{0};
  // The 18-byte subspan above guarantees these reads succeed.
  std::ignore = header_cursor.read_u16(expected_records);
  std::ignore = header_cursor.read_u32(sys_uptime);
  std::ignore = header_cursor.read_u32(export_time_seconds);
  std::ignore = header_cursor.read_u32(sequence_number);
  std::ignore = header_cursor.read_u32(domain_id);
  if (expected_records == 0) {
    v9_state = {};
    return FrameResult{FrameStatus::ready, 20, version, {}};
  }
  if (not v9_state.matches(expected_records, sys_uptime, export_time_seconds,
                           sequence_number, domain_id)
      or bytes.size() < v9_state.offset) {
    v9_state = V9FramingState{
      .active = true,
      .expected_records = expected_records,
      .sys_uptime = sys_uptime,
      .export_time_seconds = export_time_seconds,
      .sequence_number = sequence_number,
      .domain_id = domain_id,
    };
  }
  auto& offset = v9_state.offset;
  auto& minimum_record_count = v9_state.minimum_record_count;
  auto& maximum_record_count = v9_state.maximum_record_count;
  auto& record_count_known = v9_state.record_count_known;
  auto& set_count = v9_state.set_count;
  auto& template_field_count = v9_state.template_field_count;
  auto& local_templates = v9_state.local_templates;
  auto add_record_counts = [&](std::span<const size_t> additions) {
    TENZIR_ASSERT(not additions.empty());
    for (auto index = size_t{1}; index < additions.size(); ++index) {
      TENZIR_ASSERT(additions[index] == additions[index - 1] + 1);
    }
    minimum_record_count += additions.front();
    maximum_record_count += additions.back();
  };
  auto expected_record_count_is_possible = [&] {
    return minimum_record_count <= expected_records
           and expected_records <= maximum_record_count;
  };
  auto idle_boundary_is_possible = [&] {
    return set_count > 0
           and (not record_count_known or expected_record_count_is_possible());
  };
  auto ready = [&](size_t size) {
    v9_state = {};
    return FrameResult{FrameStatus::ready, size, version, {}};
  };
  auto fail = [&](std::string message) {
    v9_state = {};
    return framing_error(version, std::move(message));
  };
  while (true) {
    if (offset == bytes.size()) {
      if (end_of_input) {
        if (record_count_known and not expected_record_count_is_possible()) {
          return fail(fmt::format("NetFlow v9 header record count {} does "
                                  "not match the message",
                                  expected_records));
        }
        return ready(offset);
      }
      if (record_count_known and minimum_record_count == expected_records) {
        return ready(offset);
      }
      if (idle_boundary_is_possible()) {
        return FrameResult{
          .status = FrameStatus::ambiguous,
          .size = offset,
          .version = version,
        };
      }
      return FrameResult{.version = version};
    }
    if (bytes.size() - offset < 4) {
      if (idle_boundary_is_possible()) {
        if (end_of_input) {
          return ready(offset);
        }
        return FrameResult{
          .status = FrameStatus::ambiguous,
          .size = offset,
          .version = version,
        };
      }
      if (end_of_input) {
        return fail("truncated NetFlow v9 set header");
      }
      return FrameResult{.version = version};
    }
    auto const set_id = read_u16_at(bytes, offset);
    if (set_id < 256 and set_id != 0 and set_id != 1) {
      if (is_supported_version(set_id)) {
        return ready(offset);
      }
      return fail(fmt::format("invalid reserved NetFlow v9 set ID {}", set_id));
    }
    if (set_count == max_sets_per_message) {
      return fail(fmt::format("NetFlow v9 message contains more than {} sets",
                              max_sets_per_message));
    }
    auto const set_length = size_t{read_u16_at(bytes, offset + 2)};
    if (set_length < 4) {
      return fail(fmt::format("invalid NetFlow v9 set length {}", set_length));
    }
    if (bytes.size() - offset < set_length) {
      if (end_of_input) {
        return fail(fmt::format("truncated NetFlow v9 set: expected {} bytes, "
                                "got {}",
                                set_length, bytes.size() - offset));
      }
      return FrameResult{.version = version};
    }
    ++set_count;
    auto const payload = bytes.subspan(offset + 4, set_length - 4);
    if (set_id == 0 or set_id == 1) {
      auto template_records = size_t{0};
      if (auto error
          = parse_framing_templates(payload, set_id == 1, local_templates,
                                    template_records, template_field_count)) {
        return fail(std::move(*error));
      }
      add_record_counts(std::span{&template_records, size_t{1}});
    } else {
      auto fields = FramingTemplate{};
      if (auto position = local_templates.find(set_id);
          position != local_templates.end()) {
        fields = position->second;
      } else if (not lookup(domain_id, sys_uptime, export_time_seconds,
                            sequence_number, set_id, fields)) {
        record_count_known = false;
      }
      if (not fields.empty()) {
        auto candidates = std::vector<size_t>{};
        if (auto error
            = framed_data_record_counts(payload, fields, candidates)) {
          return fail(std::move(*error));
        }
        add_record_counts(candidates);
      }
    }
    offset += set_length;
    if (record_count_known and minimum_record_count > expected_records) {
      return fail(fmt::format("NetFlow v9 header record count {} is too small",
                              expected_records));
    }
  }
}

} // namespace tenzir::netflow::detail
