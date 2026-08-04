//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/netflow.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <compare>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <ranges>
#include <string_view>
#include <tuple>
#include <utility>

#include "detail.hpp"

namespace tenzir::netflow {

// Pull in the subsystem internals shared between the netflow translation
// units; this TU defines the public `Decoder` on top of them.
using namespace detail;

namespace {

using Clock = std::chrono::steady_clock;

struct MessageHeader {
  Version version = Version::v5;
  uint16_t count = 0;
  uint16_t length = 0;
  duration sys_uptime = {};
  uint32_t export_time_seconds = 0;
  int64_t unfolded_export_time_seconds = 0;
  time export_time = {};
  uint32_t sequence_number = 0;
  uint32_t domain_id = 0;
  uint8_t engine_type = 0;
  uint8_t engine_id = 0;
  uint8_t sampling_mode = 0;
  uint16_t sampling_interval = 0;

  friend auto inspect(auto& f, MessageHeader& x) -> bool {
    return f.object(x).fields(
      f.field("version", x.version), f.field("count", x.count),
      f.field("length", x.length), f.field("sys_uptime", x.sys_uptime),
      f.field("export_time_seconds", x.export_time_seconds),
      f.field("unfolded_export_time_seconds", x.unfolded_export_time_seconds),
      f.field("export_time", x.export_time),
      f.field("sequence_number", x.sequence_number),
      f.field("domain_id", x.domain_id), f.field("engine_type", x.engine_type),
      f.field("engine_id", x.engine_id),
      f.field("sampling_mode", x.sampling_mode),
      f.field("sampling_interval", x.sampling_interval));
  }
};

auto parse_header(std::span<const std::byte> bytes, MessageHeader& result,
                  int64_t reference_unix_seconds) -> Option<std::string> {
  auto cursor = Cursor{bytes};
  auto version = uint16_t{0};
  if (not cursor.read_u16(version)) {
    return std::string{"message is shorter than the version field"};
  }
  if (not is_supported_version(version)) {
    return fmt::format("unsupported NetFlow version {}", version);
  }
  result.version = static_cast<Version>(version);
  if (result.version == Version::v5) {
    auto uptime = uint32_t{0};
    auto seconds = uint32_t{0};
    auto nanoseconds = uint32_t{0};
    auto sampling = uint16_t{0};
    if (not cursor.read_u16(result.count) or not cursor.read_u32(uptime)
        or not cursor.read_u32(seconds) or not cursor.read_u32(nanoseconds)
        or not cursor.read_u32(result.sequence_number)
        or not cursor.read_u8(result.engine_type)
        or not cursor.read_u8(result.engine_id)
        or not cursor.read_u16(sampling)) {
      return std::string{"truncated NetFlow v5 header"};
    }
    if (result.count > 30) {
      return fmt::format("invalid NetFlow v5 record count {}", result.count);
    }
    if (nanoseconds >= 1'000'000'000) {
      return fmt::format("NetFlow v5 unix_nsecs {} is outside [0, 1000000000)",
                         nanoseconds);
    }
    result.sys_uptime = std::chrono::milliseconds{uptime};
    result.export_time_seconds = seconds;
    result.unfolded_export_time_seconds
      = unfold_seconds(seconds, reference_unix_seconds);
    auto export_time
      = make_time(result.unfolded_export_time_seconds, nanoseconds);
    if (not export_time) {
      return std::string{"NetFlow v5 export time is outside the supported "
                         "range"};
    }
    result.export_time = *export_time;
    result.sampling_mode = static_cast<uint8_t>(sampling >> 14);
    result.sampling_interval = sampling & uint16_t{0x3fff};
    result.length = static_cast<uint16_t>(24 + result.count * 48);
    return None{};
  }
  if (result.version == Version::v9) {
    auto uptime = uint32_t{0};
    auto seconds = uint32_t{0};
    if (not cursor.read_u16(result.count) or not cursor.read_u32(uptime)
        or not cursor.read_u32(seconds)
        or not cursor.read_u32(result.sequence_number)
        or not cursor.read_u32(result.domain_id)) {
      return std::string{"truncated NetFlow v9 header"};
    }
    result.sys_uptime = std::chrono::milliseconds{uptime};
    result.export_time_seconds = seconds;
    result.unfolded_export_time_seconds
      = unfold_seconds(seconds, reference_unix_seconds);
    auto export_time = make_time(result.unfolded_export_time_seconds);
    if (not export_time) {
      return std::string{"NetFlow v9 export time is outside the supported "
                         "range"};
    }
    result.export_time = *export_time;
    result.length = static_cast<uint16_t>(bytes.size());
    return None{};
  }
  auto seconds = uint32_t{0};
  if (not cursor.read_u16(result.length) or not cursor.read_u32(seconds)
      or not cursor.read_u32(result.sequence_number)
      or not cursor.read_u32(result.domain_id)) {
    return std::string{"truncated IPFIX header"};
  }
  if (result.length < 16) {
    return fmt::format("invalid IPFIX message length {}", result.length);
  }
  result.export_time_seconds = seconds;
  result.unfolded_export_time_seconds
    = unfold_seconds(seconds, reference_unix_seconds);
  auto export_time = make_time(result.unfolded_export_time_seconds);
  if (not export_time) {
    return std::string{"IPFIX export time is outside the supported range"};
  }
  result.export_time = *export_time;
  return None{};
}

struct ExporterKey {
  Version version = Version::v5;
  bool implicit = true;
  ip address = {};
  uint16_t port = 0;
  uint32_t domain_id = 0;

  friend auto operator<=>(ExporterKey const&, ExporterKey const&) = default;

  friend auto inspect(auto& f, ExporterKey& x) -> bool {
    return f.object(x).fields(f.field("version", x.version),
                              f.field("implicit", x.implicit),
                              f.field("address", x.address),
                              f.field("port", x.port),
                              f.field("domain_id", x.domain_id));
  }
};

struct FieldDefinition {
  uint16_t id = 0;
  uint16_t length = 0;
  uint32_t enterprise_number = 0;
  std::string name;
  InformationElementType type = InformationElementType::octet_array;
  bool unknown = false;
  bool unsupported_structured = false;

  friend auto inspect(auto& f, FieldDefinition& x) -> bool {
    return f.object(x).fields(f.field("id", x.id), f.field("length", x.length),
                              f.field("enterprise_number", x.enterprise_number),
                              f.field("name", x.name), f.field("type", x.type),
                              f.field("unknown", x.unknown),
                              f.field("unsupported_structured",
                                      x.unsupported_structured));
  }
};

struct TemplateDefinition {
  uint16_t id = 0;
  RecordKind record_kind = RecordKind::flow;
  std::vector<FieldDefinition> fields;
  uint64_t last_used_generation = 0;

  friend auto inspect(auto& f, TemplateDefinition& x) -> bool {
    return f.object(x).fields(
      f.field("id", x.id), f.field("record_kind", x.record_kind),
      f.field("fields", x.fields),
      f.field("last_used_generation", x.last_used_generation));
  }
};

struct ExporterState {
  std::map<uint16_t, TemplateDefinition> templates;
  Option<uint32_t> last_sys_uptime_milliseconds;
  Option<uint32_t> last_sequence_number;
  Option<int64_t> last_unfolded_export_time_seconds;
  uint64_t accepted_message_generation = 0;
  uint64_t last_used_generation = 0;

  friend auto inspect(auto& f, ExporterState& x) -> bool {
    return f.object(x).fields(
      f.field("templates", x.templates),
      f.field("last_sys_uptime_milliseconds", x.last_sys_uptime_milliseconds),
      f.field("last_sequence_number", x.last_sequence_number),
      f.field("last_unfolded_export_time_seconds",
              x.last_unfolded_export_time_seconds),
      f.field("accepted_message_generation", x.accepted_message_generation),
      f.field("last_used_generation", x.last_used_generation));
  }
};

enum class MessageOrder : uint8_t {
  newer,
  reordered,
  restart,
};

auto message_order(ExporterState const* state, MessageHeader const& header)
  -> MessageOrder {
  TENZIR_ASSERT(header.version == Version::v9
                or header.version == Version::ipfix);
  if (not state or not state->last_sequence_number
      or not state->last_unfolded_export_time_seconds) {
    return MessageOrder::newer;
  }
  auto const sequence_delta
    = modular_delta(header.sequence_number, *state->last_sequence_number);
  if (header.version == Version::ipfix) {
    if (header.unfolded_export_time_seconds
          < *state->last_unfolded_export_time_seconds
        or (header.unfolded_export_time_seconds
              == *state->last_unfolded_export_time_seconds
            and sequence_delta < 0)) {
      return MessageOrder::reordered;
    }
    // IPFIX has no uptime that distinguishes a process restart from sequence
    // number rollover. After rejecting reordered messages, conservatively
    // invalidate templates for any remaining raw sequence decrease. This also
    // covers process resets within the same one-second export-time bucket.
    // Treating a real rollover as a restart can only delay records until
    // templates refresh; retaining templates across an actual restart can
    // silently misdecode.
    if (header.unfolded_export_time_seconds
          >= *state->last_unfolded_export_time_seconds
        and header.sequence_number < *state->last_sequence_number) {
      return MessageOrder::restart;
    }
    return MessageOrder::newer;
  }
  if (header.unfolded_export_time_seconds
        < *state->last_unfolded_export_time_seconds
      or (header.unfolded_export_time_seconds
            == *state->last_unfolded_export_time_seconds
          and sequence_delta < 0)) {
    return MessageOrder::reordered;
  }
  if (sequence_delta < 0) {
    return MessageOrder::restart;
  }
  if (not state->last_sys_uptime_milliseconds) {
    return MessageOrder::newer;
  }
  auto const uptime = static_cast<uint32_t>(
    std::chrono::duration_cast<std::chrono::milliseconds>(header.sys_uptime)
      .count());
  if (modular_delta(uptime, *state->last_sys_uptime_milliseconds) < 0) {
    return MessageOrder::restart;
  }
  return MessageOrder::newer;
}

struct BufferedDataSet {
  uint16_t template_id = 0;
  Option<size_t> record_count;
  Option<Arc<const TemplateDefinition>> definition;
  std::vector<std::byte> payload;

  friend auto inspect(auto& f, BufferedDataSet& x) -> bool {
    auto definition = Option<TemplateDefinition>{None{}};
    if constexpr (not std::remove_cvref_t<decltype(f)>::is_loading) {
      if (x.definition) {
        definition = **x.definition;
      }
    }
    auto result = f.object(x).fields(f.field("template_id", x.template_id),
                                     f.field("record_count", x.record_count),
                                     f.field("definition", definition),
                                     f.field("payload", x.payload));
    if constexpr (std::remove_cvref_t<decltype(f)>::is_loading) {
      if (result and definition) {
        x.definition = Arc<const TemplateDefinition>{std::in_place,
                                                     std::move(*definition)};
      } else if (result) {
        x.definition = None{};
      }
    }
    return result;
  }
};

struct BufferedMessage {
  ExporterKey exporter;
  MessageHeader header;
  Option<Peer> peer;
  Option<size_t> expected_record_count;
  std::vector<BufferedDataSet> data_sets;
  size_t definition_field_count = 0;
  Clock::time_point added_at = Clock::now();
  uint64_t added_generation = 0;

  friend auto inspect(auto& f, BufferedMessage& x) -> bool {
    // `added_at` uses a steady clock and cannot survive process restoration.
    // A restored message starts a fresh wall-clock TTL, while the serialized
    // exporter-local generation preserves the deterministic message-count
    // expiry bound.
    return f.object(x).fields(
      f.field("exporter", x.exporter), f.field("header", x.header),
      f.field("peer", x.peer),
      f.field("expected_record_count", x.expected_record_count),
      f.field("data_sets", x.data_sets),
      f.field("definition_field_count", x.definition_field_count),
      f.field("added_generation", x.added_generation));
  }
};

struct TemplateAction {
  RecordKind record_kind = RecordKind::flow;
  uint16_t template_id = 0;
  bool withdraw_all = false;
  Option<TemplateDefinition> definition;
};

struct ParsedSet {
  Option<uint16_t> data_template_id;
  Option<size_t> data_record_count;
  Option<Arc<const TemplateDefinition>> data_definition;
  bool data_definition_is_local = false;
  std::span<const std::byte> payload;
  std::vector<size_t> record_count_candidates;
  std::vector<TemplateAction> template_actions;
};

struct TemplateOverlay {
  std::map<uint16_t, Option<Arc<const TemplateDefinition>>> templates;
  bool withdrew_all_flows = false;
  bool withdrew_all_options = false;
};

auto make_exporter_key(MessageHeader const& header, Option<Peer> const& peer)
  -> ExporterKey {
  return ExporterKey{
    .version = header.version,
    .implicit = peer.is_none(),
    .address = peer ? peer->address : ip{},
    .port = static_cast<uint16_t>(
      header.version == Version::ipfix and peer ? peer->port : 0),
    .domain_id = header.domain_id,
  };
}

auto metadata_from(MessageHeader const& header, Option<Peer> peer,
                   RecordKind record_kind, Option<uint16_t> template_id)
  -> Metadata {
  auto result = Metadata{
    .version = header.version,
    .record_kind = record_kind,
    .export_time = header.export_time,
    .sequence_number = header.sequence_number,
    .observation_domain_id = None{},
    .template_id = template_id,
    .sys_uptime = None{},
    .exporter = std::move(peer),
    .v5 = None{},
  };
  if (header.version != Version::v5) {
    result.observation_domain_id = header.domain_id;
  }
  if (header.version == Version::v5 or header.version == Version::v9) {
    result.sys_uptime = header.sys_uptime;
  }
  if (header.version == Version::v5) {
    result.v5 = V5Metadata{
      .engine_type = header.engine_type,
      .engine_id = header.engine_id,
      .sampling_mode = header.sampling_mode,
      .sampling_interval = header.sampling_interval,
    };
  }
  return result;
}

auto v5_flow_time(MessageHeader const& header, uint32_t flow_uptime) -> time {
  auto const export_uptime = static_cast<uint32_t>(
    std::chrono::duration_cast<std::chrono::milliseconds>(header.sys_uptime)
      .count());
  return header.export_time
         + std::chrono::milliseconds{modular_delta(flow_uptime, export_uptime)};
}

auto malformed(std::string message) -> DecodeResult {
  return DecodeResult{
    .records = {},
    .error = DecodeError{
      .kind = DecodeErrorKind::malformed,
      .version = 0,
      .message = std::move(message),
    },
  };
}

} // namespace

struct Decoder::State {
  auto erase_template(ExporterState& state,
                      std::map<uint16_t, TemplateDefinition>::iterator position)
    -> void {
    TENZIR_ASSERT(position != state.templates.end());
    TENZIR_ASSERT(template_count > 0);
    TENZIR_ASSERT(template_field_count >= position->second.fields.size());
    --template_count;
    template_field_count -= position->second.fields.size();
    state.templates.erase(position);
  }

  auto clear_templates(ExporterState& state) -> void {
    TENZIR_ASSERT(template_count >= state.templates.size());
    template_count -= state.templates.size();
    for (auto const& definition : state.templates | std::views::values) {
      TENZIR_ASSERT(template_field_count >= definition.fields.size());
      template_field_count -= definition.fields.size();
    }
    state.templates.clear();
  }

  auto erase_buffered(std::deque<BufferedMessage>::iterator position)
    -> std::deque<BufferedMessage>::iterator {
    for (auto const& data_set : position->data_sets) {
      TENZIR_ASSERT(buffered_bytes >= data_set.payload.size());
      buffered_bytes -= data_set.payload.size();
    }
    TENZIR_ASSERT(buffered_set_count >= position->data_sets.size());
    buffered_set_count -= position->data_sets.size();
    TENZIR_ASSERT(buffered_template_field_count
                  >= position->definition_field_count);
    buffered_template_field_count -= position->definition_field_count;
    return buffered_messages.erase(position);
  }

  auto expire_buffered(diagnostic_handler& dh, bool final = false) -> void {
    auto const now = Clock::now();
    for (auto index = buffered_messages.begin();
         index != buffered_messages.end();) {
      // Exporter eviction drops that exporter's buffered messages, so the
      // lookup should always succeed; treat a missing exporter as expired
      // instead of relying on that invariant.
      auto const exporter = exporters.find(index->exporter);
      auto const expired = final or now - index->added_at >= buffered_set_ttl
                           or exporter == exporters.end()
                           or exporter->second.accepted_message_generation
                                  - index->added_generation
                                >= max_buffer_generations;
      if (not expired) {
        ++index;
        continue;
      }
      for (auto const& data_set : index->data_sets) {
        diagnostic::warning("discarded buffered NetFlow data set")
          .note("template {} did not arrive before the data set expired",
                data_set.template_id)
          .emit(dh);
      }
      index = erase_buffered(index);
    }
  }

  auto drop_buffered_for(ExporterKey const& key, diagnostic_handler& dh,
                         std::string_view reason) -> void {
    for (auto index = buffered_messages.begin();
         index != buffered_messages.end();) {
      if (index->exporter != key) {
        ++index;
        continue;
      }
      for (auto const& data_set : index->data_sets) {
        diagnostic::warning("discarded buffered NetFlow data set")
          .note("template {} data was discarded {}", data_set.template_id,
                reason)
          .emit(dh);
      }
      index = erase_buffered(index);
    }
  }

  auto drop_buffered_template(ExporterKey const& key, uint16_t template_id,
                              diagnostic_handler& dh, std::string_view reason)
    -> void {
    for (auto index = buffered_messages.begin();
         index != buffered_messages.end();) {
      auto const depends_on_template
        = std::ranges::any_of(index->data_sets, [&](auto const& data_set) {
            return data_set.template_id == template_id
                   and not data_set.definition;
          });
      if (index->exporter != key or not depends_on_template) {
        ++index;
        continue;
      }
      diagnostic::warning("discarded buffered NetFlow data set")
        .note("template {} data was discarded {}", template_id, reason)
        .emit(dh);
      index = erase_buffered(index);
    }
  }

  auto exporter(ExporterKey const& key, diagnostic_handler& dh)
    -> ExporterState& {
    if (auto position = exporters.find(key); position != exporters.end()) {
      position->second.last_used_generation = generation;
      return position->second;
    }
    if (exporters.size() >= max_exporters) {
      auto oldest
        = std::ranges::min_element(exporters, {}, [](auto const& item) {
            return item.second.last_used_generation;
          });
      TENZIR_ASSERT(oldest != exporters.end());
      auto evicted_key = oldest->first;
      clear_templates(oldest->second);
      exporters.erase(oldest);
      drop_buffered_for(evicted_key, dh, "after exporter state was evicted");
      diagnostic::warning("evicted inactive NetFlow exporter state")
        .note("the decoder retains at most {} exporters", max_exporters)
        .emit(dh);
    }
    auto [position, inserted] = exporters.emplace(key, ExporterState{});
    TENZIR_ASSERT(inserted);
    position->second.last_used_generation = generation;
    return position->second;
  }

  auto resolve_field(uint16_t id, uint16_t length, uint32_t enterprise_number)
    -> FieldDefinition {
    auto result = FieldDefinition{
      .id = id,
      .length = length,
      .enterprise_number = enterprise_number,
      .name = {},
      .type = InformationElementType::octet_array,
      .unknown = false,
      .unsupported_structured = false,
    };
    if (enterprise_number == 0) {
      if (auto const* element = standard_element(id)) {
        result.name = element->name;
        result.type = element->type;
      } else {
        result.name = fmt::format("ie_{}", id);
        result.unknown = true;
      }
    } else if (enterprise_number == 29305) {
      if (auto const* element = standard_element(id)) {
        result.name = fmt::format("reverse_{}", element->name);
        result.type = element->type;
      } else {
        result.name = fmt::format("pen_{}_ie_{}", enterprise_number, id);
        result.unknown = true;
      }
    } else if (enterprise_number == 3054) {
      if (auto const* element = ixia_element(id)) {
        result.name = element->name;
        result.type = element->type;
      } else {
        result.name = fmt::format("pen_{}_ie_{}", enterprise_number, id);
        result.unknown = true;
      }
    } else {
      result.name = fmt::format("pen_{}_ie_{}", enterprise_number, id);
      result.unknown = true;
    }
    result.unsupported_structured
      = result.type == InformationElementType::basic_list
        or result.type == InformationElementType::sub_template_list
        or result.type == InformationElementType::sub_template_multi_list;
    return result;
  }

  auto deduplicate_field_names(std::vector<FieldDefinition>& fields) -> void {
    auto duplicates = std::map<std::string, size_t>{};
    for (auto& field : fields) {
      auto [position, inserted] = duplicates.emplace(field.name, 1);
      if (inserted) {
        continue;
      }
      ++position->second;
      field.name = fmt::format("{}_{}", field.name, position->second);
    }
  }

  auto parse_field(Cursor& cursor, std::vector<FieldDefinition>& fields,
                   size_t& message_field_count, bool v9_scope = false)
    -> Option<std::string> {
    auto raw_id = uint16_t{0};
    auto length = uint16_t{0};
    if (not cursor.read_u16(raw_id) or not cursor.read_u16(length)) {
      return std::string{"truncated template field specifier"};
    }
    auto const enterprise = (raw_id & uint16_t{0x8000}) != 0;
    auto const id = raw_id & uint16_t{0x7fff};
    auto enterprise_number = uint32_t{0};
    if (enterprise and not cursor.read_u32(enterprise_number)) {
      return std::string{"truncated enterprise field specifier"};
    }
    if (length == 0) {
      return fmt::format("information element {} has zero length", id);
    }
    if (message_field_count == max_template_fields) {
      return fmt::format("NetFlow message contains more than {} template "
                         "fields",
                         max_template_fields);
    }
    ++message_field_count;
    auto field = resolve_field(id, length, enterprise_number);
    if (v9_scope and not enterprise) {
      field.name = [&] {
        switch (id) {
          case 1:
            return std::string{"scope_system"};
          case 2:
            return std::string{"scope_interface"};
          case 3:
            return std::string{"scope_line_card"};
          case 4:
            return std::string{"scope_cache"};
          case 5:
            return std::string{"scope_template"};
          default:
            return fmt::format("scope_{}", id);
        }
      }();
      field.type = InformationElementType::unsigned64;
      field.unknown = id == 0 or id > 5;
    }
    fields.push_back(std::move(field));
    return None{};
  }

  auto parse_fields(Cursor& cursor, size_t count,
                    std::vector<FieldDefinition>& fields,
                    size_t& message_field_count) -> Option<std::string> {
    if (count > 1024) {
      return fmt::format("template declares too many fields ({})", count);
    }
    if (count > max_template_fields - message_field_count) {
      return fmt::format("NetFlow message contains more than {} template "
                         "fields",
                         max_template_fields);
    }
    fields.reserve(fields.size() + count);
    for (auto index = size_t{0}; index < count; ++index) {
      if (auto error = parse_field(cursor, fields, message_field_count)) {
        return error;
      }
    }
    deduplicate_field_names(fields);
    return None{};
  }

  auto parse_fields_by_length(Cursor& cursor, size_t length,
                              std::vector<FieldDefinition>& fields,
                              size_t& message_field_count, bool v9_scope)
    -> Option<std::string> {
    auto bytes = std::span<const std::byte>{};
    if (not cursor.take(length, bytes)) {
      return std::string{"truncated NetFlow v9 options template fields"};
    }
    auto fields_cursor = Cursor{bytes};
    while (fields_cursor.remaining() > 0) {
      if (fields.size() >= 1024) {
        return std::string{"template declares too many fields"};
      }
      if (auto error
          = parse_field(fields_cursor, fields, message_field_count, v9_scope)) {
        return error;
      }
    }
    return None{};
  }

  auto same_template(TemplateDefinition const& lhs,
                     TemplateDefinition const& rhs) const -> bool {
    if (lhs.record_kind != rhs.record_kind
        or lhs.fields.size() != rhs.fields.size()) {
      return false;
    }
    return std::ranges::equal(
      lhs.fields, rhs.fields, [](auto const& x, auto const& y) {
        return x.id == y.id and x.length == y.length
               and x.enterprise_number == y.enterprise_number
               and x.name == y.name and x.type == y.type
               and x.unknown == y.unknown
               and x.unsupported_structured == y.unsupported_structured;
      });
  }

  auto warn_template_fields(TemplateDefinition const& definition,
                            diagnostic_handler& dh) const -> void {
    auto warned_unknown = false;
    auto warned_structured = false;
    for (auto const& field : definition.fields) {
      if (field.unknown and not warned_unknown) {
        diagnostic::warning("unknown NetFlow information element")
          .note("template {} preserves unknown fields as blobs", definition.id)
          .emit(dh);
        warned_unknown = true;
      }
      if (field.unsupported_structured and not warned_structured) {
        diagnostic::warning("unsupported structured IPFIX information element")
          .note("template {} preserves structured fields as blobs",
                definition.id)
          .emit(dh);
        warned_structured = true;
      }
    }
  }

  auto remove_oldest_template(ExporterState& state, diagnostic_handler& dh)
    -> void {
    auto oldest
      = std::ranges::min_element(state.templates, {}, [](auto const& item) {
          return item.second.last_used_generation;
        });
    TENZIR_ASSERT(oldest != state.templates.end());
    auto const id = oldest->first;
    erase_template(state, oldest);
    diagnostic::warning("evicted inactive NetFlow template")
      .note("template {} was evicted; each exporter retains at most {} "
            "templates",
            id, max_templates_per_exporter)
      .emit(dh);
  }

  auto remove_oldest_template() -> void {
    auto* oldest_state = static_cast<ExporterState*>(nullptr);
    auto oldest_id = uint16_t{0};
    auto oldest_generation = std::numeric_limits<uint64_t>::max();
    for (auto& state : exporters | std::views::values) {
      for (auto const& [id, definition] : state.templates) {
        if (oldest_state
            and definition.last_used_generation >= oldest_generation) {
          continue;
        }
        oldest_state = &state;
        oldest_id = id;
        oldest_generation = definition.last_used_generation;
      }
    }
    TENZIR_ASSERT(oldest_state);
    erase_template(*oldest_state, oldest_state->templates.find(oldest_id));
  }

  auto validate_data_payload(TemplateDefinition const& definition,
                             std::span<const std::byte> payload)
    -> Option<std::string> {
    auto minimum_record_size = size_t{0};
    for (auto const& field : definition.fields) {
      minimum_record_size += field.length == variable_length ? 1 : field.length;
    }
    if (minimum_record_size == 0) {
      return fmt::format("template {} consumes no input", definition.id);
    }
    auto cursor = Cursor{payload};
    while (cursor.remaining() >= minimum_record_size) {
      for (auto const& field : definition.fields) {
        auto length = size_t{field.length};
        if (field.length == variable_length) {
          auto short_length = uint8_t{0};
          if (not cursor.read_u8(short_length)) {
            return std::string{"truncated variable-length field prefix"};
          }
          if (short_length < 255) {
            length = short_length;
          } else {
            auto long_length = uint16_t{0};
            if (not cursor.read_u16(long_length)) {
              return std::string{"truncated variable-length field prefix"};
            }
            length = long_length;
          }
        }
        auto ignored = std::span<const std::byte>{};
        if (not cursor.take(length, ignored)) {
          return fmt::format("truncated data record for template {}",
                             definition.id);
        }
      }
    }
    return None{};
  }

  enum class DataPayloadErrorKind : uint8_t {
    malformed,
    work_limit,
  };

  struct DataPayloadError {
    DataPayloadErrorKind kind;
    std::string message;
  };

  struct DecodeBudget {
    size_t remaining_records = max_decoded_records;
    size_t remaining_fields = max_decoded_fields;

    auto consume(size_t fields) -> bool {
      if (remaining_records == 0 or fields > remaining_fields) {
        remaining_records = 0;
        remaining_fields = 0;
        return false;
      }
      --remaining_records;
      remaining_fields -= fields;
      return true;
    }
  };

  auto decode_data_payload(MessageHeader const& header, Option<Peer> peer,
                           TemplateDefinition const& definition,
                           std::span<const std::byte> payload,
                           Option<size_t> expected_record_count,
                           std::vector<DecodedRecord>& output,
                           DecodeBudget& budget) -> Option<DataPayloadError> {
    auto minimum_record_size = size_t{0};
    for (auto const& field : definition.fields) {
      minimum_record_size += field.length == variable_length ? 1 : field.length;
    }
    if (minimum_record_size == 0) {
      return DataPayloadError{
        .kind = DataPayloadErrorKind::malformed,
        .message = fmt::format("template {} consumes no input", definition.id),
      };
    }
    auto cursor = Cursor{payload};
    auto record_count = size_t{0};
    while (cursor.remaining() >= minimum_record_size
           and (not expected_record_count
                or record_count < *expected_record_count)) {
      if (not budget.consume(definition.fields.size())) {
        return DataPayloadError{
          .kind = DataPayloadErrorKind::work_limit,
          .message = fmt::format("a read_netflow input exceeds the decoded "
                                 "work limit of {} "
                                 "records or {} fields",
                                 max_decoded_records, max_decoded_fields),
        };
      }
      auto record = DecodedRecord{
        .metadata
        = metadata_from(header, peer, definition.record_kind, definition.id),
        .fields = {},
      };
      record.fields.reserve(definition.fields.size());
      auto const before = cursor.offset();
      for (auto const& field : definition.fields) {
        auto length = size_t{field.length};
        if (field.length == variable_length) {
          auto short_length = uint8_t{0};
          if (not cursor.read_u8(short_length)) {
            return DataPayloadError{
              .kind = DataPayloadErrorKind::malformed,
              .message = "truncated variable-length field prefix",
            };
          }
          if (short_length < 255) {
            length = short_length;
          } else {
            auto long_length = uint16_t{0};
            if (not cursor.read_u16(long_length)) {
              return DataPayloadError{
                .kind = DataPayloadErrorKind::malformed,
                .message = "truncated variable-length field prefix",
              };
            }
            length = long_length;
          }
        }
        auto bytes = std::span<const std::byte>{};
        if (not cursor.take(length, bytes)) {
          return DataPayloadError{
            .kind = DataPayloadErrorKind::malformed,
            .message = fmt::format("truncated data record for template {}",
                                   definition.id),
          };
        }
        record.fields.push_back(DecodedField{
          field.name,
          decode_value(field.type, bytes, header.unfolded_export_time_seconds),
        });
      }
      TENZIR_ASSERT(cursor.offset() > before);
      output.push_back(std::move(record));
      ++record_count;
    }
    if (expected_record_count
        and (record_count != *expected_record_count
             or cursor.remaining() > 3)) {
      return DataPayloadError{
        .kind = DataPayloadErrorKind::malformed,
        .message = fmt::format(
          "NetFlow v9 record count does not match template {}", definition.id),
      };
    }
    return None{};
  }

  auto
  decode_data_payload_bounded(MessageHeader const& header, Option<Peer> peer,
                              TemplateDefinition const& definition,
                              std::span<const std::byte> payload,
                              Option<size_t> expected_record_count,
                              std::vector<DecodedRecord>& output,
                              DecodeBudget& budget, diagnostic_handler& dh)
    -> void {
    auto decoded = std::vector<DecodedRecord>{};
    auto error = decode_data_payload(header, peer, definition, payload,
                                     expected_record_count, decoded, budget);
    if (error) {
      TENZIR_ASSERT(error->kind == DataPayloadErrorKind::work_limit);
      diagnostic::warning("discarded NetFlow data set after decoded work limit")
        .note("{}", error->message)
        .emit(dh);
      return;
    }
    for (auto& record : decoded) {
      output.push_back(std::move(record));
    }
  }

  auto replay_buffered(ExporterKey const& key, diagnostic_handler& dh,
                       std::vector<DecodedRecord>& output, DecodeBudget& budget)
    -> void {
    auto& state = exporters.at(key);
    for (auto index = buffered_messages.begin();
         index != buffered_messages.end();) {
      if (index->exporter != key) {
        ++index;
        continue;
      }
      auto definitions = std::vector<TemplateDefinition const*>{};
      definitions.reserve(index->data_sets.size());
      auto complete = true;
      for (auto const& data_set : index->data_sets) {
        if (data_set.definition) {
          auto const* definition = &**data_set.definition;
          definitions.push_back(definition);
          continue;
        }
        auto position = state.templates.find(data_set.template_id);
        if (position == state.templates.end()) {
          complete = false;
          break;
        }
        definitions.push_back(&position->second);
      }
      if (not complete) {
        ++index;
        continue;
      }
      for (auto data_set_index = size_t{0};
           data_set_index < index->data_sets.size(); ++data_set_index) {
        auto position
          = state.templates.find(index->data_sets[data_set_index].template_id);
        if (position != state.templates.end()
            and same_template(position->second, *definitions[data_set_index])) {
          position->second.last_used_generation = generation;
        }
      }
      auto record_counts
        = std::vector<Option<size_t>>(index->data_sets.size(), None{});
      auto error = Option<std::string>{};
      if (index->expected_record_count) {
        auto candidate_storage = std::vector<std::vector<size_t>>{};
        candidate_storage.reserve(index->data_sets.size());
        for (auto data_set_index = size_t{0};
             data_set_index < index->data_sets.size(); ++data_set_index) {
          candidate_storage.emplace_back();
          auto& candidates = candidate_storage.back();
          auto const& data_set = index->data_sets[data_set_index];
          if (data_set.record_count) {
            candidates.push_back(*data_set.record_count);
          } else {
            auto fields = FramingTemplate{};
            fields.reserve(definitions[data_set_index]->fields.size());
            for (auto const& field : definitions[data_set_index]->fields) {
              fields.push_back(field.length);
            }
            error
              = framed_data_record_counts(data_set.payload, fields, candidates);
            if (error) {
              break;
            }
          }
        }
        if (not error) {
          auto candidate_sets = std::vector<std::span<const size_t>>{};
          candidate_sets.reserve(candidate_storage.size());
          for (auto const& candidates : candidate_storage) {
            candidate_sets.push_back(candidates);
          }
          auto selected = std::vector<size_t>{};
          auto const selection = select_data_record_counts(
            candidate_sets, *index->expected_record_count, selected);
          if (selection == RecordCountSelection::no_match) {
            error = fmt::format("NetFlow v9 header record count {} does not "
                                "match the buffered data sets",
                                *index->expected_record_count);
          } else if (selection == RecordCountSelection::ambiguous) {
            error = std::string{
              "NetFlow v9 record count has an ambiguous buffered data set "
              "assignment"};
          } else {
            for (auto data_set_index = size_t{0};
                 data_set_index < selected.size(); ++data_set_index) {
              record_counts[data_set_index] = selected[data_set_index];
            }
          }
        }
      } else {
        for (auto data_set_index = size_t{0};
             data_set_index < index->data_sets.size(); ++data_set_index) {
          record_counts[data_set_index]
            = index->data_sets[data_set_index].record_count;
        }
      }
      auto decoded = std::vector<DecodedRecord>{};
      auto decode_error = Option<DataPayloadError>{};
      if (not error) {
        for (auto data_set_index = size_t{0};
             data_set_index < index->data_sets.size(); ++data_set_index) {
          auto const& data_set = index->data_sets[data_set_index];
          decode_error = decode_data_payload(
            index->header, index->peer, *definitions[data_set_index],
            data_set.payload, record_counts[data_set_index], decoded, budget);
          if (decode_error) {
            break;
          }
        }
      }
      if (error) {
        diagnostic::warning("discarded malformed buffered NetFlow data set")
          .note("{}", *error)
          .emit(dh);
      } else if (decode_error) {
        if (decode_error->kind == DataPayloadErrorKind::work_limit) {
          diagnostic::warning(
            "discarded buffered NetFlow data after decoded work limit")
            .note("{}", decode_error->message)
            .emit(dh);
        } else {
          diagnostic::warning("discarded malformed buffered NetFlow data set")
            .note("{}", decode_error->message)
            .emit(dh);
        }
      } else {
        for (auto& record : decoded) {
          output.push_back(std::move(record));
        }
      }
      index = erase_buffered(index);
    }
  }

  auto
  install_template(ExporterKey const& key, TemplateDefinition definition,
                   diagnostic_handler& dh, std::vector<DecodedRecord>& output,
                   DecodeBudget& budget) -> void {
    auto& state = exporters.at(key);
    auto position = state.templates.find(definition.id);
    if (position != state.templates.end()
        and same_template(position->second, definition)) {
      position->second.last_used_generation = generation;
      replay_buffered(key, dh, output, budget);
      return;
    }
    if (position != state.templates.end()) {
      // Exporters may legitimately redefine a template ID, but without peer
      // information distinct exporters collapse into one implicit key, where
      // interleaved traffic redefines templates on every switch and silently
      // decodes data sets with the wrong layout.
      if (key.implicit) {
        diagnostic::warning("redefined NetFlow template")
          .note("template {} arrived with a different layout", definition.id)
          .note("mixing messages from multiple exporters without peer "
                "information causes template redefinitions and may decode "
                "records incorrectly")
          .emit(dh);
      }
      erase_template(state, position);
    }
    if (state.templates.size() >= max_templates_per_exporter) {
      remove_oldest_template(state, dh);
    }
    auto evicted = size_t{0};
    while (template_count >= max_templates
           or template_field_count + definition.fields.size()
                > max_template_fields) {
      remove_oldest_template();
      ++evicted;
    }
    if (evicted > 0) {
      diagnostic::warning("evicted inactive NetFlow template state")
        .note("evicted {} template{}; the decoder retains at most {} "
              "templates and {} template fields",
              evicted, evicted == 1 ? "" : "s", max_templates,
              max_template_fields)
        .emit(dh);
    }
    definition.last_used_generation = generation;
    warn_template_fields(definition, dh);
    ++template_count;
    template_field_count += definition.fields.size();
    state.templates.insert_or_assign(definition.id, std::move(definition));
    replay_buffered(key, dh, output, budget);
  }

  auto withdraw_templates(ExporterKey const& key, ExporterState& state,
                          RecordKind record_kind, uint16_t template_id,
                          bool all, diagnostic_handler& dh) -> void {
    if (not all) {
      if (auto position = state.templates.find(template_id);
          position != state.templates.end()) {
        erase_template(state, position);
      }
      drop_buffered_template(key, template_id, dh,
                             "after its template was withdrawn");
      return;
    }
    for (auto position = state.templates.begin();
         position != state.templates.end();) {
      if (position->second.record_kind != record_kind) {
        ++position;
        continue;
      }
      auto erased = position++;
      erase_template(state, erased);
    }
    // An unresolved data set does not reveal whether its missing definition is
    // a flow or options template. Discard every atomic buffered message group
    // for this exporter so pre-withdrawal payload cannot be replayed through a
    // definition installed after the withdrawal boundary.
    drop_buffered_for(key, dh,
                      record_kind == RecordKind::flow
                        ? "after all flow templates were withdrawn"
                        : "after all options templates were withdrawn");
  }

  auto parse_template_set(Version version, bool options,
                          std::span<const std::byte> payload,
                          std::vector<TemplateAction>& actions,
                          size_t& message_field_count) -> Option<std::string> {
    auto cursor = Cursor{payload};
    while (cursor.remaining() > 0) {
      if (cursor.remaining() <= 3) {
        return None{};
      }
      auto template_id = uint16_t{0};
      auto field_count = uint16_t{0};
      auto scope_count = uint16_t{0};
      auto v9_scope_length = uint16_t{0};
      auto v9_option_length = uint16_t{0};
      if (not cursor.read_u16(template_id)) {
        return std::string{"truncated template record"};
      }
      if (options and version == Version::v9) {
        if (not cursor.read_u16(v9_scope_length)
            or not cursor.read_u16(v9_option_length)) {
          return std::string{"truncated NetFlow v9 options template header"};
        }
        field_count = 1;
      } else if (not cursor.read_u16(field_count)) {
        return std::string{"truncated template record header"};
      }
      auto const kind = options ? RecordKind::options : RecordKind::flow;
      if (field_count == 0) {
        if (version != Version::ipfix) {
          return std::string{"NetFlow v9 template has no fields"};
        }
        auto const all = template_id == (options ? 3 : 2);
        if (not all and template_id < 256) {
          return fmt::format("invalid IPFIX template withdrawal ID {}",
                             template_id);
        }
        actions.push_back(TemplateAction{
          .record_kind = kind,
          .template_id = template_id,
          .withdraw_all = all,
          .definition = None{},
        });
        continue;
      }
      if (options and version == Version::ipfix) {
        if (not cursor.read_u16(scope_count)) {
          return std::string{"truncated IPFIX options template header"};
        }
        if (scope_count == 0) {
          return std::string{"IPFIX options template has no scope fields"};
        }
        if (scope_count > field_count) {
          return std::string{"IPFIX options scope count exceeds field count"};
        }
      }
      if (template_id < 256) {
        return fmt::format("invalid template ID {}", template_id);
      }
      auto definition = TemplateDefinition{
        .id = template_id,
        .record_kind = kind,
        .fields = {},
        .last_used_generation = generation,
      };
      if (options and version == Version::v9) {
        if (auto error
            = parse_fields_by_length(cursor, v9_scope_length, definition.fields,
                                     message_field_count, true)) {
          return error;
        }
        if (auto error = parse_fields_by_length(cursor, v9_option_length,
                                                definition.fields,
                                                message_field_count, false)) {
          return error;
        }
        if (definition.fields.empty()) {
          return std::string{"NetFlow v9 template has no fields"};
        }
        deduplicate_field_names(definition.fields);
      } else if (auto error
                 = parse_fields(cursor, field_count, definition.fields,
                                message_field_count)) {
        return error;
      }
      actions.push_back(TemplateAction{
        .record_kind = kind,
        .template_id = template_id,
        .withdraw_all = false,
        .definition = std::move(definition),
      });
    }
    return None{};
  }

  auto apply_template_action(ExporterKey const& key, ExporterState& state,
                             TemplateAction action, diagnostic_handler& dh,
                             std::vector<DecodedRecord>& output,
                             DecodeBudget& budget) -> void {
    if (action.definition) {
      install_template(key, std::move(*action.definition), dh, output, budget);
      return;
    }
    withdraw_templates(key, state, action.record_kind, action.template_id,
                       action.withdraw_all, dh);
  }

  auto buffer_data_sets(ExporterKey const& key, MessageHeader const& header,
                        Option<Peer> peer, Option<size_t> expected_record_count,
                        std::vector<BufferedDataSet> data_sets,
                        diagnostic_handler& dh) -> void {
    expire_buffered(dh);
    auto byte_size = size_t{0};
    auto definition_field_count = size_t{0};
    for (auto const& data_set : data_sets) {
      byte_size += data_set.payload.size();
      if (data_set.definition) {
        // Snapshotting serializes an inline definition for every retained data
        // set, even when their Arcs share one allocation. Count each occurrence
        // so the checkpoint representation remains bounded as well.
        definition_field_count += (*data_set.definition)->fields.size();
      }
    }
    if (data_sets.size() > max_buffered_sets or byte_size > max_buffered_bytes
        or definition_field_count > max_template_fields) {
      diagnostic::warning("discarded NetFlow data sets without templates")
        .note("the message with {} data sets, {} bytes, and {} retained "
              "template fields is too large to buffer",
              data_sets.size(), byte_size, definition_field_count)
        .emit(dh);
      return;
    }
    buffered_set_count += data_sets.size();
    buffered_bytes += byte_size;
    buffered_template_field_count += definition_field_count;
    buffered_messages.push_back(BufferedMessage{
      .exporter = key,
      .header = header,
      .peer = std::move(peer),
      .expected_record_count = expected_record_count,
      .data_sets = std::move(data_sets),
      .definition_field_count = definition_field_count,
      .added_at = Clock::now(),
      .added_generation = exporters.at(key).accepted_message_generation,
    });
    while (buffered_set_count > max_buffered_sets
           or buffered_bytes > max_buffered_bytes
           or buffered_template_field_count > max_template_fields) {
      for (auto const& data_set : buffered_messages.front().data_sets) {
        diagnostic::warning("evicted buffered NetFlow data set")
          .note("template {} data was evicted before its template arrived",
                data_set.template_id)
          .emit(dh);
      }
      erase_buffered(buffered_messages.begin());
    }
  }

  auto decode_v5(MessageHeader const& header, std::span<const std::byte> bytes,
                 Option<Peer> peer) -> DecodeResult {
    auto cursor = Cursor{bytes.subspan(24)};
    auto output = std::vector<DecodedRecord>{};
    output.reserve(header.count);
    for (auto record_index = uint16_t{0}; record_index < header.count;
         ++record_index) {
      auto src = std::span<const std::byte>{};
      auto dst = std::span<const std::byte>{};
      auto next_hop = std::span<const std::byte>{};
      auto ingress = uint16_t{0};
      auto egress = uint16_t{0};
      auto packets = uint32_t{0};
      auto octets = uint32_t{0};
      auto first = uint32_t{0};
      auto last = uint32_t{0};
      auto source_port = uint16_t{0};
      auto destination_port = uint16_t{0};
      auto padding = uint8_t{0};
      auto tcp_flags = uint8_t{0};
      auto protocol = uint8_t{0};
      auto class_of_service = uint8_t{0};
      auto source_as = uint16_t{0};
      auto destination_as = uint16_t{0};
      auto source_prefix = uint8_t{0};
      auto destination_prefix = uint8_t{0};
      auto trailing = std::span<const std::byte>{};
      if (not cursor.take(4, src) or not cursor.take(4, dst)
          or not cursor.take(4, next_hop) or not cursor.read_u16(ingress)
          or not cursor.read_u16(egress) or not cursor.read_u32(packets)
          or not cursor.read_u32(octets) or not cursor.read_u32(first)
          or not cursor.read_u32(last) or not cursor.read_u16(source_port)
          or not cursor.read_u16(destination_port)
          or not cursor.read_u8(padding) or not cursor.read_u8(tcp_flags)
          or not cursor.read_u8(protocol)
          or not cursor.read_u8(class_of_service)
          or not cursor.read_u16(source_as)
          or not cursor.read_u16(destination_as)
          or not cursor.read_u8(source_prefix)
          or not cursor.read_u8(destination_prefix)
          or not cursor.take(2, trailing)) {
        return malformed("truncated NetFlow v5 record");
      }
      auto record = DecodedRecord{
        .metadata = metadata_from(header, peer, RecordKind::flow, None{}),
        .fields = {},
      };
      record.fields = {
        {"source_ipv4_address", ip::v4(src.first<4>())},
        {"destination_ipv4_address", ip::v4(dst.first<4>())},
        {"ip_next_hop_ipv4_address", ip::v4(next_hop.first<4>())},
        {"ingress_interface", uint64_t{ingress}},
        {"egress_interface", uint64_t{egress}},
        {"packet_delta_count", uint64_t{packets}},
        {"octet_delta_count", uint64_t{octets}},
        {"flow_start", v5_flow_time(header, first)},
        {"flow_end", v5_flow_time(header, last)},
        {"source_transport_port", uint64_t{source_port}},
        {"destination_transport_port", uint64_t{destination_port}},
        {"protocol_identifier", uint64_t{protocol}},
        {"tcp_control_bits", uint64_t{tcp_flags}},
        {"ip_class_of_service", uint64_t{class_of_service}},
        {"bgp_source_as_number", uint64_t{source_as}},
        {"bgp_destination_as_number", uint64_t{destination_as}},
        {"source_ipv4_prefix_length", uint64_t{source_prefix}},
        {"destination_ipv4_prefix_length", uint64_t{destination_prefix}},
      };
      output.push_back(std::move(record));
    }
    return DecodeResult{.records = std::move(output), .error = None{}};
  }

  auto overlay_template(
    TemplateOverlay const& overlay, ExporterState const* base,
    uint16_t template_id,
    std::map<uint16_t, Arc<const TemplateDefinition>>& base_snapshots,
    bool& is_local) const -> Option<Arc<const TemplateDefinition>> {
    if (auto position = overlay.templates.find(template_id);
        position != overlay.templates.end()) {
      is_local = true;
      return position->second;
    }
    if (not base) {
      return None{};
    }
    auto const position = base->templates.find(template_id);
    if (position == base->templates.end()) {
      return None{};
    }
    if ((position->second.record_kind == RecordKind::flow
         and overlay.withdrew_all_flows)
        or (position->second.record_kind == RecordKind::options
            and overlay.withdrew_all_options)) {
      return None{};
    }
    auto snapshot = base_snapshots.find(template_id);
    if (snapshot == base_snapshots.end()) {
      snapshot = base_snapshots
                   .emplace(template_id,
                            Arc<const TemplateDefinition>{std::in_place,
                                                          position->second})
                   .first;
    }
    return snapshot->second;
  }

  auto update_overlay(TemplateOverlay& overlay,
                      TemplateAction const& action) const -> void {
    if (action.definition) {
      overlay.templates.insert_or_assign(
        action.template_id,
        Arc<const TemplateDefinition>{std::in_place, *action.definition});
      return;
    }
    if (not action.withdraw_all) {
      overlay.templates.insert_or_assign(action.template_id, None{});
      return;
    }
    auto& withdrew_all = action.record_kind == RecordKind::flow
                           ? overlay.withdrew_all_flows
                           : overlay.withdrew_all_options;
    withdrew_all = true;
    for (auto& definition : overlay.templates | std::views::values) {
      if (definition and (*definition)->record_kind == action.record_kind) {
        definition = None{};
      }
    }
  }

  auto parse_message_sets(MessageHeader const& header,
                          std::span<const std::byte> bytes,
                          ExporterState const* base,
                          std::vector<ParsedSet>& parsed_sets,
                          bool& defer_data_sets) -> Option<std::string> {
    auto overlay = TemplateOverlay{};
    auto base_snapshots = std::map<uint16_t, Arc<const TemplateDefinition>>{};
    auto message_field_count = size_t{0};
    auto offset = header.version == Version::v9 ? size_t{20} : size_t{16};
    auto const end
      = header.version == Version::ipfix ? header.length : bytes.size();
    while (offset < end) {
      if (parsed_sets.size() == max_sets_per_message) {
        return fmt::format("NetFlow message contains more than {} sets",
                           max_sets_per_message);
      }
      if (end - offset < 4) {
        if (all_zero(bytes.subspan(offset, end - offset))) {
          break;
        }
        return std::string{"truncated NetFlow set header"};
      }
      auto set_cursor = Cursor{bytes.subspan(offset, end - offset)};
      auto set_id = uint16_t{0};
      auto set_length = uint16_t{0};
      std::ignore = set_cursor.read_u16(set_id);
      std::ignore = set_cursor.read_u16(set_length);
      if (set_length < 4 or offset + set_length > end) {
        return fmt::format("invalid NetFlow set length {}", set_length);
      }
      auto const payload = bytes.subspan(offset + 4, set_length - 4);
      auto const template_set
        = (header.version == Version::v9 and set_id == 0)
          or (header.version == Version::ipfix and set_id == 2);
      auto const options_set
        = (header.version == Version::v9 and set_id == 1)
          or (header.version == Version::ipfix and set_id == 3);
      if (template_set or options_set) {
        auto parsed = ParsedSet{
          .data_template_id = None{},
          .data_record_count = None{},
          .data_definition = None{},
          .data_definition_is_local = false,
          .payload = {},
          .record_count_candidates = {},
          .template_actions = {},
        };
        if (auto error = parse_template_set(header.version, options_set,
                                            payload, parsed.template_actions,
                                            message_field_count)) {
          return error;
        }
        for (auto const& action : parsed.template_actions) {
          update_overlay(overlay, action);
        }
        parsed_sets.push_back(std::move(parsed));
      } else if (set_id >= 256) {
        auto candidates = std::vector<size_t>{};
        auto data_definition_is_local = false;
        auto data_definition = overlay_template(
          overlay, base, set_id, base_snapshots, data_definition_is_local);
        if (data_definition) {
          auto const& definition = **data_definition;
          if (header.version == Version::v9) {
            auto fields = FramingTemplate{};
            fields.reserve(definition.fields.size());
            for (auto const& field : definition.fields) {
              fields.push_back(field.length);
            }
            if (auto error
                = framed_data_record_counts(payload, fields, candidates)) {
              return error;
            }
          } else {
            if (auto error = validate_data_payload(definition, payload)) {
              return error;
            }
          }
        }
        parsed_sets.push_back(ParsedSet{
          .data_template_id = set_id,
          .data_record_count = None{},
          .data_definition = std::move(data_definition),
          .data_definition_is_local = data_definition_is_local,
          .payload = payload,
          .record_count_candidates = std::move(candidates),
          .template_actions = {},
        });
      } else {
        return fmt::format("invalid reserved NetFlow set ID {}", set_id);
      }
      offset += set_length;
    }
    if (header.version != Version::v9) {
      return None{};
    }
    auto template_record_count = size_t{0};
    auto data_set_indices = std::vector<size_t>{};
    auto unknown_data_set_count = size_t{0};
    for (auto index = size_t{0}; index < parsed_sets.size(); ++index) {
      auto const& parsed = parsed_sets[index];
      template_record_count += parsed.template_actions.size();
      if (not parsed.data_template_id) {
        continue;
      }
      data_set_indices.push_back(index);
      unknown_data_set_count += parsed.record_count_candidates.empty();
    }
    if (template_record_count > header.count) {
      return fmt::format("NetFlow v9 header record count {} is too small",
                         header.count);
    }
    auto const expected_data_records
      = size_t{header.count} - template_record_count;
    if (unknown_data_set_count > 0) {
      if (unknown_data_set_count == 1 and data_set_indices.size() == 1) {
        parsed_sets[data_set_indices.front()].data_record_count
          = expected_data_records;
      } else {
        defer_data_sets = true;
      }
      return None{};
    }
    auto candidate_sets = std::vector<std::span<const size_t>>{};
    candidate_sets.reserve(data_set_indices.size());
    for (auto index : data_set_indices) {
      candidate_sets.push_back(parsed_sets[index].record_count_candidates);
    }
    auto record_counts = std::vector<size_t>{};
    auto const selection = select_data_record_counts(
      candidate_sets, expected_data_records, record_counts);
    if (selection == RecordCountSelection::no_match) {
      return fmt::format("NetFlow v9 header record count {} does not match the "
                         "message",
                         header.count);
    }
    if (selection == RecordCountSelection::ambiguous) {
      return std::string{"NetFlow v9 record count has an ambiguous data set "
                         "assignment"};
    }
    for (auto index = size_t{0}; index < data_set_indices.size(); ++index) {
      parsed_sets[data_set_indices[index]].data_record_count
        = record_counts[index];
    }
    return None{};
  }

  auto decode_template_message(MessageHeader header,
                               std::span<const std::byte> bytes,
                               Option<Peer> peer, diagnostic_handler& dh)
    -> DecodeResult {
    auto const key = make_exporter_key(header, peer);
    auto existing = exporters.find(key);
    if (existing != exporters.end()
        and existing->second.last_unfolded_export_time_seconds) {
      header.unfolded_export_time_seconds
        = unfold_seconds(header.export_time_seconds,
                         *existing->second.last_unfolded_export_time_seconds);
      auto export_time = make_time(header.unfolded_export_time_seconds);
      if (not export_time) {
        return malformed(
          fmt::format("{} export time is outside the supported range",
                      header.version == Version::v9 ? "NetFlow v9" : "IPFIX"));
      }
      header.export_time = *export_time;
    }
    auto const* base
      = existing == exporters.end() ? nullptr : &existing->second;
    auto const order = message_order(base, header);
    auto parsed_sets = std::vector<ParsedSet>{};
    auto defer_data_sets = false;
    if (auto error = parse_message_sets(
          header, bytes, order == MessageOrder::newer ? base : nullptr,
          parsed_sets, defer_data_sets)) {
      return malformed(std::move(*error));
    }
    ++generation;
    auto& state = exporter(key, dh);
    ++state.accepted_message_generation;
    expire_buffered(dh);
    if (order == MessageOrder::restart) {
      clear_templates(state);
      drop_buffered_for(key, dh, "after an exporter restart");
    }
    if (order != MessageOrder::reordered) {
      if (header.version == Version::v9) {
        state.last_sys_uptime_milliseconds = static_cast<uint32_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
            header.sys_uptime)
            .count());
      }
      state.last_sequence_number = header.sequence_number;
      state.last_unfolded_export_time_seconds
        = header.unfolded_export_time_seconds;
    }
    auto output = std::vector<DecodedRecord>{};
    auto budget = DecodeBudget{};
    if (defer_data_sets) {
      if (order == MessageOrder::reordered) {
        auto missing_template_id = Option<uint16_t>{None{}};
        for (auto const& parsed : parsed_sets) {
          if (parsed.data_template_id and not parsed.data_definition_is_local) {
            missing_template_id = *parsed.data_template_id;
            break;
          }
        }
        TENZIR_ASSERT(missing_template_id);
        diagnostic::warning("discarded reordered {} data sets",
                            header.version == Version::v9 ? "NetFlow v9"
                                                          : "IPFIX")
          .note("template {} was not established by the delayed message; "
                "discarded the count-constrained data sets together",
                *missing_template_id)
          .emit(dh);
        return DecodeResult{.records = {}, .error = None{}};
      }
      auto data_sets = std::vector<BufferedDataSet>{};
      auto template_record_count = size_t{0};
      for (auto& parsed : parsed_sets) {
        template_record_count += parsed.template_actions.size();
        if (parsed.data_template_id) {
          data_sets.push_back(BufferedDataSet{
            .template_id = *parsed.data_template_id,
            .record_count = parsed.data_record_count,
            .definition = std::move(parsed.data_definition),
            .payload = {parsed.payload.begin(), parsed.payload.end()},
          });
          continue;
        }
        if (order == MessageOrder::reordered) {
          continue;
        }
        for (auto& action : parsed.template_actions) {
          apply_template_action(key, state, std::move(action), dh, output,
                                budget);
        }
      }
      TENZIR_ASSERT(template_record_count <= header.count);
      buffer_data_sets(key, header, peer,
                       size_t{header.count} - template_record_count,
                       std::move(data_sets), dh);
      replay_buffered(key, dh, output, budget);
      return DecodeResult{.records = std::move(output), .error = None{}};
    }
    for (auto& parsed : parsed_sets) {
      if (parsed.data_template_id) {
        auto const template_id = *parsed.data_template_id;
        if (order == MessageOrder::reordered) {
          if (parsed.data_definition_is_local and parsed.data_definition) {
            decode_data_payload_bounded(header, peer, **parsed.data_definition,
                                        parsed.payload,
                                        parsed.data_record_count, output,
                                        budget, dh);
          } else {
            diagnostic::warning("discarded reordered {} data set",
                                header.version == Version::v9 ? "NetFlow v9"
                                                              : "IPFIX")
              .note("template {} was not established by the delayed message",
                    template_id)
              .emit(dh);
          }
          continue;
        }
        if (not parsed.data_definition) {
          auto data_sets = std::vector<BufferedDataSet>{};
          data_sets.push_back(BufferedDataSet{
            .template_id = template_id,
            .record_count = parsed.data_record_count,
            .definition = None{},
            .payload = {parsed.payload.begin(), parsed.payload.end()},
          });
          buffer_data_sets(key, header, peer, None{}, std::move(data_sets), dh);
        } else {
          if (auto position = state.templates.find(template_id);
              position != state.templates.end()
              and same_template(position->second, **parsed.data_definition)) {
            position->second.last_used_generation = generation;
          }
          decode_data_payload_bounded(header, peer, **parsed.data_definition,
                                      parsed.payload, parsed.data_record_count,
                                      output, budget, dh);
        }
        continue;
      }
      if (order == MessageOrder::reordered) {
        continue;
      }
      for (auto& action : parsed.template_actions) {
        apply_template_action(key, state, std::move(action), dh, output,
                              budget);
      }
    }
    return DecodeResult{.records = std::move(output), .error = None{}};
  }

  std::map<ExporterKey, ExporterState> exporters;
  std::deque<BufferedMessage> buffered_messages;
  size_t buffered_set_count = 0;
  size_t buffered_bytes = 0;
  size_t buffered_template_field_count = 0;
  size_t template_count = 0;
  size_t template_field_count = 0;
  V9FramingState v9_framing;
  uint64_t generation = 0;
  int64_t reference_unix_seconds
    = std::chrono::duration_cast<std::chrono::seconds>(
        time::clock::now().time_since_epoch())
        .count();
};

Decoder::Decoder() : state_{std::in_place} {
}

Decoder::Decoder(time reference_time) : state_{std::in_place} {
  state_->reference_unix_seconds
    = std::chrono::duration_cast<std::chrono::seconds>(
        reference_time.time_since_epoch())
        .count();
}

Decoder::Decoder(Decoder const&) = default;
Decoder::Decoder(Decoder&&) noexcept = default;
auto Decoder::operator=(Decoder const&) -> Decoder& = default;
auto Decoder::operator=(Decoder&&) noexcept -> Decoder& = default;
Decoder::~Decoder() = default;

auto Decoder::frame(std::span<const std::byte> bytes, bool end_of_input)
  -> FrameResult {
  return frame_message(
    bytes, end_of_input,
    [this](uint32_t domain_id, uint32_t sys_uptime,
           uint32_t export_time_seconds, uint32_t sequence_number,
           uint16_t template_id, FramingTemplate& fields) {
      auto const key = ExporterKey{
        .version = Version::v9,
        .implicit = true,
        .address = {},
        .port = 0,
        .domain_id = domain_id,
      };
      auto const exporter = state_->exporters.find(key);
      if (exporter == state_->exporters.end()) {
        return false;
      }
      auto reference_unix_seconds = state_->reference_unix_seconds;
      if (exporter->second.last_unfolded_export_time_seconds) {
        reference_unix_seconds
          = *exporter->second.last_unfolded_export_time_seconds;
      }
      auto const header = MessageHeader{
        .version = Version::v9,
        .sys_uptime = std::chrono::milliseconds{sys_uptime},
        .export_time_seconds = export_time_seconds,
        .unfolded_export_time_seconds
        = unfold_seconds(export_time_seconds, reference_unix_seconds),
        .sequence_number = sequence_number,
        .domain_id = domain_id,
      };
      if (message_order(&exporter->second, header) != MessageOrder::newer) {
        return false;
      }
      auto const definition = exporter->second.templates.find(template_id);
      if (definition == exporter->second.templates.end()) {
        return false;
      }
      fields.reserve(definition->second.fields.size());
      for (auto const& field : definition->second.fields) {
        fields.push_back(field.length);
      }
      return true;
    },
    state_->v9_framing);
}

auto Decoder::decode_message(std::span<const std::byte> bytes,
                             Option<Peer> peer, diagnostic_handler& dh)
  -> DecodeResult {
  state_->v9_framing = {};
  // Keep wall-clock cleanup independent from accepted-message accounting so
  // rejected traffic cannot retain buffered data past its time-to-live.
  state_->expire_buffered(dh);
  if (bytes.size() < 2) {
    return malformed("message is shorter than the version field");
  }
  auto const raw_version = read_u16_at(bytes, 0);
  if (not is_supported_version(raw_version)) {
    return DecodeResult{
      .records = {},
      .error = DecodeError{
        .kind = DecodeErrorKind::unsupported_version,
        .version = raw_version,
        .message = fmt::format("unsupported NetFlow version {}", raw_version),
      },
    };
  }
  auto header = MessageHeader{};
  if (auto error
      = parse_header(bytes, header, state_->reference_unix_seconds)) {
    auto result = malformed(std::move(*error));
    result.error->version = raw_version;
    return result;
  }
  if (header.version == Version::v5 and bytes.size() != header.length) {
    auto result = malformed(fmt::format("NetFlow v5 message length does not "
                                        "match record count: expected {} "
                                        "bytes, got {}",
                                        header.length, bytes.size()));
    result.error->version = raw_version;
    return result;
  }
  if (header.version == Version::ipfix and bytes.size() != header.length) {
    auto result = malformed(
      fmt::format("IPFIX message length is {}, but datagram contains {} bytes",
                  header.length, bytes.size()));
    result.error->version = raw_version;
    return result;
  }
  auto result
    = header.version == Version::v5
        ? state_->decode_v5(header, bytes, std::move(peer))
        : state_->decode_template_message(header, bytes, std::move(peer), dh);
  if (result.error) {
    result.error->version = raw_version;
  }
  return result;
}

auto Decoder::finish(diagnostic_handler& dh) -> void {
  state_->expire_buffered(dh, true);
}

auto Decoder::snapshot(Serde& serde) -> void {
  // Framing progress is a cache over the caller-owned stream buffer. Rebuild
  // it once after a checkpoint instead of duplicating buffered bytes and
  // message-local templates in the snapshot.
  state_->v9_framing = {};
  serde("netflow_exporters", state_->exporters);
  serde("netflow_template_count", state_->template_count);
  serde("netflow_template_field_count", state_->template_field_count);
  serde("netflow_buffered_messages", state_->buffered_messages);
  serde("netflow_buffered_set_count", state_->buffered_set_count);
  serde("netflow_buffered_bytes", state_->buffered_bytes);
  serde("netflow_buffered_template_field_count",
        state_->buffered_template_field_count);
  serde("netflow_generation", state_->generation);
  serde("netflow_reference_unix_seconds", state_->reference_unix_seconds);
}

} // namespace tenzir::netflow
