//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/aggregate_suricata_flow.hpp"

#include "vast/fwd.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <iterator>
#include <string_view>

using namespace std::string_literals;

namespace vast {
struct aggregate_suricata_flow_field {
  std::string input_key;
  std::string_view output_name;
};

struct aggregate_suricata_flow_fields {
  aggregate_suricata_flow_field timestamp;
  aggregate_suricata_flow_field count;
  aggregate_suricata_flow_field pcap_cnt;
  aggregate_suricata_flow_field src_ip;
  aggregate_suricata_flow_field dest_ip;
  aggregate_suricata_flow_field dest_port;
  aggregate_suricata_flow_field proto;
  aggregate_suricata_flow_field event_type;
  aggregate_suricata_flow_field pkts_toserver_sum;
  aggregate_suricata_flow_field pkts_toclient_sum;
  aggregate_suricata_flow_field bytes_toserver_sum;
  aggregate_suricata_flow_field bytes_toclient_sum;
  aggregate_suricata_flow_field start_min;
  aggregate_suricata_flow_field end_max;
  aggregate_suricata_flow_field contains_alerted;
};

static const aggregate_suricata_flow_fields field{
  {"timestamp"s, "timestamp"},
  {""s, "count"},
  {"pcap_cnt"s, "pcap_cnt"},
  {"src_ip"s, "src_ip"},
  {"dest_ip"s, "dest_ip"},
  {"dest_port"s, "dest_port"},
  {"proto"s, "proto"},
  {"event_type"s, "event_type"},
  {"flow.pkts_toserver"s, "pkts_toserver_sum"},
  {"flow.pkts_toclient"s, "pkts_toclient_sum"},
  {"flow.bytes_toserver"s, "bytes_toserver_sum"},
  {"flow.bytes_toclient"s, "bytes_toclient_sum"},
  {"flow.start"s, "start_min"},
  {"flow.end"s, "end_max"},
  {"flow.alerted"s, "contains_alerted"},
};

static constexpr std::string_view layout_name{"suricata.aggregated_flow"};

static const auto aggregated_layout = vast::type{
  layout_name,
  vast::record_type{
    {field.timestamp.output_name, vast::time_type{}},
    {field.count.output_name, vast::count_type{}},
    {field.pcap_cnt.output_name, vast::count_type{}},
    {field.src_ip.output_name, vast::address_type{}},
    {field.dest_ip.output_name, vast::address_type{}},
    {field.dest_port.output_name, vast::count_type{}},
    {field.proto.output_name, vast::string_type{}},
    {field.event_type.output_name, vast::string_type{}},
    {"aggregated_flow",
     vast::record_type{
       {field.pkts_toserver_sum.output_name, vast::count_type{}},
       {field.pkts_toclient_sum.output_name, vast::count_type{}},
       {field.bytes_toserver_sum.output_name, vast::count_type{}},
       {field.bytes_toclient_sum.output_name, vast::count_type{}},
       {field.start_min.output_name, vast::time_type{}},
       {field.end_max.output_name, vast::time_type{}},
       {field.contains_alerted.output_name, vast::bool_type{}},
     }},
  },
};

struct aggregate_suricata_flow_key {
  long long timestamp_group;
  vast::address src_ip;
  vast::address dest_ip;
  vast::count dest_port;
  std::string proto;
  bool operator==(const aggregate_suricata_flow_key& other) const {
    return timestamp_group == other.timestamp_group && src_ip == other.src_ip
           && dest_ip == other.dest_ip && dest_port == other.dest_port
           && proto == other.proto;
  }
};

class aggregate_suricata_flow_key_hash {
public:
  // FIXME: Is this a good hash function?
  size_t operator()(const aggregate_suricata_flow_key& k) const {
    return vast::hash(k.timestamp_group, k.src_ip, k.dest_ip, k.dest_port,
                      k.proto);
  }
};

struct aggregate_suricata_flow_value {
  vast::count pcap_cnt = 0;
  vast::count pkts_toserver_sum = 0;
  vast::count pkts_toclient_sum = 0;
  vast::count bytes_toserver_sum = 0;
  vast::count bytes_toclient_sum = 0;
  vast::time start_min = vast::time::max();
  vast::time end_max = vast::time::min();
  bool contains_alerted = false;
  vast::count count = 0;
};

caf::error
aggregate_suricata_flow_step::add(type layout,
                                  std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_TRACE("aggregate suricate flow step adds batch");
  to_transform_.emplace_back(std::move(layout), std::move(batch));
  return caf::none;
}

caf::expected<size_t>
get_index(const record_type& record, const std::string_view& name) {
  auto timestamp_offsets = record.resolve_key_suffix(field.timestamp.input_key);
  auto first = timestamp_offsets.begin();
  if (first == timestamp_offsets.end())
    return caf::make_error(
      ec::parse_error,
      fmt::format("aggregate suricate flow failed to find field: {}", name));
  size_t index = record.flat_index(*first);
  if (timestamp_offsets.end() != ++first)
    return caf::make_error(ec::parse_error, fmt::format("aggregate suricate "
                                                        "flow found an "
                                                        "ambiguous field: {}",
                                                        name));
  return index;
}

caf::error validate_timestamp(const std::shared_ptr<arrow::Field>& field) {
  if (field->type()->id() != arrow::Type::TIMESTAMP) {
    return caf::make_error(
      ec::parse_error, fmt::format("aggregate suricate flow found an "
                                   "unexpected type for the timestamp field"));
  }
  auto timestamp_type
    = static_pointer_cast<arrow::TimestampType>(field->type());
  if (timestamp_type->unit() != arrow::TimeUnit::NANO) {
    return caf::make_error(ec::parse_error,
                           fmt::format("aggregate suricate flow supports only "
                                       "the nanoseconds time unit"));
  }
  if (!timestamp_type->timezone().empty()) {
    return caf::make_error(ec::parse_error, fmt::format("aggregate suricate "
                                                        "flow does not support "
                                                        "timezone"));
  }
  return caf::none;
}

caf::error validate_schema(const std::shared_ptr<arrow::Schema>& schema) {
  for (const auto& field :
       std::array{field.timestamp, field.start_min, field.end_max}) {
    auto timestamp = schema->GetFieldByName(std::string{field.input_key});
    if (auto err = validate_timestamp(timestamp))
      return err;
  }
  return caf::none;
}

caf::expected<std::vector<transform_batch>>
aggregate_suricata_flow_step::finish() {
  VAST_DEBUG("aggregate suricate flow step finishes transformation");
  // FIXME: Should the result be sorted?
  std::unordered_map<aggregate_suricata_flow_key, aggregate_suricata_flow_value,
                     aggregate_suricata_flow_key_hash>
    groups{};
  auto timestamp_origin = long{0};
  for (const auto& [layout, batch] : to_transform_) {
    if (auto err = validate_schema(batch->schema()))
      return err;
    int64_t rows = batch->num_rows();
    auto timestamp_column = batch->GetColumnByName(field.timestamp.input_key);
    auto timestamps
      = std::static_pointer_cast<arrow::Int64Array>(timestamp_column);
    auto src_ip_column = batch->GetColumnByName(field.src_ip.input_key);
    auto src_ips
      = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(src_ip_column);
    auto dest_ip_column = batch->GetColumnByName(field.dest_ip.input_key);
    auto dest_ips
      = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(dest_ip_column);
    auto dest_port_column = batch->GetColumnByName(field.dest_port.input_key);
    auto dest_ports
      = std::static_pointer_cast<arrow::UInt64Array>(dest_port_column);
    auto proto_column = batch->GetColumnByName(field.proto.input_key);
    auto protos = std::static_pointer_cast<arrow::StringArray>(proto_column);
    auto pcap_cnt_column = batch->GetColumnByName(field.pcap_cnt.input_key);
    auto pcap_cnts
      = std::static_pointer_cast<arrow::UInt64Array>(pcap_cnt_column);
    auto pkts_toserver_sum_column
      = batch->GetColumnByName(field.pkts_toserver_sum.input_key);
    auto pkts_toserver_sums
      = std::static_pointer_cast<arrow::UInt64Array>(pkts_toserver_sum_column);
    auto pkts_toclient_sum_column
      = batch->GetColumnByName(field.pkts_toclient_sum.input_key);
    auto pkts_toclient_sums
      = std::static_pointer_cast<arrow::UInt64Array>(pkts_toclient_sum_column);
    auto bytes_toserver_sum_column
      = batch->GetColumnByName(field.bytes_toserver_sum.input_key);
    auto bytes_toserver_sums
      = std::static_pointer_cast<arrow::UInt64Array>(bytes_toserver_sum_column);
    auto bytes_toclient_sum_column
      = batch->GetColumnByName(field.bytes_toclient_sum.input_key);
    auto bytes_toclient_sums
      = std::static_pointer_cast<arrow::UInt64Array>(bytes_toclient_sum_column);
    auto start_min_column = batch->GetColumnByName(field.start_min.input_key);
    auto start_mins
      = std::static_pointer_cast<arrow::Int64Array>(start_min_column);
    auto end_max_column = batch->GetColumnByName(field.end_max.input_key);
    auto end_maxs = std::static_pointer_cast<arrow::Int64Array>(end_max_column);
    auto contains_alerted_column
      = batch->GetColumnByName(field.contains_alerted.input_key);
    auto contains_alerteds
      = std::static_pointer_cast<arrow::BooleanArray>(contains_alerted_column);
    for (int i = 0; i < rows; i++) {
      auto timestamp = timestamps->Value(i);
      auto timespan = std::chrono::nanoseconds(timestamp - timestamp_origin);
      auto timestamp_group = timespan / bucket_size_;
      const auto* src_ip_chars = src_ips->Value(i);
      const auto* src_ip_bytes
        = reinterpret_cast<const std::byte*>(src_ip_chars);
      const auto src_ip
        = address{std::span<const std::byte, 16>(src_ip_bytes, 16)};
      const auto* dest_ip_chars = dest_ips->Value(i);
      const auto* dest_ip_bytes
        = reinterpret_cast<const std::byte*>(dest_ip_chars);
      const auto dest_ip
        = address{std::span<const std::byte, 16>(dest_ip_bytes, 16)};
      const auto dest_port = vast::count{dest_ports->Value(i)};
      auto proto = protos->Value(i);
      aggregate_suricata_flow_key key{timestamp_group, src_ip, dest_ip,
                                      dest_port, proto.to_string()};
      auto& value = groups[key];
      value.count++;
      value.pcap_cnt += vast::count{pcap_cnts->Value(i)};
      value.pkts_toserver_sum += vast::count{pkts_toserver_sums->Value(i)};
      value.pkts_toclient_sum += vast::count{pkts_toclient_sums->Value(i)};
      value.bytes_toserver_sum += vast::count{bytes_toserver_sums->Value(i)};
      value.bytes_toclient_sum += vast::count{bytes_toclient_sums->Value(i)};
      auto start_min_candidate
        = vast::time{std::chrono::nanoseconds(start_mins->Value(i))};
      if (start_min_candidate < value.start_min)
        value.start_min = start_min_candidate;
      auto end_max_candidate
        = vast::time{std::chrono::nanoseconds(end_maxs->Value(i))};
      if (end_max_candidate > value.end_max)
        value.end_max = end_max_candidate;
      if (contains_alerteds->Value(i))
        value.contains_alerted = true;
    }
  }
  // FIXME: maximum slice_size?
  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::table_slice_encoding::arrow, aggregated_layout);
  if (builder == nullptr)
    return caf::make_error(ec::invalid_result,
                           "aggregate_suricata_flow_step failed to get a "
                           "table slice builder");
  for (const auto& [key, value] : groups) {
    if (!builder->add(vast::time{std::chrono::nanoseconds(key.timestamp_group
                                                          * bucket_size_)
                                 + std::chrono::nanoseconds(timestamp_origin)},
                      value.count, value.pcap_cnt, key.src_ip, key.dest_ip,
                      key.dest_port, key.proto, std::string{layout_name},
                      value.pkts_toserver_sum, value.pkts_toclient_sum,
                      value.bytes_toserver_sum, value.bytes_toclient_sum,
                      value.start_min, value.end_max, value.contains_alerted))
      return caf::make_error(ec::invalid_result,
                             "aggregate_suricata_flow_step failed to add row "
                             "to the result");
  }
  auto result = to_record_batch(builder->finish());
  auto transformed = std::vector<transform_batch>{};
  transformed.emplace_back(aggregated_layout, result);
  return transformed;
}

class aggregate_suricata_flow_step_plugin final
  : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "aggregate_suricata_flow";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const caf::settings& opts) const override {
    auto bucket_size_string = caf::get_if<std::string>(&opts, "bucket-size");
    if (!bucket_size_string)
      return caf::make_error(ec::invalid_configuration,
                             "key 'bucket-size' is missing or not a string in "
                             "configuration for aggregate suricata flow step");
    if (auto bucket_size = to<vast::duration>(*bucket_size_string))
      return std::make_unique<aggregate_suricata_flow_step>(*bucket_size);
    else
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("aggregate suricate flow step plugin was unable "
                    "parse the "
                    "bucket-size option {} as duration: {}",
                    bucket_size, bucket_size.error()));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::aggregate_suricata_flow_step_plugin)
