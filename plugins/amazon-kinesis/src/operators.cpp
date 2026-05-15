//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "operators.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/aws_credentials.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/concept/printable/tenzir/json2.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/array/array_binary.h>
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/DateTime.h>
#include <aws/kinesis/KinesisErrors.h>
#include <aws/kinesis/model/EncryptionType.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>
#include <aws/kinesis/model/ShardIteratorType.h>
#include <folly/ExceptionString.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <mutex>
#include <ranges>
#include <thread>
#include <utility>

using namespace std::chrono_literals;

namespace tenzir::plugins::amazon_kinesis {

namespace {

constexpr auto max_record_size = size_t{10 * 1024 * 1024};
constexpr auto max_put_records_request_size = max_record_size;
constexpr auto max_put_records_entries = size_t{500};
constexpr auto max_partition_key_size = size_t{256};
constexpr auto put_records_attempts = size_t{5};

struct ReadRecord {
  blob message;
  std::string stream;
  std::string shard_id;
  std::string sequence_number;
  std::string partition_key;
  Option<time> arrival_time;
  std::string encryption_type;
  int64_t millis_behind_latest = 0;
};

struct ReadResult {
  std::vector<ReadRecord> records;
  size_t shard_index = std::numeric_limits<size_t>::max();
  std::string next_iterator;
  std::string next_sequence_number;
  bool shard_closed = false;
  bool iterator_expired = false;
  bool throttled = false;
};

struct ShardInfo {
  std::string id;
  std::vector<std::string> parents;
};

struct PutRecordsResult {
  std::vector<ToAmazonKinesis::PendingRecord> failed_records;
  std::optional<std::string> error;
};

auto aws_string_view(const Aws::String& s) -> std::string_view {
  return {s.data(), s.size()};
}

auto aws_string(const Aws::String& s) -> std::string {
  return std::string{aws_string_view(s)};
}

auto endpoint_override(const Option<located<std::string>>& endpoint)
  -> std::string {
  if (endpoint) {
    return endpoint->inner;
  }
  if (auto service = detail::getenv("AWS_ENDPOINT_URL_KINESIS")) {
    return *service;
  }
  if (auto global = detail::getenv("AWS_ENDPOINT_URL")) {
    return *global;
  }
  return {};
}

auto date_time_to_time(const Aws::Utils::DateTime& date_time) -> time {
  return time{std::chrono::milliseconds{date_time.Millis()}};
}

auto time_to_date_time(time value) -> Aws::Utils::DateTime {
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    value.time_since_epoch());
  return Aws::Utils::DateTime{ms.count()};
}

auto encryption_type_to_string(Aws::Kinesis::Model::EncryptionType type)
  -> std::string {
  return aws_string(
    Aws::Kinesis::Model::EncryptionTypeMapper::GetNameForEncryptionType(type));
}

auto throw_if_error(const auto& outcome, std::string_view operation) -> void {
  if (outcome.IsSuccess()) {
    return;
  }
  const auto& error = outcome.GetError();
  throw std::runtime_error{
    fmt::format("Kinesis {} failed: {}", operation, error.GetMessage())};
}

auto ensure_aws_sdk_initialized() -> void {
  static auto options = Aws::SDKOptions{};
  static auto once = std::once_flag{};
  std::call_once(once, [] {
    Aws::InitAPI(options);
  });
}

auto configure_iterator_start(Aws::Kinesis::Model::GetShardIteratorRequest& req,
                              const Option<located<data>>& start,
                              const std::string& sequence_number) -> void {
  using Aws::Kinesis::Model::ShardIteratorType;
  if (not sequence_number.empty()) {
    req.SetShardIteratorType(ShardIteratorType::AFTER_SEQUENCE_NUMBER);
    req.SetStartingSequenceNumber(sequence_number);
    return;
  }
  if (not start) {
    req.SetShardIteratorType(ShardIteratorType::LATEST);
    return;
  }
  if (auto value = try_as<std::string>(start->inner)) {
    req.SetShardIteratorType(*value == "trim_horizon"
                               ? ShardIteratorType::TRIM_HORIZON
                               : ShardIteratorType::LATEST);
    return;
  }
  auto value = try_as<time>(start->inner);
  TENZIR_ASSERT(value);
  req.SetShardIteratorType(ShardIteratorType::AT_TIMESTAMP);
  req.SetTimestamp(time_to_date_time(*value));
}

auto starts_at_latest(const Option<located<data>>& start) -> bool {
  if (not start) {
    return true;
  }
  if (auto value = try_as<std::string>(start->inner)) {
    return *value != "trim_horizon";
  }
  return false;
}

auto make_shard_iterator(Aws::Kinesis::KinesisClient& client,
                         std::string_view stream, std::string_view shard_id,
                         const Option<located<data>>& start,
                         const std::string& sequence_number) -> std::string {
  auto request = Aws::Kinesis::Model::GetShardIteratorRequest{};
  request.SetStreamName(Aws::String{stream.data(), stream.size()});
  request.SetShardId(Aws::String{shard_id.data(), shard_id.size()});
  configure_iterator_start(request, start, sequence_number);
  auto outcome = client.GetShardIterator(request);
  throw_if_error(outcome, "GetShardIterator");
  return aws_string(outcome.GetResult().GetShardIterator());
}

auto make_shard_iterator(Aws::Kinesis::KinesisClient& client,
                         std::string_view stream, std::string_view shard_id,
                         time timestamp) -> std::string {
  auto request = Aws::Kinesis::Model::GetShardIteratorRequest{};
  request.SetStreamName(Aws::String{stream.data(), stream.size()});
  request.SetShardId(Aws::String{shard_id.data(), shard_id.size()});
  request.SetShardIteratorType(
    Aws::Kinesis::Model::ShardIteratorType::AT_TIMESTAMP);
  request.SetTimestamp(time_to_date_time(timestamp));
  auto outcome = client.GetShardIterator(request);
  throw_if_error(outcome, "GetShardIterator");
  return aws_string(outcome.GetResult().GetShardIterator());
}

auto make_shard_iterator(Aws::Kinesis::KinesisClient& client,
                         std::string_view stream, std::string_view shard_id,
                         Aws::Kinesis::Model::ShardIteratorType type)
  -> std::string {
  auto request = Aws::Kinesis::Model::GetShardIteratorRequest{};
  request.SetStreamName(Aws::String{stream.data(), stream.size()});
  request.SetShardId(Aws::String{shard_id.data(), shard_id.size()});
  request.SetShardIteratorType(type);
  auto outcome = client.GetShardIterator(request);
  throw_if_error(outcome, "GetShardIterator");
  return aws_string(outcome.GetResult().GetShardIterator());
}

auto list_shards(Aws::Kinesis::KinesisClient& client, std::string_view stream,
                 bool latest_only = false) -> std::vector<ShardInfo> {
  auto result = std::vector<ShardInfo>{};
  auto next = Aws::String{};
  do {
    auto request = Aws::Kinesis::Model::ListShardsRequest{};
    if (next.empty()) {
      request.SetStreamName(Aws::String{stream.data(), stream.size()});
      if (latest_only) {
        auto filter = Aws::Kinesis::Model::ShardFilter{};
        filter.SetType(Aws::Kinesis::Model::ShardFilterType::AT_LATEST);
        request.SetShardFilter(std::move(filter));
      }
    } else {
      request.SetNextToken(next);
    }
    auto outcome = client.ListShards(request);
    throw_if_error(outcome, "ListShards");
    const auto& page = outcome.GetResult();
    for (const auto& shard : page.GetShards()) {
      auto info = ShardInfo{.id = aws_string(shard.GetShardId())};
      if (shard.ParentShardIdHasBeenSet()) {
        info.parents.push_back(aws_string(shard.GetParentShardId()));
      }
      if (shard.AdjacentParentShardIdHasBeenSet()) {
        info.parents.push_back(aws_string(shard.GetAdjacentParentShardId()));
      }
      result.push_back(std::move(info));
    }
    next = page.GetNextToken();
  } while (not next.empty());
  return result;
}

auto read_from_shard(Aws::Kinesis::KinesisClient& client,
                     std::string_view stream, size_t shard_index,
                     std::string_view shard_id, std::string iterator, int limit)
  -> ReadResult {
  auto request = Aws::Kinesis::Model::GetRecordsRequest{};
  request.SetShardIterator(Aws::String{iterator.data(), iterator.size()});
  request.SetLimit(limit);
  auto outcome = client.GetRecords(request);
  if (not outcome.IsSuccess()) {
    const auto& error = outcome.GetError();
    if (error.GetErrorType() == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR) {
      return {.shard_index = shard_index, .iterator_expired = true};
    }
    if (error.GetErrorType()
        == Aws::Kinesis::KinesisErrors::PROVISIONED_THROUGHPUT_EXCEEDED) {
      return {.shard_index = shard_index, .throttled = true};
    }
    throw_if_error(outcome, "GetRecords");
  }
  const auto& result = outcome.GetResult();
  auto output = ReadResult{};
  output.shard_index = shard_index;
  output.next_iterator = aws_string(result.GetNextShardIterator());
  output.shard_closed = output.next_iterator.empty();
  output.records.reserve(result.GetRecords().size());
  for (const auto& record : result.GetRecords()) {
    const auto& bytes = record.GetData();
    auto item = ReadRecord{};
    item.message = blob{
      std::span{reinterpret_cast<const std::byte*>(bytes.GetUnderlyingData()),
                bytes.GetLength()}};
    item.stream = std::string{stream};
    item.shard_id = std::string{shard_id};
    item.sequence_number = aws_string(record.GetSequenceNumber());
    item.partition_key = aws_string(record.GetPartitionKey());
    if (record.ApproximateArrivalTimestampHasBeenSet()) {
      item.arrival_time
        = date_time_to_time(record.GetApproximateArrivalTimestamp());
    }
    if (record.EncryptionTypeHasBeenSet()) {
      item.encryption_type
        = encryption_type_to_string(record.GetEncryptionType());
    }
    item.millis_behind_latest = result.GetMillisBehindLatest();
    output.next_sequence_number = item.sequence_number;
    output.records.push_back(std::move(item));
  }
  return output;
}

auto validate_partition_key(std::string_view key, const location& loc,
                            diagnostic_handler& dh) -> bool {
  auto characters = size_t{0};
  for (auto byte : key) {
    if ((static_cast<unsigned char>(byte) & 0b1100'0000) != 0b1000'0000) {
      ++characters;
    }
  }
  if (key.empty()) {
    diagnostic::warning("partition key must not be empty")
      .primary(loc)
      .note("event is skipped")
      .emit(dh);
    return false;
  }
  if (characters > max_partition_key_size) {
    diagnostic::warning("partition key must be at most {} characters",
                        max_partition_key_size)
      .primary(loc)
      .note("event is skipped")
      .emit(dh);
    return false;
  }
  return true;
}

auto record_size(const ToAmazonKinesis::PendingRecord& record) -> size_t {
  return record.message.size() + record.partition_key.size();
}

auto append_messages(std::vector<Option<blob>>& out,
                     const ast::expression& expr, table_slice input,
                     diagnostic_handler& dh) -> void {
  for (const auto& messages : eval(expr, input, dh)) {
    const auto append = [&](const auto& array) {
      for (auto i = int64_t{0}; i < array.length(); ++i) {
        if (array.IsNull(i)) {
          diagnostic::warning("expected `string` or `blob`, got `null`")
            .primary(expr)
            .note("event is skipped")
            .emit(dh);
          out.emplace_back(None{});
          continue;
        }
        auto bytes = as_bytes(array.Value(i));
        out.emplace_back(blob{bytes});
      }
    };
    if (auto strings = messages.template as<string_type>()) {
      append(*strings->array);
      continue;
    }
    if (auto blobs = messages.template as<blob_type>()) {
      append(*blobs->array);
      continue;
    }
    diagnostic::warning("expected `string` or `blob`, got `{}`",
                        messages.type.kind())
      .primary(expr)
      .note("events are skipped")
      .emit(dh);
    out.resize(out.size() + messages.length());
  }
}

auto append_partition_keys(std::vector<std::string>& out,
                           const ast::expression& expr, table_slice input,
                           diagnostic_handler& dh) -> void {
  for (const auto& keys : eval(expr, input, dh)) {
    if (auto strings = keys.template as<string_type>()) {
      const auto& array = *strings->array;
      for (auto i = int64_t{0}; i < array.length(); ++i) {
        if (array.IsNull(i)) {
          diagnostic::warning("expected `string`, got `null`")
            .primary(expr)
            .note("event is skipped")
            .emit(dh);
          out.emplace_back();
          continue;
        }
        auto value = array.GetString(i);
        out.emplace_back(value.data(), value.size());
      }
      continue;
    }
    diagnostic::warning("expected `string`, got `{}`", keys.type.kind())
      .primary(expr)
      .note("events are skipped")
      .emit(dh);
    out.resize(out.size() + keys.length());
  }
}

auto put_records(Aws::Kinesis::KinesisClient& client, std::string_view stream,
                 std::vector<ToAmazonKinesis::PendingRecord> records)
  -> PutRecordsResult {
  for (auto attempt = size_t{0}; attempt < put_records_attempts; ++attempt) {
    auto request = Aws::Kinesis::Model::PutRecordsRequest{};
    request.SetStreamName(Aws::String{stream.data(), stream.size()});
    for (const auto& record : records) {
      auto entry = Aws::Kinesis::Model::PutRecordsRequestEntry{};
      entry.SetPartitionKey(record.partition_key);
      entry.SetData(Aws::Utils::ByteBuffer{
        const_cast<unsigned char*>(
          reinterpret_cast<const unsigned char*>(record.message.data())),
        record.message.size()});
      request.AddRecords(std::move(entry));
    }
    auto outcome = client.PutRecords(request);
    if (not outcome.IsSuccess()) {
      if (attempt + 1 == put_records_attempts) {
        return {.failed_records = std::move(records),
                .error = outcome.GetError().GetMessage()};
      }
      std::this_thread::sleep_for(100ms * (1 << attempt));
      continue;
    }
    const auto& result = outcome.GetResult();
    if (result.GetFailedRecordCount() == 0) {
      return {};
    }
    auto retry = std::vector<ToAmazonKinesis::PendingRecord>{};
    const auto& statuses = result.GetRecords();
    for (auto i = size_t{0}; i < records.size() and i < statuses.size(); ++i) {
      if (statuses[i].ErrorCodeHasBeenSet()) {
        retry.push_back(std::move(records[i]));
      }
    }
    records = std::move(retry);
    if (records.empty()) {
      return {};
    }
    std::this_thread::sleep_for(100ms * (1 << attempt));
  }
  return {.failed_records = std::move(records),
          .error = "retry attempts exhausted"};
}

} // namespace

auto default_message_expression() -> ast::expression {
  auto function
    = ast::entity{{ast::identifier{"print_ndjson", location::unknown}}};
  function.ref
    = entity_path{std::string{entity_pkg_std}, {"print_ndjson"}, entity_ns::fn};
  return ast::function_call{
    std::move(function),
    {ast::this_{location::unknown}},
    location::unknown,
    true,
  };
}

auto make_kinesis_client(const Option<located<std::string>>& aws_region,
                         const Option<located<record>>& aws_iam,
                         const Option<located<std::string>>& endpoint,
                         OpCtx& ctx)
  -> Task<std::shared_ptr<Aws::Kinesis::KinesisClient>> {
  ensure_aws_sdk_initialized();
  auto auth = co_await resolve_aws_iam_auth(
    aws_iam ? std::optional<located<record>>{*aws_iam} : std::nullopt,
    aws_region ? std::optional<located<std::string>>{*aws_region}
               : std::nullopt,
    ctx);
  if (not auth) {
    co_return nullptr;
  }
  auto region = std::string{};
  if (aws_region) {
    region = aws_region->inner;
  } else if (auth->credentials and not auth->credentials->region.empty()) {
    region = auth->credentials->region;
  } else {
    region = Aws::Client::ClientConfiguration{}.region;
  }
  auto config = Aws::Client::ClientConfiguration{};
  config.region = region;
  if (auto override = endpoint_override(endpoint); not override.empty()) {
    config.endpointOverride = override;
  }
  auto region_opt
    = region.empty() ? std::optional<std::string>{} : std::optional{region};
  auto provider = make_aws_credentials_provider(auth->credentials, region_opt);
  if (not provider) {
    diagnostic::error(provider.error()).emit(ctx);
    co_return nullptr;
  }
  co_return std::make_shared<Aws::Kinesis::KinesisClient>(*provider, config);
}

FromAmazonKinesis::FromAmazonKinesis(FromAmazonKinesisArgs args)
  : args_{std::move(args)} {
}

auto FromAmazonKinesis::start(OpCtx& ctx) -> Task<void> {
  client_ = co_await make_kinesis_client(args_.aws_region, args_.aws_iam,
                                         args_.endpoint, ctx);
  if (not client_) {
    done_ = true;
    co_return;
  }
  limit_ = args_.count ? args_.count->inner : 0;
  records_per_call_ = args_.records_per_call
                        ? detail::narrow<int>(args_.records_per_call->inner)
                        : 1000;
  poll_idle_ = args_.poll_idle ? args_.poll_idle->inner : 1s;
  if (shards_.empty()) {
    auto shard_infos
      = co_await spawn_blocking([client = client_, stream = args_.stream.inner,
                                 latest_only = starts_at_latest(args_.start)] {
          return list_shards(*client, stream, latest_only);
        });
    shards_.reserve(shard_infos.size());
    for (auto& info : shard_infos) {
      shards_.push_back(ShardState{.id = std::move(info.id),
                                   .parents = std::move(info.parents)});
    }
  }
  for (auto& shard : shards_) {
    if (shard.closed) {
      continue;
    }
    if (starts_at_latest(args_.start) and not shard.latest_start_time) {
      shard.latest_start_time = time::clock::now();
    }
    auto latest_start_time = shard.latest_start_time;
    shard.iterator = co_await spawn_blocking(
      [client = client_, stream = args_.stream.inner, id = shard.id,
       start = args_.start, sequence = shard.next_sequence_number,
       latest_start_time] {
        if (sequence.empty() and latest_start_time) {
          return make_shard_iterator(*client, stream, id, *latest_start_time);
        }
        return make_shard_iterator(*client, stream, id, start, sequence);
      });
  }
  bytes_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_kinesis"},
                       MetricsDirection::read, MetricsVisibility::external_);
}

auto FromAmazonKinesis::discover_new_shards(OpCtx& ctx) -> Task<void> {
  auto shard_infos
    = co_await spawn_blocking([client = client_, stream = args_.stream.inner,
                               latest_only = starts_at_latest(args_.start)] {
        return list_shards(*client, stream, latest_only);
      });
  for (auto& info : shard_infos) {
    const auto known
      = std::ranges::any_of(shards_, [&](const ShardState& shard) {
          return shard.id == info.id;
        });
    if (known) {
      continue;
    }
    auto iterator = co_await spawn_blocking(
      [client = client_, stream = args_.stream.inner, id = info.id] {
        return make_shard_iterator(
          *client, stream, id,
          Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
      });
    shards_.push_back(ShardState{.id = std::move(info.id),
                                 .parents = std::move(info.parents),
                                 .iterator = std::move(iterator)});
  }
  if (next_shard_ >= shards_.size()) {
    next_shard_ = 0;
  }
}

auto FromAmazonKinesis::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  if (done_ or shards_.empty()) {
    co_await folly::coro::sleep(std::chrono::milliseconds{10});
    co_return ReadResult{};
  }
  auto index = std::optional<size_t>{};
  const auto parents_closed = [&](const ShardState& shard) {
    return std::ranges::all_of(shard.parents, [&](const std::string& parent) {
      auto it = std::ranges::find(shards_, parent, &ShardState::id);
      return it == shards_.end() or it->closed;
    });
  };
  auto any_eligible = false;
  auto all_eligible_idle = true;
  for (const auto& shard : shards_) {
    const auto eligible = not shard.closed and not shard.iterator.empty()
                          and parents_closed(shard);
    any_eligible = any_eligible or eligible;
    all_eligible_idle = all_eligible_idle and (not eligible or shard.idle);
  }
  if (any_eligible and all_eligible_idle) {
    co_await folly::coro::sleep(
      std::chrono::duration_cast<folly::HighResDuration>(poll_idle_));
  }
  for (auto i = size_t{0}; i < shards_.size(); ++i) {
    auto candidate = (next_shard_ + i) % shards_.size();
    if (not shards_[candidate].closed
        and not shards_[candidate].iterator.empty()
        and parents_closed(shards_[candidate])) {
      index = candidate;
      break;
    }
  }
  if (not index) {
    co_await folly::coro::sleep(std::chrono::milliseconds{10});
    co_return ReadResult{};
  }
  const auto& shard = shards_[*index];
  auto result = co_await spawn_blocking(
    [client = client_, stream = args_.stream.inner, index = *index,
     id = shard.id, iterator = shard.iterator, limit = records_per_call_] {
      return read_from_shard(*client, stream, index, id, iterator, limit);
    });
  if (result.throttled) {
    co_await folly::coro::sleep(
      std::chrono::duration_cast<folly::HighResDuration>(poll_idle_));
  }
  co_return std::move(result);
}

auto FromAmazonKinesis::process_task(Any result, Push<table_slice>& push,
                                     OpCtx& ctx) -> Task<void> {
  auto batch = std::move(result).as<ReadResult>();
  if (batch.shard_index < shards_.size()) {
    auto& shard = shards_[batch.shard_index];
    if (batch.throttled) {
      shard.idle = false;
    } else if (batch.iterator_expired) {
      auto latest_start_time = shard.latest_start_time;
      shard.iterator = co_await spawn_blocking(
        [client = client_, stream = args_.stream.inner, id = shard.id,
         start = args_.start, sequence = shard.next_sequence_number,
         latest_start_time] {
          if (sequence.empty() and latest_start_time) {
            return make_shard_iterator(*client, stream, id, *latest_start_time);
          }
          return make_shard_iterator(*client, stream, id, start, sequence);
        });
      shard.closed = false;
      shard.idle = false;
    } else {
      shard.iterator = std::move(batch.next_iterator);
      shard.closed = batch.shard_closed;
      shard.idle = batch.records.empty();
      if (not batch.next_sequence_number.empty()) {
        shard.next_sequence_number = batch.next_sequence_number;
      }
    }
    next_shard_ = (batch.shard_index + 1) % shards_.size();
    if (batch.shard_closed) {
      co_await discover_new_shards(ctx);
    }
  }
  if (batch.records.empty()) {
    if (args_.exit) {
      done_ = std::ranges::all_of(shards_, [](const ShardState& shard) {
        return shard.closed or shard.idle;
      });
    }
    co_return;
  }
  auto opts = multi_series_builder::options{};
  opts.settings.ordered = true;
  opts.settings.raw = true;
  opts.settings.default_schema_name = "tenzir.amazon_kinesis";
  auto msb = multi_series_builder{std::move(opts), ctx.dh()};
  for (const auto& record : batch.records) {
    if (limit_ != 0 and emitted_ >= limit_) {
      done_ = true;
      break;
    }
    bytes_read_counter_.add(record.message.size());
    auto event = msb.record();
    event.field("message").data(record.message);
    event.field("stream").data(record.stream);
    event.field("shard_id").data(record.shard_id);
    event.field("sequence_number").data(record.sequence_number);
    event.field("partition_key").data(record.partition_key);
    if (record.arrival_time) {
      event.field("arrival_time").data(*record.arrival_time);
    }
    if (not record.encryption_type.empty()) {
      event.field("encryption_type").data(record.encryption_type);
    }
    event.field("millis_behind_latest").data(record.millis_behind_latest);
    ++emitted_;
  }
  for (auto&& slice : msb.finalize_as_table_slice()) {
    co_await push(std::move(slice));
  }
  if (limit_ != 0 and emitted_ >= limit_) {
    done_ = true;
  }
}

auto FromAmazonKinesis::state() -> OperatorState {
  return done_ ? OperatorState::done : OperatorState::normal;
}

auto FromAmazonKinesis::snapshot(Serde& serde) -> void {
  serde("shards", shards_);
  serde("next_shard", next_shard_);
  serde("emitted", emitted_);
  serde("done", done_);
}

ToAmazonKinesis::ToAmazonKinesis(ToAmazonKinesisArgs args)
  : args_{std::move(args)} {
}

auto ToAmazonKinesis::start(OpCtx& ctx) -> Task<void> {
  client_ = co_await make_kinesis_client(args_.aws_region, args_.aws_iam,
                                         args_.endpoint, ctx);
  if (not client_) {
    throw std::runtime_error{"failed to initialize Kinesis client"};
  }
  batch_size_
    = args_.batch_size ? detail::narrow<size_t>(args_.batch_size->inner) : 500;
  batch_timeout_ = args_.batch_timeout ? args_.batch_timeout->inner : 1s;
  parallel_ = args_.parallel ? args_.parallel->inner : 1;
  bytes_write_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_amazon_kinesis"},
                       MetricsDirection::write, MetricsVisibility::external_);
}

auto ToAmazonKinesis::process(table_slice input, OpCtx& ctx) -> Task<void> {
  if (input.rows() == 0) {
    co_return;
  }
  auto messages = std::vector<Option<blob>>{};
  append_messages(messages, args_.message, input, ctx.dh());
  auto partition_keys = std::vector<std::string>{};
  if (args_.partition_key) {
    append_partition_keys(partition_keys, *args_.partition_key, input,
                          ctx.dh());
  }
  for (auto i = size_t{0}; i < messages.size(); ++i) {
    auto& message = messages[i];
    if (not message) {
      continue;
    }
    auto key = std::string{};
    if (args_.partition_key) {
      if (i >= partition_keys.size()) {
        continue;
      }
      key = std::move(partition_keys[i]);
    } else {
      key = fmt::to_string(uuid::random());
    }
    const auto key_loc = args_.operator_location;
    if (not validate_partition_key(key, key_loc, ctx.dh())) {
      continue;
    }
    if (message->size() + key.size() > max_record_size) {
      diagnostic::warning("Kinesis record payload and partition key must be at "
                          "most {} bytes",
                          max_record_size)
        .primary(args_.message)
        .note("event is skipped")
        .emit(ctx);
      continue;
    }
    bytes_write_counter_.add(message->size());
    const auto was_empty = batch_.empty();
    const auto now = std::chrono::steady_clock::now();
    batch_.push_back(PendingRecord{std::move(*message), std::move(key)});
    if (was_empty) {
      batch_started_ = now;
    }
    if (batch_.size() >= batch_size_) {
      co_await flush(ctx);
    } else {
      co_await flush_if_timed_out(ctx);
    }
  }
}

auto ToAmazonKinesis::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  co_await folly::coro::sleep(
    std::chrono::ceil<folly::HighResDuration>(batch_timeout_));
  co_return true;
}

auto ToAmazonKinesis::process_task(Any result, OpCtx& ctx) -> Task<void> {
  TENZIR_UNUSED(result);
  co_await flush_if_timed_out(ctx);
}

auto ToAmazonKinesis::flush_if_timed_out(OpCtx& ctx) -> Task<void> {
  if (batch_.empty()) {
    batch_started_ = None{};
    co_return;
  }
  TENZIR_ASSERT(batch_started_);
  if (std::chrono::steady_clock::now() - *batch_started_ >= batch_timeout_) {
    co_await flush(ctx);
  }
}

auto ToAmazonKinesis::flush(OpCtx& ctx) -> Task<void> {
  if (batch_.empty() or not client_) {
    batch_started_ = None{};
    co_return;
  }
  auto records = std::exchange(batch_, {});
  batch_started_ = None{};
  auto chunks = std::vector<std::vector<PendingRecord>>{};
  auto chunk = std::vector<PendingRecord>{};
  auto chunk_size = size_t{0};
  for (auto& record : records) {
    const auto size = record_size(record);
    if ((not chunk.empty() and chunk_size + size > max_put_records_request_size)
        or chunk.size() >= max_put_records_entries) {
      chunks.push_back(std::move(chunk));
      chunk_size = 0;
    }
    chunk_size += size;
    chunk.push_back(std::move(record));
  }
  if (not chunk.empty()) {
    chunks.push_back(std::move(chunk));
  }
  auto errors = std::vector<std::string>{};
  for (auto offset = size_t{0}; offset < chunks.size(); offset += parallel_) {
    auto tasks = std::vector<Task<PutRecordsResult>>{};
    const auto end = std::min<size_t>(chunks.size(), offset + parallel_);
    tasks.reserve(end - offset);
    for (auto index = offset; index < end; ++index) {
      tasks.push_back(
        spawn_blocking([client = client_, stream = args_.stream.inner,
                        records = std::move(chunks[index])]() mutable {
          return put_records(*client, stream, std::move(records));
        }));
    }
    auto results = co_await folly::coro::collectAllRange(std::move(tasks));
    for (auto& result : results) {
      if (result.error) {
        errors.push_back(std::move(*result.error));
      }
      std::ranges::move(result.failed_records, std::back_inserter(batch_));
    }
  }
  if (not batch_.empty()) {
    batch_started_ = std::chrono::steady_clock::now();
  }
  if (not errors.empty()) {
    diagnostic::error("failed to write records to Kinesis")
      .primary(args_.stream.source)
      .note("{}", fmt::join(errors, "; "))
      .emit(ctx);
  }
}

auto ToAmazonKinesis::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  co_await flush(ctx);
  if (not batch_.empty()) {
    diagnostic::error("failed to write all records to Kinesis")
      .primary(args_.stream.source)
      .note("{} records remain unsent", batch_.size())
      .emit(ctx);
    throw std::runtime_error{"failed to write all records to Kinesis"};
  }
  co_return FinalizeBehavior::done;
}

} // namespace tenzir::plugins::amazon_kinesis
