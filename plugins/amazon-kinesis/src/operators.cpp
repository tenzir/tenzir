//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "operators.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/array/array_binary.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/DateTime.h>
#include <aws/kinesis/model/EncryptionType.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/ListShardsResult.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>
#include <aws/kinesis/model/PutRecordsResult.h>
#include <aws/kinesis/model/ShardIteratorType.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <ranges>
#include <utility>

using namespace std::chrono_literals;

namespace tenzir::plugins::amazon_kinesis {

namespace {

constexpr auto max_record_size = size_t{10 * 1024 * 1024};
constexpr auto max_put_records_request_size = max_record_size;
constexpr auto max_put_records_entries = size_t{500};
constexpr auto max_partition_key_size = size_t{256};
constexpr auto put_records_attempts = size_t{5};
constexpr auto throttle_backoff = 1s;

struct ReadRecord {
  blob message;
  std::string stream;
  std::string shard_id;
  std::string sequence_number;
  std::string partition_key;
  Option<time> arrival_time;
  std::string encryption_type;
  duration behind_latest = {};
};

struct ReadResult {
  std::vector<ReadRecord> records;
  size_t shard_index = std::numeric_limits<size_t>::max();
  std::string next_iterator;
  std::string next_sequence_number;
  bool shard_closed = false;
  bool iterator_expired = false;
  bool throttled = false;
  std::string error;
};

struct ShardInfo {
  std::string id;
  std::vector<std::string> parents;
};

struct PutRecordsResult {
  std::vector<ToAmazonKinesis::PendingRecord> failed_records;
  std::optional<std::string> error;
};

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
  return amazon::from_aws_string(
    Aws::Kinesis::Model::EncryptionTypeMapper::GetNameForEncryptionType(type));
}

template <class Request>
auto kinesis_api_call(amazon::SignedHttpClient& client,
                      std::string_view operation, Request& request)
  -> Task<Result<Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>,
                 KinesisApiError>> {
  auto response = co_await client.raw_api_call(operation, request);
  if (response.is_err()) {
    co_return Err{KinesisApiError{.message = std::move(response).unwrap_err()}};
  }
  auto http_response = std::move(response).unwrap();
  if (not http_response.is_status_success()) {
    co_return Err{KinesisApiError{
      .message = fmt::format(
        "{} returned HTTP {}: {}", operation, http_response.status_code,
        amazon::extract_aws_error_message(http_response.body)),
      .code = amazon::extract_aws_error_code(http_response.body),
    }};
  }
  co_return amazon::to_aws_json_result(std::move(http_response));
}

auto is_error(KinesisApiError const& error, std::string_view code) -> bool {
  return error.code == code or error.message.find(code) != std::string::npos;
}

auto is_retryable(KinesisApiError const& error) -> bool {
  for (const auto* code :
       {"ValidationException", "ResourceNotFoundException",
        "ResourceInUseException", "AccessDeniedException",
        "InvalidArgumentException", "SerializationException",
        "UnrecognizedClientException", "MissingAuthenticationTokenException",
        "InvalidSignatureException"}) {
    if (is_error(error, code)) {
      return false;
    }
  }
  return true;
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

auto make_iterator_request(std::string_view stream, std::string_view shard_id)
  -> Aws::Kinesis::Model::GetShardIteratorRequest {
  auto request = Aws::Kinesis::Model::GetShardIteratorRequest{};
  request.SetStreamName(Aws::String{stream.data(), stream.size()});
  request.SetShardId(Aws::String{shard_id.data(), shard_id.size()});
  return request;
}

auto fetch_shard_iterator(amazon::SignedHttpClient& client,
                          Aws::Kinesis::Model::GetShardIteratorRequest request)
  -> Task<Result<std::string, KinesisApiError>> {
  auto aws_result
    = co_await kinesis_api_call(client, "GetShardIterator", request);
  if (aws_result.is_err()) {
    co_return Err{std::move(aws_result).unwrap_err()};
  }
  auto result = Aws::Kinesis::Model::GetShardIteratorResult{
    std::move(aws_result).unwrap()};
  co_return amazon::from_aws_string(result.GetShardIterator());
}

auto make_shard_iterator(amazon::SignedHttpClient& client,
                         std::string_view stream, std::string_view shard_id,
                         const Option<located<data>>& start,
                         const std::string& sequence_number)
  -> Task<Result<std::string, KinesisApiError>> {
  auto request = make_iterator_request(stream, shard_id);
  configure_iterator_start(request, start, sequence_number);
  co_return co_await fetch_shard_iterator(client, std::move(request));
}

auto make_shard_iterator(amazon::SignedHttpClient& client,
                         std::string_view stream, std::string_view shard_id,
                         time timestamp)
  -> Task<Result<std::string, KinesisApiError>> {
  auto request = make_iterator_request(stream, shard_id);
  request.SetShardIteratorType(
    Aws::Kinesis::Model::ShardIteratorType::AT_TIMESTAMP);
  request.SetTimestamp(time_to_date_time(timestamp));
  co_return co_await fetch_shard_iterator(client, std::move(request));
}

auto make_shard_iterator(amazon::SignedHttpClient& client,
                         std::string_view stream, std::string_view shard_id,
                         Aws::Kinesis::Model::ShardIteratorType type)
  -> Task<Result<std::string, KinesisApiError>> {
  auto request = make_iterator_request(stream, shard_id);
  request.SetShardIteratorType(type);
  co_return co_await fetch_shard_iterator(client, std::move(request));
}

auto list_shards(amazon::SignedHttpClient& client, std::string_view stream,
                 bool latest_only = false)
  -> Task<Result<std::vector<ShardInfo>, KinesisApiError>> {
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
    auto aws_result = co_await kinesis_api_call(client, "ListShards", request);
    if (aws_result.is_err()) {
      co_return Err{std::move(aws_result).unwrap_err()};
    }
    auto page
      = Aws::Kinesis::Model::ListShardsResult{std::move(aws_result).unwrap()};
    for (const auto& shard : page.GetShards()) {
      auto info = ShardInfo{.id = amazon::from_aws_string(shard.GetShardId())};
      if (shard.ParentShardIdHasBeenSet()) {
        info.parents.push_back(
          amazon::from_aws_string(shard.GetParentShardId()));
      }
      if (shard.AdjacentParentShardIdHasBeenSet()) {
        info.parents.push_back(
          amazon::from_aws_string(shard.GetAdjacentParentShardId()));
      }
      result.push_back(std::move(info));
    }
    next = page.GetNextToken();
  } while (not next.empty());
  co_return result;
}

auto read_from_shard(amazon::SignedHttpClient& client, std::string_view stream,
                     size_t shard_index, std::string_view shard_id,
                     std::string iterator, int limit) -> Task<ReadResult> {
  auto request = Aws::Kinesis::Model::GetRecordsRequest{};
  request.SetShardIterator(Aws::String{iterator.data(), iterator.size()});
  request.SetLimit(limit);
  auto aws_result = co_await kinesis_api_call(client, "GetRecords", request);
  if (aws_result.is_err()) {
    auto error = std::move(aws_result).unwrap_err();
    if (is_error(error, "ExpiredIteratorException")) {
      co_return {.shard_index = shard_index, .iterator_expired = true};
    }
    if (is_error(error, "ProvisionedThroughputExceededException")) {
      co_return {.shard_index = shard_index, .throttled = true};
    }
    co_return {.shard_index = shard_index, .error = std::move(error.message)};
  }
  auto result
    = Aws::Kinesis::Model::GetRecordsResult{std::move(aws_result).unwrap()};
  auto output = ReadResult{};
  output.shard_index = shard_index;
  output.next_iterator = amazon::from_aws_string(result.GetNextShardIterator());
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
    item.sequence_number = amazon::from_aws_string(record.GetSequenceNumber());
    item.partition_key = amazon::from_aws_string(record.GetPartitionKey());
    if (record.ApproximateArrivalTimestampHasBeenSet()) {
      item.arrival_time
        = date_time_to_time(record.GetApproximateArrivalTimestamp());
    }
    if (record.EncryptionTypeHasBeenSet()) {
      item.encryption_type
        = encryption_type_to_string(record.GetEncryptionType());
    }
    item.behind_latest
      = std::chrono::milliseconds{result.GetMillisBehindLatest()};
    output.next_sequence_number = item.sequence_number;
    output.records.push_back(std::move(item));
  }
  co_return output;
}

auto validate_partition_key(std::string_view key, const location& loc,
                            diagnostic_handler& dh) -> bool {
  if (key.empty()) {
    diagnostic::warning("partition key must not be empty")
      .primary(loc)
      .note("event is skipped")
      .emit(dh);
    return false;
  }
  const auto characters = detail::utf8_codepoint_count(key);
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

auto append_partition_keys(std::vector<Option<std::string>>& out,
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
          out.emplace_back(None{});
          continue;
        }
        auto value = array.GetString(i);
        out.emplace_back(std::string{value.data(), value.size()});
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

auto put_records(amazon::SignedHttpClient& client, std::string stream,
                 std::vector<ToAmazonKinesis::PendingRecord> records)
  -> Task<PutRecordsResult> {
  for (auto attempt = size_t{0}; attempt < put_records_attempts; ++attempt) {
    auto request = Aws::Kinesis::Model::PutRecordsRequest{};
    request.SetStreamName(Aws::String{stream.data(), stream.size()});
    for (const auto& record : records) {
      auto entry = Aws::Kinesis::Model::PutRecordsRequestEntry{};
      entry.SetPartitionKey(record.partition_key);
      entry.SetData(Aws::Utils::ByteBuffer{
        reinterpret_cast<const unsigned char*>(record.message.data()),
        record.message.size()});
      request.AddRecords(std::move(entry));
    }
    auto aws_result = co_await kinesis_api_call(client, "PutRecords", request);
    if (aws_result.is_err()) {
      auto error = std::move(aws_result).unwrap_err();
      if (not is_retryable(error) or attempt + 1 == put_records_attempts) {
        co_return {.failed_records = std::move(records),
                   .error = std::move(error.message)};
      }
      co_await folly::coro::sleep(100ms * (1 << attempt));
      continue;
    }
    auto result
      = Aws::Kinesis::Model::PutRecordsResult{std::move(aws_result).unwrap()};
    if (result.GetFailedRecordCount() == 0) {
      co_return PutRecordsResult{};
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
      co_return PutRecordsResult{};
    }
    if (attempt + 1 < put_records_attempts) {
      co_await folly::coro::sleep(100ms * (1 << attempt));
    }
  }
  co_return {.failed_records = std::move(records),
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

auto make_kinesis_http_client(Option<located<std::string>> const& aws_region,
                              Option<located<record>> const& aws_iam,
                              Option<located<std::string>> const& endpoint,
                              OpCtx& ctx)
  -> Task<std::shared_ptr<amazon::SignedHttpClient>> {
  auto auth = co_await resolve_aws_iam_auth(
    aws_iam ? std::optional<located<record>>{*aws_iam} : std::nullopt,
    aws_region ? std::optional<located<std::string>>{*aws_region}
               : std::nullopt,
    ctx);
  if (not auth) {
    co_return nullptr;
  }
  auto credentials = Option<resolved_aws_credentials>{};
  if (auth->credentials) {
    credentials = std::move(*auth->credentials);
  }
  auto region = amazon::resolve_region(
    aws_region ? Option<std::string>{aws_region->inner} : None{}, credentials);
  auto endpoint_value
    = endpoint ? endpoint->inner
               : amazon::service_endpoint_url("kinesis", region, "KINESIS");
  auto config = amazon::SignedHttpClientConfig{
    .service = "kinesis",
    .region = std::move(region),
    .endpoint = std::move(endpoint_value),
    .credentials = std::move(credentials),
    .io_executor = ctx.io_executor(),
    .retry_delay = std::chrono::milliseconds{100},
  };
  try {
    co_return std::make_shared<amazon::SignedHttpClient>(std::move(config));
  } catch (diagnostic& d) {
    ctx.dh().emit(std::move(d));
  } catch (std::exception const& e) {
    diagnostic::error("failed to initialize Kinesis HTTP client: {}", e.what())
      .emit(ctx);
  }
  co_return nullptr;
}

FromAmazonKinesis::FromAmazonKinesis(FromAmazonKinesisArgs args)
  : args_{std::move(args)} {
}

auto FromAmazonKinesis::start(OpCtx& ctx) -> Task<void> {
  client_ = co_await make_kinesis_http_client(args_.aws_region, args_.aws_iam,
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
  const auto fail = [&](KinesisApiError error) {
    diagnostic::error("{}", error.message)
      .primary(args_.stream.source)
      .emit(ctx);
    done_ = true;
  };
  if (shards_.empty()) {
    auto shard_infos = co_await list_shards(*client_, args_.stream.inner,
                                            starts_at_latest(args_.start));
    if (shard_infos.is_err()) {
      fail(std::move(shard_infos).unwrap_err());
      co_return;
    }
    auto infos = std::move(shard_infos).unwrap();
    shards_.reserve(infos.size());
    for (auto& info : infos) {
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
    auto iterator = co_await recreate_iterator(shard);
    if (iterator.is_err()) {
      fail(std::move(iterator).unwrap_err());
      co_return;
    }
    shard.iterator = std::move(iterator).unwrap();
  }
  bytes_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_kinesis"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_kinesis"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::events);
}

auto FromAmazonKinesis::recreate_iterator(ShardState& shard)
  -> Task<Result<std::string, KinesisApiError>> {
  if (shard.next_sequence_number.empty() and shard.trim_horizon_start) {
    co_return co_await make_shard_iterator(
      *client_, args_.stream.inner, shard.id,
      Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
  }
  if (shard.next_sequence_number.empty() and shard.latest_start_time) {
    co_return co_await make_shard_iterator(*client_, args_.stream.inner,
                                           shard.id, *shard.latest_start_time);
  }
  co_return co_await make_shard_iterator(*client_, args_.stream.inner, shard.id,
                                         args_.start,
                                         shard.next_sequence_number);
}

auto FromAmazonKinesis::discover_new_shards(OpCtx& ctx) -> Task<void> {
  const auto fail = [&](KinesisApiError error) {
    diagnostic::error("{}", error.message)
      .primary(args_.stream.source)
      .emit(ctx);
    done_ = true;
  };
  auto shard_infos = co_await list_shards(*client_, args_.stream.inner,
                                          starts_at_latest(args_.start));
  if (shard_infos.is_err()) {
    fail(std::move(shard_infos).unwrap_err());
    co_return;
  }
  for (auto& info : std::move(shard_infos).unwrap()) {
    if (std::ranges::find(shards_, info.id, &ShardState::id) != shards_.end()) {
      continue;
    }
    auto iterator = co_await make_shard_iterator(
      *client_, args_.stream.inner, info.id,
      Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
    if (iterator.is_err()) {
      fail(std::move(iterator).unwrap_err());
      co_return;
    }
    shards_.push_back(ShardState{.id = std::move(info.id),
                                 .parents = std::move(info.parents),
                                 .iterator = std::move(iterator).unwrap(),
                                 .trim_horizon_start = true});
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
  auto result
    = co_await read_from_shard(*client_, args_.stream.inner, *index, shard.id,
                               shard.iterator, records_per_call_);
  if (result.throttled) {
    co_await folly::coro::sleep(
      std::chrono::duration_cast<folly::HighResDuration>(throttle_backoff));
  }
  co_return std::move(result);
}

auto FromAmazonKinesis::process_task(Any result, Push<table_slice>& push,
                                     OpCtx& ctx) -> Task<void> {
  auto batch = std::move(result).as<ReadResult>();
  if (not batch.error.empty()) {
    diagnostic::error("{}", batch.error).primary(args_.stream.source).emit(ctx);
    done_ = true;
    co_return;
  }
  if (batch.shard_index < shards_.size()) {
    auto& shard = shards_[batch.shard_index];
    if (batch.throttled) {
      shard.idle = false;
    } else if (batch.iterator_expired) {
      auto iterator = co_await recreate_iterator(shard);
      if (iterator.is_err()) {
        diagnostic::error("{}", std::move(iterator).unwrap_err().message)
          .primary(args_.stream.source)
          .emit(ctx);
        done_ = true;
        co_return;
      }
      shard.iterator = std::move(iterator).unwrap();
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
    event.field("behind_latest").data(record.behind_latest);
    ++emitted_;
  }
  for (auto&& slice : msb.finalize_as_table_slice()) {
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    events_read_counter_.add(rows);
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
  client_ = co_await make_kinesis_http_client(args_.aws_region, args_.aws_iam,
                                              args_.endpoint, ctx);
  if (not client_) {
    failed_ = true;
    co_return;
  }
  batch_size_
    = args_.batch_size ? detail::narrow<size_t>(args_.batch_size->inner) : 500;
  batch_timeout_ = args_.batch_timeout ? args_.batch_timeout->inner : 1s;
  parallel_ = args_.parallel ? args_.parallel->inner : 1;
  bytes_write_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_amazon_kinesis"},
                       MetricsDirection::write, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_write_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_amazon_kinesis"},
                       MetricsDirection::write, MetricsVisibility::external_,
                       MetricsUnit::events);
}

auto ToAmazonKinesis::process(table_slice input, OpCtx& ctx) -> Task<void> {
  if (failed_ or input.rows() == 0) {
    co_return;
  }
  auto messages = std::vector<Option<blob>>{};
  append_messages(messages, args_.message, input, ctx.dh());
  auto partition_keys = std::vector<Option<std::string>>{};
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
      if (i >= partition_keys.size() or not partition_keys[i]) {
        continue;
      }
      key = std::move(*partition_keys[i]);
      if (not validate_partition_key(key, args_.partition_key->get_location(),
                                     ctx.dh())) {
        continue;
      }
    } else {
      key = fmt::to_string(uuid::random());
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
    const auto was_empty = batch_.empty();
    const auto now = std::chrono::steady_clock::now();
    batch_.push_back(PendingRecord{std::move(*message), std::move(key)});
    if (was_empty) {
      batch_deadline_ = now + batch_timeout_;
      batch_ready_->notify_one();
    }
    if (batch_.size() >= batch_size_) {
      co_await flush(ctx);
      if (failed_) {
        co_return;
      }
    } else {
      co_await flush_if_timed_out(ctx);
      if (failed_) {
        co_return;
      }
    }
  }
}

auto ToAmazonKinesis::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  while (true) {
    if (failed_) {
      co_await folly::coro::sleep(std::chrono::milliseconds{10});
      co_return true;
    }
    if (not batch_deadline_) {
      co_await batch_ready_->wait();
      continue;
    }
    auto const deadline = *batch_deadline_;
    auto [timeout, ready] = co_await folly::coro::collectAnyNoDiscard(
      sleep_until(deadline), batch_ready_->wait());
    if (timeout.hasValue()) {
      co_return true;
    }
    if (ready.hasValue()) {
      continue;
    }
    co_return true;
  }
}

auto ToAmazonKinesis::process_task(Any result, OpCtx& ctx) -> Task<void> {
  TENZIR_UNUSED(result);
  if (failed_) {
    co_return;
  }
  co_await flush_if_timed_out(ctx);
}

auto ToAmazonKinesis::flush_if_timed_out(OpCtx& ctx) -> Task<void> {
  if (failed_ or batch_.empty()) {
    batch_deadline_ = None{};
    co_return;
  }
  TENZIR_ASSERT(batch_deadline_);
  if (std::chrono::steady_clock::now() >= *batch_deadline_) {
    co_await flush(ctx);
  }
}

auto ToAmazonKinesis::flush(OpCtx& ctx) -> Task<void> {
  if (failed_ or batch_.empty() or not client_) {
    batch_deadline_ = None{};
    co_return;
  }
  auto records = std::exchange(batch_, {});
  batch_deadline_ = None{};
  auto sent_events = records.size();
  auto sent_bytes = size_t{0};
  for (const auto& record : records) {
    sent_bytes += record.message.size();
  }
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
        put_records(*client_, args_.stream.inner, std::move(chunks[index])));
    }
    auto results = co_await folly::coro::collectAllRange(std::move(tasks));
    for (auto& result : results) {
      if (result.error) {
        errors.push_back(std::move(*result.error));
      }
      for (const auto& failed : result.failed_records) {
        sent_bytes -= failed.message.size();
        --sent_events;
      }
      std::ranges::move(result.failed_records, std::back_inserter(batch_));
    }
  }
  bytes_write_counter_.add(sent_bytes);
  events_write_counter_.add(sent_events);
  if (not batch_.empty()) {
    batch_deadline_ = std::chrono::steady_clock::now() + batch_timeout_;
    batch_ready_->notify_one();
  }
  if (not errors.empty()) {
    failed_ = true;
    diagnostic::error("failed to write records to Kinesis")
      .primary(args_.stream.source)
      .note("{}", fmt::join(errors, "; "))
      .emit(ctx);
  }
}

auto ToAmazonKinesis::prepare_snapshot(OpCtx& ctx) -> Task<void> {
  co_await flush(ctx);
  fail_if_unsent(ctx);
  co_return;
}

auto ToAmazonKinesis::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  co_await flush(ctx);
  fail_if_unsent(ctx);
  co_return FinalizeBehavior::done;
}

auto ToAmazonKinesis::state() -> OperatorState {
  return failed_ ? OperatorState::done : OperatorState::normal;
}

auto ToAmazonKinesis::fail_if_unsent(OpCtx& ctx) -> void {
  if (failed_) {
    return;
  }
  if (not batch_.empty()) {
    failed_ = true;
    diagnostic::error("failed to write all records to Kinesis")
      .primary(args_.stream.source)
      .note("{} records remain unsent", batch_.size())
      .emit(ctx);
  }
}

} // namespace tenzir::plugins::amazon_kinesis
