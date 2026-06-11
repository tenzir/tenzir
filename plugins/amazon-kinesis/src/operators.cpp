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
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/array/array_binary.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/DateTime.h>
#include <aws/kinesis/model/DescribeStreamSummaryRequest.h>
#include <aws/kinesis/model/DescribeStreamSummaryResult.h>
#include <aws/kinesis/model/EncryptionType.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/ListShardsResult.h>
#include <aws/kinesis/model/PutRecordsResult.h>
#include <aws/kinesis/model/ShardIteratorType.h>
#include <folly/coro/Sleep.h>

#include <algorithm>
#include <iterator>
#include <ranges>
#include <span>
#include <utility>

using namespace std::chrono_literals;

namespace tenzir::plugins::amazon_kinesis {

namespace {

/// The API-level record size ceiling; streams default to 1 MiB unless their
/// maximum record size is raised explicitly.
constexpr auto max_record_size = size_t{10 * 1024 * 1024};
constexpr auto default_stream_record_size = size_t{1024 * 1024};
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
  std::string shard_id;
  std::string next_iterator;
  std::string next_sequence_number;
  /// How far this response lags behind the tip of the shard; zero means the
  /// read is caught up with the stream.
  duration behind_latest = {};
  bool shard_closed = false;
  bool iterator_expired = false;
  bool throttled = false;
  /// Whether the shard loop finished because it reached the tip in exit mode.
  bool caught_up = false;
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

auto kinesis_api_call(amazon::SignedHttpClient& client,
                      std::string_view operation, std::string body)
  -> Task<Result<Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>,
                 KinesisApiError>> {
  auto headers = Aws::Http::HeaderValueCollection{
    {"X-Amz-Target", fmt::format("Kinesis_20131202.{}", operation)},
    {"Content-Type", "application/x-amz-json-1.1"},
    {"X-Amz-Api-Version", "2013-12-02"},
  };
  auto response = co_await client.raw_post("/", std::move(body),
                                           std::move(headers), operation);
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

template <class Request>
auto kinesis_api_call(amazon::SignedHttpClient& client,
                      std::string_view operation, Request& request)
  -> Task<Result<Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>,
                 KinesisApiError>> {
  auto payload = request.SerializePayload();
  co_return co_await kinesis_api_call(
    client, operation, std::string{payload.c_str(), payload.size()});
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

auto starts_at_latest(const Option<located<data>>& start) -> bool {
  if (not start) {
    return true;
  }
  if (auto value = try_as<std::string>(start->inner)) {
    return *value != "trim_horizon";
  }
  return false;
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
                     std::string_view shard_id, std::string iterator, int limit)
  -> Task<ReadResult> {
  auto request = Aws::Kinesis::Model::GetRecordsRequest{};
  request.SetShardIterator(Aws::String{iterator.data(), iterator.size()});
  request.SetLimit(limit);
  auto aws_result = co_await kinesis_api_call(client, "GetRecords", request);
  if (aws_result.is_err()) {
    auto error = std::move(aws_result).unwrap_err();
    if (is_error(error, "ExpiredIteratorException")) {
      co_return {.shard_id = std::string{shard_id}, .iterator_expired = true};
    }
    if (is_error(error, "ProvisionedThroughputExceededException")) {
      co_return {.shard_id = std::string{shard_id}, .throttled = true};
    }
    co_return {.shard_id = std::string{shard_id},
               .error = std::move(error.message)};
  }
  auto result
    = Aws::Kinesis::Model::GetRecordsResult{std::move(aws_result).unwrap()};
  auto output = ReadResult{};
  output.shard_id = std::string{shard_id};
  output.next_iterator = amazon::from_aws_string(result.GetNextShardIterator());
  output.shard_closed = output.next_iterator.empty();
  output.behind_latest
    = std::chrono::milliseconds{result.GetMillisBehindLatest()};
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
      item.arrival_time = time{std::chrono::milliseconds{
        record.GetApproximateArrivalTimestamp().Millis()}};
    }
    if (record.EncryptionTypeHasBeenSet()) {
      item.encryption_type = amazon::from_aws_string(
        Aws::Kinesis::Model::EncryptionTypeMapper::GetNameForEncryptionType(
          record.GetEncryptionType()));
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

/// Serializes a PutRecords request body directly instead of going through the
/// AWS SDK request model. The SDK copies every payload into a JsonValue DOM
/// and then re-serializes the whole tree, costing two extra passes over every
/// payload byte; a CPU profile attributed roughly 60% of the send path to
/// this. Writing the body directly lifted sink throughput by 15-20% across
/// record sizes (562-byte records: 266 to 307 MB/s) and cut send-path CPU by
/// about 3x, leaving SigV4 payload hashing as the dominant remaining cost.
auto make_put_records_body(
  std::string_view escaped_stream,
  std::span<const ToAmazonKinesis::PendingRecord> records) -> std::string {
  auto size = escaped_stream.size() + 32;
  for (const auto& record : records) {
    size += detail::base64::encoded_size(record.message.size())
            + record.partition_key.size() + 40;
  }
  auto body = std::string{};
  body.reserve(size);
  body += R"({"StreamName":)";
  body += escaped_stream;
  body += R"(,"Records":[)";
  for (const auto& record : records) {
    if (body.back() != '[') {
      body += ',';
    }
    body += R"({"Data":")";
    const auto offset = body.size();
    body.resize(offset + detail::base64::encoded_size(record.message.size()));
    detail::base64::encode(body.data() + offset, record.message.data(),
                           record.message.size());
    body += R"(","PartitionKey":)";
    body += detail::json_escape(record.partition_key);
    body += '}';
  }
  body += "]}";
  return body;
}

auto put_records(amazon::SignedHttpClient& client, std::string stream,
                 std::vector<ToAmazonKinesis::PendingRecord> records)
  -> Task<PutRecordsResult> {
  const auto escaped_stream = detail::json_escape(stream);
  for (auto attempt = size_t{0}; attempt < put_records_attempts; ++attempt) {
    auto aws_result = co_await kinesis_api_call(
      client, "PutRecords", make_put_records_body(escaped_stream, records));
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
  limit_ = args_.count ? args_.count->inner : 0;
  records_per_call_ = args_.records_per_call
                        ? detail::narrow<int>(args_.records_per_call->inner)
                        : 1000;
  poll_idle_ = args_.poll_idle ? args_.poll_idle->inner : 1s;
}

auto FromAmazonKinesis::start(OpCtx& ctx) -> Task<void> {
  client_ = co_await make_kinesis_http_client(args_.aws_region, args_.aws_iam,
                                              args_.endpoint, ctx);
  if (not client_) {
    done_ = true;
    co_return;
  }
  if (shards_.empty()) {
    auto shard_infos = co_await list_shards(*client_, args_.stream.inner,
                                            starts_at_latest(args_.start));
    if (shard_infos.is_err()) {
      diagnostic::error("{}", shard_infos.unwrap_err().message)
        .primary(args_.stream.source)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto infos = std::move(shard_infos).unwrap();
    shards_.reserve(infos.size());
    for (auto& info : infos) {
      shards_.push_back(ShardState{.id = std::move(info.id),
                                   .parents = std::move(info.parents)});
    }
  }
  if (shards_.empty() and args_.exit) {
    done_ = true;
    co_return;
  }
  for (auto& shard : shards_) {
    if (shard.closed) {
      continue;
    }
    if (starts_at_latest(args_.start) and not shard.latest_start_time) {
      shard.latest_start_time = time::clock::now();
    }
  }
  spawn_ready_loops(ctx);
  bytes_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_kinesis"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_kinesis"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::events);
}

auto FromAmazonKinesis::recreate_shard_iterator(const ShardState& shard)
  -> Task<Result<std::string, KinesisApiError>> {
  using Aws::Kinesis::Model::ShardIteratorType;
  auto request = Aws::Kinesis::Model::GetShardIteratorRequest{};
  request.SetStreamName(
    Aws::String{args_.stream.inner.data(), args_.stream.inner.size()});
  request.SetShardId(Aws::String{shard.id.data(), shard.id.size()});
  if (not shard.next_sequence_number.empty()) {
    request.SetShardIteratorType(ShardIteratorType::AFTER_SEQUENCE_NUMBER);
    request.SetStartingSequenceNumber(shard.next_sequence_number);
  } else if (shard.trim_horizon_start) {
    request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);
  } else {
    auto timestamp = shard.latest_start_time;
    if (not timestamp and args_.start) {
      if (auto value = try_as<time>(args_.start->inner)) {
        timestamp = *value;
      }
    }
    if (timestamp) {
      const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
        timestamp->time_since_epoch());
      request.SetShardIteratorType(ShardIteratorType::AT_TIMESTAMP);
      request.SetTimestamp(Aws::Utils::DateTime{millis.count()});
    } else if (starts_at_latest(args_.start)) {
      request.SetShardIteratorType(ShardIteratorType::LATEST);
    } else {
      request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);
    }
  }
  auto aws_result
    = co_await kinesis_api_call(*client_, "GetShardIterator", request);
  if (aws_result.is_err()) {
    co_return Err{std::move(aws_result).unwrap_err()};
  }
  auto result = Aws::Kinesis::Model::GetShardIteratorResult{
    std::move(aws_result).unwrap()};
  co_return amazon::from_aws_string(result.GetShardIterator());
}

auto FromAmazonKinesis::shard_loop(ShardState shard) -> Task<void> {
  auto iterator = std::string{};
  while (true) {
    if (iterator.empty()) {
      auto created = co_await recreate_shard_iterator(shard);
      if (created.is_err()) {
        auto result = ReadResult{};
        result.shard_id = shard.id;
        result.error = std::move(created).unwrap_err().message;
        co_await results_->enqueue(Any{std::move(result)});
        co_return;
      }
      iterator = std::move(created).unwrap();
      if (iterator.empty()) {
        auto result = ReadResult{};
        result.shard_id = shard.id;
        result.shard_closed = true;
        co_await results_->enqueue(Any{std::move(result)});
        co_return;
      }
    }
    auto result = co_await read_from_shard(
      *client_, args_.stream.inner, shard.id, iterator, records_per_call_);
    if (result.throttled) {
      co_await folly::coro::sleep(
        std::chrono::duration_cast<folly::HighResDuration>(throttle_backoff));
      continue;
    }
    if (result.iterator_expired) {
      iterator.clear();
      continue;
    }
    iterator = result.next_iterator;
    if (not result.next_sequence_number.empty()) {
      shard.next_sequence_number = result.next_sequence_number;
    }
    // In exit mode, a shard is finished once it reaches the tip of the
    // stream; the loop ends and reports this completion as its final result.
    // An empty response alone is not enough: sparse streams can return empty
    // batches with records still ahead.
    result.caught_up = args_.exit and result.behind_latest == duration::zero();
    const auto failed = not result.error.empty();
    const auto closed = result.shard_closed;
    const auto finished = result.caught_up;
    const auto idle = result.records.empty();
    co_await results_->enqueue(Any{std::move(result)});
    if (failed or closed or finished) {
      co_return;
    }
    if (idle) {
      co_await folly::coro::sleep(
        std::chrono::duration_cast<folly::HighResDuration>(poll_idle_));
    }
  }
}

auto FromAmazonKinesis::parents_closed(const ShardState& shard) const -> bool {
  return std::ranges::all_of(shard.parents, [&](const std::string& parent) {
    auto it = std::ranges::find(shards_, parent, &ShardState::id);
    return it == shards_.end() or it->closed;
  });
}

auto FromAmazonKinesis::spawn_ready_loops(OpCtx& ctx) -> void {
  for (auto& shard : shards_) {
    if (shard.closed or shard.caught_up or shard.loop_running
        or not parents_closed(shard)) {
      continue;
    }
    shard.loop_running = true;
    ctx.spawn_task(shard_loop(shard));
  }
}

auto FromAmazonKinesis::discover_new_shards(OpCtx& ctx) -> Task<void> {
  // List without the AT_LATEST filter: descendants of a consumed shard may
  // themselves have closed again before discovery runs, and the latest-only
  // view would skip them and lose their records. For latest-start pipelines,
  // lineage decides membership instead: a discovered shard joins only if it
  // descends from a tracked shard, which keeps pre-start history excluded.
  auto shard_infos = co_await list_shards(*client_, args_.stream.inner);
  if (shard_infos.is_err()) {
    diagnostic::error("{}", shard_infos.unwrap_err().message)
      .primary(args_.stream.source)
      .emit(ctx);
    done_ = true;
    co_return;
  }
  auto infos = std::move(shard_infos).unwrap();
  const auto latest = starts_at_latest(args_.start);
  auto added = true;
  while (added) {
    added = false;
    for (auto& info : infos) {
      if (info.id.empty()
          or std::ranges::find(shards_, info.id, &ShardState::id)
               != shards_.end()) {
        continue;
      }
      const auto descends_from_tracked
        = std::ranges::any_of(info.parents, [&](const std::string& parent) {
            return std::ranges::find(shards_, parent, &ShardState::id)
                   != shards_.end();
          });
      if (latest and not descends_from_tracked) {
        continue;
      }
      shards_.push_back(ShardState{.id = std::move(info.id),
                                   .parents = std::move(info.parents),
                                   .trim_horizon_start = true});
      added = true;
    }
  }
}

auto FromAmazonKinesis::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  co_return co_await results_->dequeue();
}

auto FromAmazonKinesis::process_task(Any result, Push<table_slice>& push,
                                     OpCtx& ctx) -> Task<void> {
  auto batch = std::move(result).as<ReadResult>();
  if (not batch.error.empty()) {
    diagnostic::error("{}", batch.error).primary(args_.stream.source).emit(ctx);
    done_ = true;
    co_return;
  }
  if (auto it = std::ranges::find(shards_, batch.shard_id, &ShardState::id);
      it != shards_.end()) {
    it->closed = batch.shard_closed;
    if (batch.caught_up) {
      it->caught_up = true;
      it->loop_running = false;
    }
    if (not batch.next_sequence_number.empty()) {
      it->next_sequence_number = batch.next_sequence_number;
    }
    if (batch.shard_closed) {
      it->loop_running = false;
      co_await discover_new_shards(ctx);
      if (done_) {
        co_return;
      }
      spawn_ready_loops(ctx);
    }
  }
  if (batch.records.empty()) {
    if (args_.exit) {
      done_ = all_shards_finished();
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
  // A shard can deliver its final records and finish or close in the same
  // batch, so the exit condition must also be evaluated when records arrived.
  if (args_.exit and not done_) {
    done_ = all_shards_finished();
  }
}

auto FromAmazonKinesis::all_shards_finished() const -> bool {
  // Exit once every shard has either closed or caught up with the stream
  // tip. Both flags are monotone and only set when the shard's own loop has
  // already terminated, and the results queue is FIFO, so by the time the
  // last completion marker is processed, every record enqueued before it has
  // been delivered. No in-flight read can race this decision.
  return std::ranges::all_of(shards_, [](const ShardState& shard) {
    return shard.closed or shard.caught_up;
  });
}

auto FromAmazonKinesis::state() -> OperatorState {
  return done_ ? OperatorState::done : OperatorState::normal;
}

auto FromAmazonKinesis::snapshot(Serde& serde) -> void {
  serde("shards", shards_);
  serde("emitted", emitted_);
  serde("done", done_);
}

ToAmazonKinesis::ToAmazonKinesis(ToAmazonKinesisArgs args)
  : args_{std::move(args)} {
  batch_size_
    = args_.batch_size ? detail::narrow<size_t>(args_.batch_size->inner) : 500;
  batch_timeout_ = args_.batch_timeout ? args_.batch_timeout->inner : 1s;
  request_slots_ = Semaphore{
    detail::narrow<size_t>(args_.parallel ? args_.parallel->inner : 1)};
}

auto ToAmazonKinesis::start(OpCtx& ctx) -> Task<void> {
  client_ = co_await make_kinesis_http_client(args_.aws_region, args_.aws_iam,
                                              args_.endpoint, ctx);
  if (not client_) {
    failed_ = true;
    co_return;
  }
  auto request = Aws::Kinesis::Model::DescribeStreamSummaryRequest{};
  request.SetStreamName(
    Aws::String{args_.stream.inner.data(), args_.stream.inner.size()});
  auto aws_result
    = co_await kinesis_api_call(*client_, "DescribeStreamSummary", request);
  if (aws_result.is_ok()) {
    auto result = Aws::Kinesis::Model::DescribeStreamSummaryResult{
      std::move(aws_result).unwrap()};
    const auto& summary = result.GetStreamDescriptionSummary();
    max_record_size_
      = summary.MaxRecordSizeInKiBHasBeenSet()
          ? detail::narrow<size_t>(summary.GetMaxRecordSizeInKiB()) * 1024
          : default_stream_record_size;
  } else if (is_error(aws_result.unwrap_err(), "ResourceNotFoundException")) {
    diagnostic::error("{}", aws_result.unwrap_err().message)
      .primary(args_.stream.source)
      .emit(ctx);
    failed_ = true;
    co_return;
  }
  // Without permission to describe the stream we cannot know its configured
  // record size limit, so stay at the API maximum and let the service enforce
  // the effective limit.
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
    if (message->size() + key.size() > max_record_size_) {
      diagnostic::warning("Kinesis record payload and partition key must be at "
                          "most {} bytes for this stream",
                          max_record_size_)
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
      if (not timer_armed_) {
        arm_flush_timer(ctx);
      }
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
  co_return co_await wakeup_queue_->dequeue();
}

auto ToAmazonKinesis::process_task(Any result, OpCtx& ctx) -> Task<void> {
  if (failed_) {
    co_return;
  }
  if (result.try_as<ReportReady>()) {
    drain_send_reports(ctx);
    co_return;
  }
  TENZIR_ASSERT(result.try_as<FlushTimeout>());
  timer_armed_ = false;
  co_await flush_if_timed_out(ctx);
  if (not failed_ and batch_deadline_) {
    // The timer was armed for a batch that has since been flushed by size;
    // re-arm it for the batch that started afterwards.
    arm_flush_timer(ctx);
  }
}

auto ToAmazonKinesis::arm_flush_timer(OpCtx& ctx) -> void {
  TENZIR_ASSERT(batch_deadline_);
  timer_armed_ = true;
  ctx.spawn_task([queue = wakeup_queue_,
                  deadline = *batch_deadline_]() mutable -> Task<void> {
    co_await sleep_until(deadline);
    co_await queue->enqueue(Any{FlushTimeout{}});
  });
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
  drain_send_reports(ctx);
  auto chunks = std::vector<std::vector<PendingRecord>>{};
  auto chunk = std::vector<PendingRecord>{};
  auto chunk_size = size_t{0};
  for (auto& record : records) {
    const auto size = record.message.size() + record.partition_key.size();
    if ((not chunk.empty() and chunk_size + size > max_put_records_request_size)
        or chunk.size() >= max_put_records_entries) {
      chunks.push_back(std::exchange(chunk, {}));
      chunk_size = 0;
    }
    chunk_size += size;
    chunk.push_back(std::move(record));
  }
  if (not chunk.empty()) {
    chunks.push_back(std::move(chunk));
  }
  for (auto index = size_t{0}; index < chunks.size(); ++index) {
    auto permit = request_slots_.try_acquire();
    while (not permit and not failed_) {
      auto report = co_await send_queue_->dequeue();
      handle_send_report(std::move(report), ctx);
      permit = request_slots_.try_acquire();
    }
    if (failed_) {
      // Return unsent chunks to the batch so that fail_if_unsent() accounts
      // for them.
      for (auto& chunk : chunks | std::views::drop(index)) {
        std::ranges::move(chunk, std::back_inserter(batch_));
      }
      co_return;
    }
    ++pending_reports_;
    ctx.spawn_task([client = client_, stream = args_.stream.inner,
                    records = std::move(chunks[index]), queue = send_queue_,
                    wakeup = wakeup_queue_,
                    permit = std::move(*permit)]() mutable -> Task<void> {
      auto report = SendReport{};
      report.events = records.size();
      for (const auto& record : records) {
        report.bytes += record.message.size();
      }
      auto result
        = co_await put_records(*client, std::move(stream), std::move(records));
      for (const auto& failed : result.failed_records) {
        report.bytes -= failed.message.size();
        --report.events;
      }
      report.failed_records = std::move(result.failed_records);
      if (result.error) {
        report.errors.push_back(std::move(*result.error));
      }
      permit.release();
      co_await queue->enqueue(std::move(report));
      co_await wakeup->enqueue(Any{ReportReady{}});
    });
  }
}

auto ToAmazonKinesis::handle_send_report(SendReport report, OpCtx& ctx)
  -> void {
  bytes_write_counter_.add(report.bytes);
  events_write_counter_.add(report.events);
  TENZIR_ASSERT(pending_reports_ > 0);
  --pending_reports_;
  if (not report.errors.empty()) {
    failed_ = true;
    diagnostic::error("failed to write records to Kinesis")
      .primary(args_.stream.source)
      .note("{}", fmt::join(report.errors, "; "))
      .emit(ctx);
  }
  // Return failed records to the batch so that fail_if_unsent() accounts for
  // them.
  std::ranges::move(report.failed_records, std::back_inserter(batch_));
}

auto ToAmazonKinesis::drain_send_reports(OpCtx& ctx) -> void {
  while (auto report = send_queue_->try_dequeue()) {
    handle_send_report(std::move(*report), ctx);
  }
}

auto ToAmazonKinesis::wait_for_requests(OpCtx& ctx) -> Task<void> {
  while (pending_reports_ > 0) {
    auto report = co_await send_queue_->dequeue();
    handle_send_report(std::move(report), ctx);
  }
}

auto ToAmazonKinesis::prepare_snapshot(OpCtx& ctx) -> Task<void> {
  co_await flush(ctx);
  co_await wait_for_requests(ctx);
  fail_if_unsent(ctx);
  co_return;
}

auto ToAmazonKinesis::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  co_await flush(ctx);
  co_await wait_for_requests(ctx);
  fail_if_unsent(ctx);
  co_return FinalizeBehavior::done;
}

auto ToAmazonKinesis::state() -> OperatorState {
  return failed_ ? OperatorState::done : OperatorState::normal;
}

auto ToAmazonKinesis::fail_if_unsent(OpCtx& ctx) -> void {
  if (batch_.empty()) {
    return;
  }
  failed_ = true;
  diagnostic::error("failed to write all records to Kinesis")
    .primary(args_.stream.source)
    .note("{} records remain unsent", batch_.size())
    .emit(ctx);
  // Clearing the batch makes the report idempotent if both a snapshot and
  // finalization observe the same failure.
  batch_.clear();
}

} // namespace tenzir::plugins::amazon_kinesis
