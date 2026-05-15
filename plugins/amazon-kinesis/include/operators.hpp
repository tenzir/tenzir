//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/data.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/tql2/ast.hpp>

#include <aws/kinesis/KinesisClient.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace tenzir::plugins::amazon_kinesis {

struct FromAmazonKinesisArgs {
  located<std::string> stream;
  Option<located<data>> start;
  Option<located<uint64_t>> count;
  bool exit = false;
  Option<located<uint64_t>> records_per_call;
  Option<located<duration>> poll_idle;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  Option<located<std::string>> endpoint;
  location operator_location;
};

struct ToAmazonKinesisArgs {
  located<std::string> stream;
  ast::expression message;
  Option<ast::expression> partition_key;
  Option<located<uint64_t>> batch_size;
  Option<located<duration>> batch_timeout;
  Option<located<uint64_t>> parallel;
  Option<located<std::string>> aws_region;
  Option<located<record>> aws_iam;
  Option<located<std::string>> endpoint;
  location operator_location;
};

auto default_message_expression() -> ast::expression;

auto make_kinesis_client(const Option<located<std::string>>& aws_region,
                         const Option<located<record>>& aws_iam,
                         const Option<located<std::string>>& endpoint,
                         OpCtx& ctx)
  -> Task<std::shared_ptr<Aws::Kinesis::KinesisClient>>;

class FromAmazonKinesis final : public Operator<void, table_slice> {
public:
  explicit FromAmazonKinesis(FromAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;
  auto state() -> OperatorState override;
  auto snapshot(Serde& serde) -> void override;

private:
  auto discover_new_shards(OpCtx& ctx) -> Task<void>;

  struct ShardState {
    std::string id;
    std::vector<std::string> parents;
    std::string iterator;
    std::string next_sequence_number;
    Option<time> latest_start_time;
    bool closed = false;
    bool idle = false;

    friend auto inspect(auto& f, ShardState& x) -> bool {
      return f.object(x).fields(
        f.field("id", x.id), f.field("parents", x.parents),
        f.field("iterator", x.iterator),
        f.field("next_sequence_number", x.next_sequence_number),
        f.field("latest_start_time", x.latest_start_time),
        f.field("closed", x.closed), f.field("idle", x.idle));
    }
  };

  FromAmazonKinesisArgs args_;
  std::shared_ptr<Aws::Kinesis::KinesisClient> client_;
  std::vector<ShardState> shards_;
  size_t next_shard_ = 0;
  uint64_t emitted_ = 0;
  uint64_t limit_ = 0;
  int records_per_call_ = 1000;
  duration poll_idle_ = std::chrono::seconds{1};
  bool done_ = false;
  MetricsCounter bytes_read_counter_;
};

class ToAmazonKinesis final : public Operator<table_slice, void> {
public:
  struct PendingRecord {
    blob message;
    std::string partition_key;
  };

  explicit ToAmazonKinesis(ToAmazonKinesisArgs args);

  auto start(OpCtx& ctx) -> Task<void> override;
  auto process(table_slice input, OpCtx& ctx) -> Task<void> override;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override;
  auto process_task(Any result, OpCtx& ctx) -> Task<void> override;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override;

private:
  auto flush_if_timed_out(OpCtx& ctx) -> Task<void>;
  auto flush(OpCtx& ctx) -> Task<void>;

  ToAmazonKinesisArgs args_;
  std::shared_ptr<Aws::Kinesis::KinesisClient> client_;
  std::vector<PendingRecord> batch_;
  size_t batch_size_ = 500;
  duration batch_timeout_ = std::chrono::seconds{1};
  Option<std::chrono::steady_clock::time_point> batch_started_ = None{};
  uint64_t parallel_ = 1;
  MetricsCounter bytes_write_counter_;
};

} // namespace tenzir::plugins::amazon_kinesis
