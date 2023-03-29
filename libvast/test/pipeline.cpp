//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/test/fixtures/events.hpp"

#include <vast/pipeline.hpp>
#include <vast/test/test.hpp>

#include <caf/test/dsl.hpp>

namespace vast {
namespace {

class dummy_control_plane final : public operator_control_plane {
public:
  auto get_error() const -> caf::error {
    return error_;
  }

  auto self() noexcept -> caf::event_based_actor& override {
    die("not implemented");
  }

  auto abort(caf::error error) noexcept -> void override {
    VAST_ASSERT(error != caf::none);
    error_ = error;
  }

  auto warn(caf::error) noexcept -> void override {
    die("not implemented");
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto demand(type = {}) const noexcept -> size_t override {
    die("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    die("not implemented");
  }

  auto concepts() const noexcept -> const concepts_map& override {
    die("not implemented");
  }

private:
  caf::error error_{};
};

struct command final : public crtp_operator<command> {
  auto operator()() const -> generator<std::monostate> {
    MESSAGE("hello, world!");
    co_return;
  }

  auto to_string() const -> std::string override {
    return "command";
  }
};

struct source final : public crtp_operator<source> {
  explicit source(std::vector<table_slice> events)
    : events_(std::move(events)) {
  }

  auto operator()() const -> generator<table_slice> {
    auto guard = caf::detail::scope_guard{[] {
      MESSAGE("source destroy");
    }};
    for (auto& table_slice : events_) {
      MESSAGE("source yield");
      co_yield table_slice;
    }
    MESSAGE("source return");
  }

  auto to_string() const -> std::string override {
    return "source";
  }

  std::vector<table_slice> events_;
};

struct sink final : public crtp_operator<sink> {
  explicit sink(std::function<void(table_slice)> callback)
    : callback_(std::move(callback)) {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<std::monostate> {
    auto guard = caf::detail::scope_guard{[] {
      MESSAGE("sink destroy");
    }};
    for (auto&& slice : input) {
      if (slice.rows() != 0) {
        MESSAGE("sink callback");
        callback_(slice);
      }
      MESSAGE("sink yield");
      co_yield {};
    }
    MESSAGE("sink return");
  }

  auto to_string() const -> std::string override {
    return "sink";
  }

  std::function<void(table_slice)> callback_;
};

struct fixture : fixtures::events {};

FIXTURE_SCOPE(pipeline_fixture, fixture)

TEST(taste 42) {
  {
    auto v = unbox(pipeline::parse("taste 42")).unwrap();
    v.insert(v.begin(),
             std::make_unique<source>(std::vector<table_slice>{
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1),
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1)}));
    auto count = 0;
    v.push_back(std::make_unique<sink>([&](table_slice) {
      count += 1;
    }));
    auto p = pipeline{std::move(v)};
    for (auto&& error : make_local_executor(std::move(p))) {
      REQUIRE_EQUAL(error, caf::error{});
    }
    CHECK_GREATER(count, 0);
  }
}

TEST(source | where #type == "zeek.conn" | sink) {
  auto count = 0;
  auto v = unbox(pipeline::parse(R"(taste 42 | where #type == "zeek.conn")"))
             .unwrap();
  v.insert(v.begin(),
           std::make_unique<source>(std::vector<table_slice>{
             head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1),
             head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1)}));
  v.push_back(std::make_unique<sink>([&](table_slice) {
    MESSAGE("---- sink ----");
    count += 1;
  }));
  auto executor = make_local_executor(pipeline{std::move(v)});
  for (auto&& error : executor) {
    REQUIRE_EQUAL(error, caf::none);
  }
  REQUIRE_GREATER(count, 0);
}

FIXTURE_SCOPE_END()

TEST(pipeline operator typing) {
  dummy_control_plane ctrl;
  {
    auto p = unbox(pipeline::parse(""));
    REQUIRE(std::holds_alternative<generator<std::monostate>>(
      unbox(p.instantiate(std::monostate{}, ctrl))));
    REQUIRE(std::holds_alternative<generator<chunk_ptr>>(
      unbox(p.instantiate(generator<chunk_ptr>{}, ctrl))));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("pass"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE(std::holds_alternative<generator<chunk_ptr>>(
      unbox(p.instantiate(generator<chunk_ptr>{}, ctrl))));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("taste 42"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("where :ip"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("taste 13 | pass | where abc == 123"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
}

TEST(command) {
  std::vector<operator_ptr> ops;
  ops.push_back(std::make_unique<command>());
  auto put = pipeline{std::move(ops)};
  for (auto&& error : make_local_executor(std::move(put))) {
    REQUIRE_EQUAL(error, caf::none);
  }
}

TEST(roundtrip) {
  auto original = std::string{
    "drop xyz, :ip "
    "| hash --salt=\"eIudsnREd\" name "
    "| head 42 "
    "| pseudonymize --method=\"crypto-pan\" --seed=\"abcd1234\" a "
    "| rename test=:suricata.flow, source_port=src_port "
    "| replace a=\"xyz\", b=[1, 2, 3], c=[\"foo\"] "
    "| extend a=\"xyz\", b=[1, 2, 3], c=[\"foo\"] "
    "| select :ip, timestamp "
    "| summarize abc=sum(:uint64,def), any(:ip) by ghi, :subnet resolution 5ns "
    "| taste 123"};
  auto roundtrip = unbox(pipeline::parse(original)).to_string();
  CHECK_EQUAL(roundtrip, original);
}

TEST(to - stdout) {
  auto to_pipeline = pipeline::parse("to stdout");
  REQUIRE_NOERROR(to_pipeline);
  REQUIRE_EQUAL(to_pipeline->to_string(), "write json | to stdout");
}

TEST(to - to stdout write json) {
  auto to_pipeline = pipeline::parse("to stdout write json");
  REQUIRE_NOERROR(to_pipeline);
  REQUIRE_EQUAL(to_pipeline->to_string(), "write json | to stdout");
}

TEST(to - invalid inputs) {
  REQUIRE_ERROR(pipeline::parse("to json write stdout"));
}

TEST(write - write json) {
  auto write_pipeline = pipeline::parse("write json");
  REQUIRE_NOERROR(write_pipeline);
  REQUIRE_EQUAL(write_pipeline->to_string(), "write json | to stdout");
}

TEST(write - write json to stdout) {
  auto write_pipeline = pipeline::parse("write json to stdout");
  REQUIRE_NOERROR(write_pipeline);
  REQUIRE_EQUAL(write_pipeline->to_string(), "write json | to stdout");
}

TEST(write - invalid inputs) {
  REQUIRE_ERROR(pipeline::parse("write stdout to json"));
}

} // namespace
} // namespace vast
