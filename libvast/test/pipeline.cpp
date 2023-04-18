//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/pipeline.hpp>
#include <vast/test/fixtures/events.hpp>
#include <vast/test/stdin_file_inut.hpp>
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
    auto v = unbox(pipeline::parse("taste 42", record{})).unwrap();
    v.insert(v.begin(),
             std::make_unique<source>(std::vector<table_slice>{
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1),
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1)}));
    auto count = 0;
    v.push_back(std::make_unique<sink>([&](table_slice) {
      count += 1;
    }));
    auto p = pipeline{std::move(v)};
    for (auto&& result : make_local_executor(std::move(p))) {
      REQUIRE_NOERROR(result);
    }
    CHECK_GREATER(count, 0);
  }
}

TEST(source | where #type == "zeek.conn" | sink) {
  auto count = size_t{0};
  auto v = unbox(pipeline::parse(R"(taste 42 | where #type == "zeek.conn")",
                                 record{}))
             .unwrap();
  v.insert(v.begin(),
           std::make_unique<source>(std::vector<table_slice>{
             head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 2),
             head(zeek_conn_log.at(0), 3), head(zeek_conn_log.at(0), 4)}));
  v.push_back(std::make_unique<sink>([&](table_slice slice) {
    MESSAGE("---- sink ----");
    count += slice.rows();
  }));
  auto executor = make_local_executor(pipeline{std::move(v)});
  for (auto&& result : executor) {
    REQUIRE_NOERROR(result);
  }
  REQUIRE_EQUAL(count, size_t{10});
}

TEST(tail 5) {
  {
    auto v = unbox(pipeline::parse("tail 5", record{})).unwrap();
    v.insert(v.begin(),
             std::make_unique<source>(std::vector<table_slice>{
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1),
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1)}));
    auto count = 0;
    v.push_back(std::make_unique<sink>([&](table_slice) {
      count += 1;
    }));
    auto p = pipeline{std::move(v)};
    for (auto&& result : make_local_executor(std::move(p))) {
      REQUIRE_NOERROR(result);
    }
    CHECK_GREATER(count, 0);
  }
}

TEST(unique) {
  auto ops
    = unbox(pipeline::parse("select id.orig_h | unique", record{})).unwrap();
  ops.insert(ops.begin(), std::make_unique<source>(std::vector<table_slice>{
                            head(zeek_conn_log.at(0), 1), // = 1
                            head(zeek_conn_log.at(0), 1), // + 0
                            head(zeek_conn_log.at(0), 5), // + 4
                            head(zeek_conn_log.at(0), 0), // + 0
                            head(zeek_conn_log.at(0), 5), // + 4
                            head(zeek_conn_log.at(0), 7), // + 5
                            head(zeek_conn_log.at(0), 7), // + 6
                            head(zeek_conn_log.at(0), 8), // + 7
                          }));
  auto count = size_t{0};
  ops.push_back(std::make_unique<sink>([&](table_slice slice) {
    count += slice.rows();
  }));
  auto executor = make_local_executor(pipeline{std::move(ops)});
  for (auto&& error : executor) {
    REQUIRE_NOERROR(error);
  }
  CHECK_EQUAL(count, size_t{1 + 0 + 4 + 0 + 4 + 5 + 6 + 7});
}

FIXTURE_SCOPE_END()

TEST(pipeline operator typing) {
  dummy_control_plane ctrl;
  {
    auto p = unbox(pipeline::parse("", record{}));
    REQUIRE_NOERROR((p.check_type<void, void>()));
    REQUIRE(std::holds_alternative<generator<std::monostate>>(
      unbox(p.instantiate(std::monostate{}, ctrl))));
    REQUIRE(p.infer_type<chunk_ptr>().value().is<chunk_ptr>());
    REQUIRE(std::holds_alternative<generator<chunk_ptr>>(
      unbox(p.instantiate(generator<chunk_ptr>{}, ctrl))));
    REQUIRE(p.infer_type<table_slice>().value().is<table_slice>());
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("pass", record{}));
    REQUIRE(!p.infer_type<void>());
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE(p.infer_type<chunk_ptr>().value().is<chunk_ptr>());
    REQUIRE(std::holds_alternative<generator<chunk_ptr>>(
      unbox(p.instantiate(generator<chunk_ptr>{}, ctrl))));
    REQUIRE(p.infer_type<table_slice>().value().is<table_slice>());
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("taste 42", record{}));
    REQUIRE(!p.infer_type<void>());
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.infer_type<chunk_ptr>());
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(p.infer_type<table_slice>().value().is<table_slice>());
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("where :ip", record{}));
    REQUIRE(!p.infer_type<void>());
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.infer_type<chunk_ptr>());
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(p.infer_type<table_slice>().value().is<table_slice>());
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p
      = unbox(pipeline::parse("taste 13 | pass | where abc == 123", record{}));
    REQUIRE(!p.infer_type<void>());
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.infer_type<chunk_ptr>());
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(p.infer_type<table_slice>().value().is<table_slice>());
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

TEST(to_string) {
  // The behavior tested here should not be relied upon and may change.
  auto expected = std::string{
    "drop xyz, :ip "
    "| hash --salt=\"eIudsnREd\" name "
    "| head 42 "
    "| pseudonymize --method=\"crypto-pan\" --seed=\"abcd1234\" a "
    "| rename test=:suricata.flow, source_port=src_port "
    "| put a=\"xyz\", b=[1, 2, 3], c=[\"foo\"] "
    "| tail 1 "
    "| select :ip, timestamp "
    "| summarize abc=sum(:uint64,def), any(:ip) by ghi, :subnet resolution 5ns "
    "| taste 123 "
    "| unique"};
  auto actual = unbox(pipeline::parse(expected, record{})).to_string();
  CHECK_EQUAL(actual, expected);
}

TEST(predicate pushdown into empty pipeline) {
  auto pipeline
    = unbox(pipeline::parse("where x == 1 | where y == 2", record{}));
  auto result
    = pipeline.predicate_pushdown_pipeline(unbox(to<expression>("z == 3")));
  REQUIRE(result);
  auto [expr, op] = std::move(*result);
  CHECK(std::move(op).unwrap().empty());
  CHECK_EQUAL(unbox(normalize_and_validate(expr)),
              to<expression>("x == 1 && y == 2 && z == 3"));
}

TEST(predicate pushdown select conflict) {
  auto pipeline = unbox(pipeline::parse("where x == 0 | select x, z | "
                                        "where y > 0 | where y < 5",
                                        record{}));
  auto result = pipeline.predicate_pushdown(unbox(to<expression>("z == 3")));
  REQUIRE(result);
  auto [expr, op] = std::move(*result);
  CHECK_EQUAL(op->to_string(),
              "select x, z | where (y > 0 && y < 5 && z == 3)");
  auto expected_expr
    = conjunction{unbox(to<expression>("x == 0")), trivially_true_expression()};
  CHECK_EQUAL(unbox(normalize_and_validate(expr)), expected_expr);
}

TEST(to - stdout) {
  auto to_pipeline = pipeline::parse("to stdout", record{});
  REQUIRE_NOERROR(to_pipeline);
  REQUIRE_EQUAL(to_pipeline->to_string(), "print json | save stdout");
}

TEST(to - to stdout write json) {
  auto to_pipeline = pipeline::parse("to stdout write json", record{});
  REQUIRE_NOERROR(to_pipeline);
  REQUIRE_EQUAL(to_pipeline->to_string(), "print json | save stdout");
}

TEST(to - invalid inputs) {
  REQUIRE_ERROR(pipeline::parse("to json write stdout", record{}));
}

TEST(stdin with json parser with all from and read combinations) {
  auto definitions = {"from stdin",
                      "from stdin --timeout 1s",
                      "from stdin read json",
                      "from stdin --timeout 1s read json",
                      "read json from stdin",
                      "read json from stdin --timeout 1s",
                      "read json"};
  for (auto definition : definitions) {
    MESSAGE("trying '" << definition << "'");
    test::stdin_file_input<"artifacts/inputs/json.txt"> file;
    auto source = unbox(pipeline::parse(definition, record{}));
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<pipeline>(std::move(source)));
    auto sink_called = false;
    ops.push_back(std::make_unique<sink>([&sink_called](table_slice slice) {
      sink_called = true;
      REQUIRE_EQUAL(slice.rows(), 2u);
      REQUIRE_EQUAL(slice.columns(), 2u);
      CHECK_EQUAL(materialize(slice.at(0u, 0u)), int64_t{1});
      CHECK_EQUAL(materialize(slice.at(0u, 1u)), caf::none);
      CHECK_EQUAL(materialize(slice.at(1u, 0u)), caf::none);
      CHECK_EQUAL(materialize(slice.at(1u, 1u)), "2");
    }));
    for (auto&& x : make_local_executor(pipeline{std::move(ops)})) {
      REQUIRE_NOERROR(x);
    }
    REQUIRE(sink_called);
  }
}

TEST(user defined operator alias) {
  // We could detect some errors in the config file when loading the config.
  // This test assumes that the error is only triggered when using the alias.
  auto config_data = unbox(from_yaml(R"__(
vast:
  operators:
    something_random: put something_random=123
    anonymize_urls: put net.url="xxx" | something_random
    self_recursive: self_recursive | self_recursive
    mut_recursive1: mut_recursive2
    mut_recursive2: mut_recursive1
    head: tail
  pipelines: # <-- TODO: This is deprecated. Remove.
    aggregate_flows: |
       summarize
         pkts_toserver=sum(flow.pkts_toserver),
         pkts_toclient=sum(flow.pkts_toclient),
         bytes_toserver=sum(flow.bytes_toserver),
         bytes_toclient=sum(flow.bytes_toclient),
         start=min(flow.start),
         end=max(flow.end)
       by
         timestamp,
         src_ip,
         dest_ip
       resolution
         10 mins
  )__"));
  auto config = caf::get_if<record>(&config_data);
  REQUIRE(config);
  auto ops = unbox(pipeline::parse("anonymize_urls | aggregate_flows", *config))
               .unwrap();
  REQUIRE_EQUAL(ops.size(), size_t{3});
  REQUIRE_ERROR(pipeline::parse("aggregate_urls", *config));
  REQUIRE_ERROR(pipeline::parse("self_recursive", *config));
  REQUIRE_ERROR(pipeline::parse("mut_recursive1", *config));
  REQUIRE_ERROR(pipeline::parse("head", *config));
}

auto execute(pipeline pipe) -> caf::expected<void> {
  for (auto&& result : make_local_executor(std::move(pipe))) {
    if (!result) {
      return result;
    }
  }
  return {};
}

// TODO: Investigate crash for changed stdin plugin name.

TEST(load_stdin_arguments) {
  auto success = {"load stdin", "load stdin --timeout 1s"};
  auto error = {"load stdin --timeout", "load stdin --timeout nope",
                "load stdin --t1me0ut 1s", "load stdin --timeout 1s 2s"};
  for (auto x : success) {
    MESSAGE(x);
    test::stdin_file_input<"artifacts/inputs/json.txt"> file;
    REQUIRE_NOERROR(
      execute(unbox(pipeline::parse(fmt::format("{} | save stdout", x), {}))));
  }
  for (auto x : error) {
    MESSAGE(x);
    test::stdin_file_input<"artifacts/inputs/json.txt"> file;
    // This test shows that pipeline parsing still succeeds. This is because
    // arguments are only checked when the actual loader is created, which
    // currently happens during instantiation.
    REQUIRE_ERROR(unbox(pipeline::parse(fmt::format("{} | save stdout", x), {}))
                    .infer_type<void>());
  }
}

} // namespace
} // namespace vast
