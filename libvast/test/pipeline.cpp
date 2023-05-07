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
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/pp.hpp>
#include <vast/pipeline.hpp>
#include <vast/pipeline_executor.hpp>
#include <vast/plugin.hpp>
#include <vast/test/fixtures/actor_system.hpp>
#include <vast/test/fixtures/actor_system_and_events.hpp>
#include <vast/test/fixtures/events.hpp>
#include <vast/test/stdin_file_inut.hpp>
#include <vast/test/test.hpp>
#include <vast/test/utils.hpp>

#include <caf/detail/scope_guard.hpp>
#include <caf/test/dsl.hpp>

namespace vast {
namespace {

class dummy_control_plane final : public operator_control_plane {
public:
  auto get_error() const -> caf::error {
    return error_;
  }

  auto self() noexcept -> system::execution_node_actor::base& override {
    FAIL("no mock implementation available");
  }

  auto node() noexcept -> system::node_actor override {
    FAIL("no mock implementation available");
  }

  auto abort(caf::error error) noexcept -> void override {
    VAST_ASSERT(error != caf::none);
    error_ = error;
  }

  auto warn(caf::error) noexcept -> void override {
    FAIL("not implemented");
  }

  auto emit(table_slice) noexcept -> void override {
    FAIL("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    FAIL("not implemented");
  }

  auto concepts() const noexcept -> const concepts_map& override {
    FAIL("not implemented");
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

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() : deterministic_actor_system_and_events{VAST_PP_STRINGIFY(SUITE)} {
  }

  auto execute(pipeline p) -> caf::expected<void> {
    MESSAGE("executing pipeline: " << p.to_string());
    auto self = caf::scoped_actor{sys};
    auto executor = self->spawn(pipeline_executor, std::move(p));
    auto handle = self->request(executor, caf::infinite, atom::run_v);
    run();
    auto result = std::optional<caf::expected<void>>{};
    std::move(handle).receive(
      [&, executor] {
        (void)executor;
        result.emplace();
      },
      [&, executor](caf::error& error) {
        (void)executor;
        result.emplace(std::move(error));
      });
    REQUIRE(result.has_value());
    return std::move(*result);
  }
};

FIXTURE_SCOPE(pipeline_fixture, fixture)

TEST(actor executor success) {
  for (auto num : {0, 1, 4, 5}) {
    auto v = unbox(pipeline::parse(fmt::format("head {}", num))).unwrap();
    v.insert(v.begin(),
             std::make_unique<source>(std::vector<table_slice>{
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1),
               head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1)}));
    auto actual = 0;
    v.push_back(std::make_unique<sink>([&](table_slice) {
      actual += 1;
    }));
    auto p = pipeline{std::move(v)};
    REQUIRE_NOERROR(execute(p));
    CHECK_EQUAL(actual, std::min(num, 4));
  }
}

TEST(actor executor execution error) {
  class execution_error_operator final
    : public crtp_operator<execution_error_operator> {
  public:
    auto operator()(generator<table_slice>, operator_control_plane& ctrl) const
      -> generator<table_slice> {
      ctrl.abort(caf::make_error(ec::unspecified));
      co_return;
    }

    auto to_string() const -> std::string override {
      return "error";
    }
  };

  auto ops = std::vector<operator_ptr>{};
  ops.push_back(std::make_unique<source>(zeek_conn_log));
  ops.push_back(std::make_unique<execution_error_operator>());
  ops.push_back(std::make_unique<sink>([](table_slice input) {
    CHECK(input.rows() == 0);
  }));
  auto pipe = pipeline{std::move(ops)};
  REQUIRE_ERROR(execute(pipe));
}

TEST(actor executor instantiation error) {
  class instantiation_error_operator final
    : public crtp_operator<instantiation_error_operator> {
  public:
    auto operator()(generator<table_slice>, operator_control_plane&) const
      -> caf::expected<generator<table_slice>> {
      return caf::make_error(ec::unspecified);
    }

    auto to_string() const -> std::string override {
      return "error";
    }
  };

  auto ops = std::vector<operator_ptr>{};
  ops.push_back(std::make_unique<source>(zeek_conn_log));
  ops.push_back(std::make_unique<instantiation_error_operator>());
  ops.push_back(std::make_unique<sink>([](table_slice) {
    CHECK(false);
  }));
  auto pipe = pipeline{std::move(ops)};
  REQUIRE_ERROR(execute(pipe));
}

TEST(taste 42) {
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
  for (auto&& result : make_local_executor(std::move(p))) {
    REQUIRE_NOERROR(result);
  }
  CHECK_GREATER(count, 0);
}

TEST(source | where #type == "zeek.conn" | sink) {
  auto count = size_t{0};
  auto v = unbox(pipeline::parse(R"(taste 42 | where #type == "zeek.conn")"))
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
    auto v = unbox(pipeline::parse("tail 5")).unwrap();
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
  auto ops = unbox(pipeline::parse("select id.orig_h | unique")).unwrap();
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
    auto p = unbox(pipeline::parse(""));
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
    auto p = unbox(pipeline::parse("pass"));
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
    auto p = unbox(pipeline::parse("taste 42"));
    REQUIRE(!p.infer_type<void>());
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.infer_type<chunk_ptr>());
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(p.infer_type<table_slice>().value().is<table_slice>());
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("where :ip"));
    REQUIRE(!p.infer_type<void>());
    REQUIRE_ERROR(p.instantiate(std::monostate{}, ctrl));
    REQUIRE_ERROR(p.infer_type<chunk_ptr>());
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, ctrl));
    REQUIRE(p.infer_type<table_slice>().value().is<table_slice>());
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, ctrl))));
  }
  {
    auto p = unbox(pipeline::parse("taste 13 | pass | where abc == 123"));
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
  auto actual = unbox(pipeline::parse(expected)).to_string();
  CHECK_EQUAL(actual, expected);
}

TEST(predicate pushdown into empty pipeline) {
  auto pipeline = unbox(pipeline::parse("where x == 1 | where y == 2"));
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
                                        "where y > 0 | where y < 5"));
  auto result = pipeline.predicate_pushdown(unbox(to<expression>("z == 3")));
  REQUIRE(result);
  auto [expr, op] = std::move(*result);
  CHECK_EQUAL(op->to_string(),
              "select x, z | where (y > 0 && y < 5 && z == 3)");
  auto expected_expr
    = conjunction{unbox(to<expression>("x == 0")), trivially_true_expression()};
  CHECK_EQUAL(unbox(normalize_and_validate(expr)), expected_expr);
}

TEST(to -) {
  auto to_pipeline = pipeline::parse("to stdout");
  REQUIRE_NOERROR(to_pipeline);
  REQUIRE_EQUAL(to_pipeline->to_string(), "local print json | save stdout");
}

TEST(to - write json) {
  auto to_pipeline = pipeline::parse("to stdout write json");
  REQUIRE_NOERROR(to_pipeline);
  REQUIRE_EQUAL(to_pipeline->to_string(), "local print json | save stdout");
}

TEST(to with invalid inputs) {
  REQUIRE_ERROR(pipeline::parse("to json write stdout"));
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
    auto source = unbox(pipeline::parse(definition));
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
  test::reinit_vast_language(*config);
  auto guard = caf::detail::scope_guard([] {
    // The config makes `head` unusable, so we have to restore.
    test::reinit_vast_language(record{});
  });
  auto ops
    = unbox(pipeline::parse("anonymize_urls | aggregate_flows")).unwrap();
  REQUIRE_EQUAL(ops.size(), size_t{3});
  REQUIRE_ERROR(pipeline::parse("aggregate_urls"));
  REQUIRE_ERROR(pipeline::parse("self_recursive"));
  REQUIRE_ERROR(pipeline::parse("mut_recursive1"));
  REQUIRE_ERROR(pipeline::parse("head"));
}

auto execute(pipeline pipe) -> caf::expected<void> {
  for (auto&& result : make_local_executor(std::move(pipe))) {
    if (!result) {
      return result;
    }
  }
  return {};
}

TEST(load_stdin_arguments) {
  auto success = {"load stdin", "load stdin --timeout 1s"};
  auto error = {"load stdin --timeout", "load stdin --timeout nope",
                "load stdin --t1me0ut 1s", "load stdin --timeout 1s 2s"};
  for (auto x : success) {
    MESSAGE(x);
    test::stdin_file_input<"artifacts/inputs/json.txt"> file;
    REQUIRE_NOERROR(
      execute(unbox(pipeline::parse(fmt::format("{} | save stdout", x)))));
  }
  for (auto x : error) {
    MESSAGE(x);
    test::stdin_file_input<"artifacts/inputs/json.txt"> file;
    // This test shows that pipeline parsing still succeeds. This is because
    // arguments are only checked when the actual loader is created, which
    // currently happens during instantiation.
    REQUIRE_ERROR(
      execute(unbox(pipeline::parse(fmt::format("{} | to stdout", x)))));
  }
}

TEST(operator argument parsing and escaping) {
  using namespace std::literals;
  auto a1 = "42\n --abc /**/ 'read' \n \\ \\\\"sv;
  auto a2 = "42 --abc 'read' \\ \\\\"sv; // NOLINT
  auto b1 = "' ' ~/okay/test.txt \"/*\" \t'1 2 3' "sv;
  auto b2 = "' ' ~/okay/test.txt '/*' '1 2 3'"sv;
  auto input = fmt::format("foo {} read xyz {}", a1, b1);
  auto f = input.begin();
  auto result
    = parsers::name_args_opt_keyword_name_args("read").apply(f, input.end());
  REQUIRE(result);
  auto& [first, first_args, opt_second] = *result;
  CHECK_EQUAL(first, "foo");
  CHECK_EQUAL(first_args.size(), size_t{5});
  REQUIRE(opt_second);
  auto& [second, second_args] = *opt_second;
  CHECK_EQUAL(second, "xyz");
  CHECK_EQUAL(second_args.size(), size_t{4});
  CHECK_EQUAL(escape_operator_args(first_args), a2);
  CHECK_EQUAL(escape_operator_args(second_args), b2);
}

TEST(file loader - arguments) {
  auto success = {
    "from -",
    "from file -",
    "from file - read json",
    "from file " VAST_TEST_PATH "artifacts/inputs/json.json",
    "from file " VAST_TEST_PATH "artifacts/inputs/json.json read json",
    "read json from file " VAST_TEST_PATH "artifacts/inputs/json.json",
    "read json from file " VAST_TEST_PATH "artifacts/inputs/json.json --follow",
    "read json from file " VAST_TEST_PATH "artifacts/inputs/json.json -f",
    "read json from file --follow " VAST_TEST_PATH "artifacts/inputs/json.json",
    "read json from file -f " VAST_TEST_PATH "artifacts/inputs/json.json",
    "read json from file -",
    "read json from file stdin",
    "load file " VAST_TEST_PATH "artifacts/inputs/json.json | parse json",
    "load file - | parse json",
    "load file " VAST_TEST_PATH
    "artifacts/inputs/json.json --timeout 2m | parse json"};
  auto error
    = {"from - --timeout",
       "from - --timeout nope",
       "from file stdin --timeout 1s",
       "from - --t1me0ut 2m",
       "from - --timeout 20s 23s",
       "from file",
       "from file --timeout 2m",
       "load file stdin | parse json",
       "load stdin --timeout 1s /home/dakostu/Documents/vast2/version.json",
       "load file " VAST_TEST_PATH
       "artifacts/inputs/json.json --timeout | parse json",
       "load file " VAST_TEST_PATH
       "artifacts/inputs/json.json --timeout wtf | parse json"};
  for (const auto* x : success) {
    MESSAGE(x);
    test::stdin_file_input<"artifacts/inputs/json.json"> file;
    REQUIRE_NOERROR(pipeline::parse(fmt::format("{} | to stdout", x)));
  }
  for (const auto* x : error) {
    MESSAGE(x);
    test::stdin_file_input<"artifacts/inputs/json.json"> file;
    REQUIRE_ERROR(
      execute(unbox(pipeline::parse(fmt::format("{} | to stdout", x)))));
  }
}

} // namespace
} // namespace vast
