//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/logical_pipeline.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/overload.hpp"
#include "vast/expression.hpp"
#include "vast/operator.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <caf/detail/scope_guard.hpp>
#include <caf/test/dsl.hpp>

namespace vast {
namespace {

struct command final : public logical_operator<void, void> {
  caf::expected<physical_operator<void, void>>
  make_physical_operator(const type& input_schema,
                         operator_control_plane&) noexcept override {
    REQUIRE(!input_schema);
    return [=]() -> generator<std::monostate> {
      MESSAGE("hello, world!");
      co_return;
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return "command";
  }
};

struct source final : public logical_operator<void, events> {
  explicit source(std::vector<table_slice> events)
    : events_(std::move(events)) {
  }

  caf::expected<physical_operator<void, events>>
  make_physical_operator(const type& input_schema,
                         operator_control_plane&) noexcept override {
    REQUIRE(!input_schema);
    return [*this]() -> generator<table_slice> {
      auto guard = caf::detail::scope_guard{[] {
        MESSAGE("source destroy");
      }};
      for (auto& table_slice : events_) {
        MESSAGE("source yield");
        co_yield table_slice;
      }
      MESSAGE("source return");
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return "source";
  }

  std::vector<table_slice> events_;
};

struct sink final : public logical_operator<events, void> {
  explicit sink(std::function<void(table_slice)> callback)
    : callback_(std::move(callback)) {
  }

  caf::expected<physical_operator<events, void>>
  make_physical_operator(const type& input_schema,
                         operator_control_plane&) noexcept override {
    return [this, input_schema](
             generator<table_slice> input) -> generator<std::monostate> {
      auto guard = caf::detail::scope_guard{[] {
        MESSAGE("sink destroy");
      }};
      for (auto&& slice : input) {
        if (slice.rows() != 0) {
          REQUIRE_EQUAL(slice.schema(), input_schema);
          MESSAGE("sink callback");
          callback_(slice);
        }
        MESSAGE("sink yield");
        co_yield {};
      }
      MESSAGE("sink return");
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return "sink";
  }

  std::function<void(table_slice)> callback_;
};

struct where final : public logical_operator<events, events> {
  explicit where(expression expr) : expr_(std::move(expr)) {
  }

  caf::expected<physical_operator<events, events>>
  make_physical_operator(const type& input_schema,
                         operator_control_plane&) noexcept override {
    auto expr = tailor(expr_, input_schema);
    if (!expr) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("failed to instantiate where "
                                         "operator: {}",
                                         expr.error()));
    }
    return [expr = std::move(*expr)](
             generator<table_slice> input) mutable -> generator<table_slice> {
      auto guard = caf::detail::scope_guard{[] {
        MESSAGE("where destroy");
      }};
      for (auto&& slice : input) {
        // TODO: Adjust `filter` to make this check unnecessary.
        if (auto result = filter(slice, expr)) {
          MESSAGE("where yield result");
          co_yield *result;
        } else {
          MESSAGE("where yield no result");
          co_yield {};
        }
      }
      MESSAGE("where return");
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("where {}", expr_);
  }

  expression expr_;
};

template <class... Ts>
auto make_pipeline(Ts&&... ts) -> logical_pipeline {
  auto ops = std::vector<logical_operator_ptr>{};
  (ops.push_back(std::make_unique<Ts>(ts)), ...);
  return unbox(logical_pipeline::make(std::move(ops)));
}

struct fixture : fixtures::events {};

} // namespace

TEST(command) {
  auto put = make_pipeline(command{});
  for (auto&& x : make_local_executor(std::move(put))) {
    REQUIRE_NOERROR(x);
  }
}

FIXTURE_SCOPE(pipeline_fixture, fixture)

TEST(source | where #type == "zeek.conn" | sink) {
  auto put = make_local_executor(make_pipeline(
    source{{head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1),
            head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1)}},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    sink{[](const table_slice&) {
      MESSAGE("---- sink ----");
      return;
    }}));
  for (auto&& x : put) {
    REQUIRE_NOERROR(x);
  }
}

FIXTURE_SCOPE_END()

} // namespace vast
