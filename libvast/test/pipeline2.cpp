//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline2.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/overload.hpp"
#include "vast/expression.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

namespace vast {
namespace {
struct source final : public logical_operator<void, events> {
  explicit source(std::vector<table_slice> events)
    : events_(std::move(events)) {
  }

  caf::expected<physical_operator<void, events>>
  instantiate(const type& input_schema) noexcept override {
    REQUIRE(!input_schema);
    return [=]() -> generator<table_slice> {
      for (auto& table_slice : events_) {
        co_yield table_slice;
      }
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
  instantiate(const type& input_schema) noexcept override {
    return [=](generator<table_slice> input) -> generator<std::monostate> {
      for (auto&& slice : input) {
        REQUIRE_EQUAL(slice.schema(), input_schema);
        callback_(slice);
      }
      co_return;
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return "source";
  }

  std::function<void(table_slice)> callback_;
};

struct where final : public logical_operator<events, events> {
  explicit where(expression expr) : expr_(std::move(expr)) {
  }

  caf::expected<physical_operator<events, events>>
  instantiate(const type& input_schema) noexcept override {
    auto expr = tailor(expr_, input_schema);
    if (!expr) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("failed to instantiate where "
                                         "operator: {}",
                                         expr.error()));
    }
    return [expr = std::move(*expr)](
             generator<table_slice> input) -> generator<table_slice> {
      for (auto&& slice : input) {
        if (auto result = filter(slice, expr)) {
          co_yield *result;
        }
      }
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("where {}", expr_);
  }

  expression expr_;
};

auto compile_into(std::vector<runtime_physical_operator>& out,
                  const runtime_logical_operator& op) -> caf::expected<void> {
  if (auto p = dynamic_cast<const pipeline2*>(&op)) {
    for (auto& pipeline_op : p->definition()) {
      auto result = compile_into(out, *pipeline_op);
      if (!result)
        return result.error();
    }
  } else {
    auto schema = TODO;
    auto result = op.runtime_instantiate(schema);
    if (result) {
      out.push_back(std::move(*result));
    } else {
      return result.error();
    }
  }
  return {};
}

auto compile(const runtime_logical_operator& op)
  -> caf::expected<std::vector<runtime_physical_operator>> {
  auto out = std::vector<runtime_physical_operator>{};
  auto result = compile_into(out, op);
  if (!result) {
    return result.error();
  }
  return out;
}

auto execute(pipeline2 p) -> caf::expected<void> {
  auto compiled = compile(p);
  if (!compiled) {
    return compiled.error();
  }
  for (auto& op : *compiled) {
    // TODO
  }
  return {};
}

template <class... Ts>
auto make_pipeline(Ts&&... ts) -> pipeline2 {
  auto ops = std::vector<logical_operator_ptr>{};
  (ops.push_back(std::make_unique<Ts>(ts)), ...);
  return unbox(pipeline2::make(std::move(ops)));
}
} // namespace

TEST(pipeline2) {
  auto expr = unbox(to<expression>("#type == \"zeek.conn\""));
  auto p
    = make_pipeline(source{{}}, where{expr}, sink{[](table_slice const&) {}});
}
} // namespace vast
