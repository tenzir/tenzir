//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

#include <string>

namespace tenzir::plugins::repeat {

namespace {

auto empty(const table_slice& slice) -> bool {
  return slice.rows() == 0;
}

auto empty(const chunk_ptr& chunk) -> bool {
  return !chunk || chunk->size() == 0;
}

class repeat_operator final : public crtp_operator<repeat_operator> {
public:
  repeat_operator() = default;

  explicit repeat_operator(uint64_t repetitions) : repetitions_{repetitions} {
  }

  template <class Batch>
  auto operator()(generator<Batch> input) const -> generator<Batch> {
    if (repetitions_ == 0) {
      co_return;
    }
    if (repetitions_ == 1) {
      for (auto&& batch : input) {
        co_yield std::move(batch);
      }
      co_return;
    }
    auto cache = std::vector<Batch>{};
    for (auto&& batch : input) {
      if (not empty(batch)) {
        cache.push_back(batch);
      }
      co_yield std::move(batch);
    }
    for (auto i = uint64_t{1}; i < repetitions_; ++i) {
      co_yield {};
      for (const auto& batch : cache) {
        co_yield batch;
      }
    }
  }

  auto name() const -> std::string override {
    return "repeat";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, copy()};
  }

  friend auto inspect(auto& f, repeat_operator& x) -> bool {
    return f.apply(x.repetitions_);
  }

private:
  uint64_t repetitions_;
};

class plugin final : public virtual operator_plugin<repeat_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto repetitions = std::optional<uint64_t>{};
    auto parser = argument_parser{"repeat", "https://docs.tenzir.com/"
                                            "operators/repeat"};
    parser.add(repetitions, "<count>");
    parser.parse(p);
    return std::make_unique<repeat_operator>(
      repetitions.value_or(std::numeric_limits<uint64_t>::max()));
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto count = std::optional<uint64_t>{};
    argument_parser2::operator_("repeat")
      .positional("count", count)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<repeat_operator>(
      count.value_or(std::numeric_limits<uint64_t>::max()));
  }
};

template <class T>
struct description {
  description(auto&&...) {
  }
};

auto positional(auto&&...) -> int {
  return 0;
}

struct repeat_args {
  uint64_t count;

  friend auto inspect(auto& f, repeat_args& x) {
    return f.object(x).fields(f.field("count", x.count));
  }
};

struct repeat_state {
  std::vector<table_slice> slices;

  friend auto inspect(auto& f, repeat_state& x) {
    return f.object(x).fields(f.field("slices", x.slices));
  }
};

#if 0
class plugin2 /*: transformation_operator<repeat_args, repeat_state> */ {
  auto name() -> std::string {
    return "repeat";
  }

  auto describe() -> description<repeat_args> {
    return {
      positional("count", &repeat_args::count),
    };
  }

  struct exec_ctx {
    exec::operator_actor::pointer self;
    repeat_args args;
    base_ctx ctx;
    std::reference_wrapper<repeat_state> state;
  };

  auto
  run(exec_ctx ctx, caf::flow::observable<exec::message<table_slice>> input)
    -> caf::flow::observable<exec::message<table_slice>> {
    return self->make_observable().repeat(std::move(input)).as_observable();
  }
};
#endif

} // namespace

} // namespace tenzir::plugins::repeat

TENZIR_REGISTER_PLUGIN(tenzir::plugins::repeat::plugin)
