//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/table_slice_builder.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/type.h>

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
    // TODO: Generalize this optimization.
    if constexpr (std::same_as<Batch, table_slice>) {
      if (cache.size() == 1) {
        auto& batch = static_cast<table_slice&>(cache[0]);
        auto schema = batch.schema();
        auto builder = schema.make_arrow_builder(arrow::default_memory_pool());
        auto array = to_record_batch(batch)->ToStructArray().ValueOrDie();
        for (auto i = uint64_t{1}; i < repetitions_; ++i) {
          auto status
            = append_array_slice(*builder, schema, *array, 0, array->length());
          TENZIR_ASSERT(status.ok());
        }
        auto output = builder->Finish().ValueOrDie();
        auto* cast = dynamic_cast<arrow::StructArray*>(output.get());
        TENZIR_ASSERT(cast);
        auto arrow_schema = schema.to_arrow_schema();
        auto output_rb = arrow::RecordBatch::Make(
          std::move(arrow_schema), cast->length(), cast->fields());
        co_yield table_slice{output_rb, std::move(schema)};
        co_return;
      }
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

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    using namespace tql2;
    if (inv.args.size() != 1) {
      diagnostic::error("`repeat` expects exactly one argument, got {}",
                        inv.args.size())
        .primary(inv.self.get_location())
        .emit(ctx);
      return nullptr;
    }
    auto count = inv.args[0].match(
      [](ast::literal& x) {
        return x.value.match(
          [](int64_t x) -> std::optional<int64_t> {
            return x;
          },
          [](auto&) -> std::optional<int64_t> {
            return std::nullopt;
          });
      },
      [](auto&) -> std::optional<int64_t> {
        return std::nullopt;
      });
    if (not count) {
      diagnostic::error("expected integer")
        .primary(inv.args[0].get_location())
        .emit(ctx);
    }
    return std::make_unique<repeat_operator>(*count);
  }
};

} // namespace

} // namespace tenzir::plugins::repeat

TENZIR_REGISTER_PLUGIN(tenzir::plugins::repeat::plugin)
