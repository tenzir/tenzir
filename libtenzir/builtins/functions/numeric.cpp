//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_time_utils.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/util/tdigest.h>

#include <random>

namespace tenzir::plugins::numeric {

namespace {
class sqrt final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sqrt";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(
      argument_parser2::function("sqrt").add(expr, "<number>").parse(inv, ctx));
    return function_use::make([expr = std::move(expr)](evaluator eval,
                                                       session ctx) -> series {
      auto value = eval(expr);
      auto compute = [&](const arrow::DoubleArray& x) {
        auto b = arrow::DoubleBuilder{};
        check(b.Reserve(x.length()));
        for (auto y : x) {
          if (not y) {
            check(b.AppendNull());
            continue;
          }
          auto z = *y;
          if (z < 0.0) {
            // TODO: Warning?
            check(b.AppendNull());
            continue;
          }
          check(b.Append(std::sqrt(z)));
        }
        return finish(b);
      };
      auto f = detail::overload{
        [&](const arrow::DoubleArray& value) {
          return compute(value);
        },
        [&](const arrow::Int64Array& value) {
          // TODO: Conversation should be automatic (if not
          // part of the kernel).
          auto b = arrow::DoubleBuilder{};
          check(b.Reserve(value.length()));
          for (auto y : value) {
            if (y) {
              check(b.Append(static_cast<double>(*y)));
            } else {
              check(b.AppendNull());
            }
          }
          return compute(*finish(b));
        },
        [&](const arrow::NullArray& value) {
          return series::null(double_type{}, value.length()).array;
        },
        [&](const auto&) {
          // TODO: Think about what we want and generalize this.
          diagnostic::warning("expected `number`, got `{}`", value.type.kind())
            .primary(expr)
            .docs("https://docs.tenzir.com/functions/sqrt")
            .emit(ctx);
          auto b = arrow::DoubleBuilder{};
          check(b.AppendNulls(value.length()));
          return finish(b);
        },
      };
      return {double_type{}, match(*value.array, f)};
    });
  }
};

class random final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.random";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    argument_parser2::function("random").parse(inv, ctx).ignore();
    return function_use::make([](evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto b = arrow::DoubleBuilder{};
      check(b.Reserve(eval.length()));
      auto engine = std::default_random_engine{std::random_device{}()};
      auto dist = std::uniform_real_distribution<double>{0.0, 1.0};
      for (auto i = int64_t{0}; i < eval.length(); ++i) {
        check(b.Append(dist(engine)));
      }
      return {double_type{}, finish(b)};
    });
  }
};

class count_instance final : public aggregation_instance {
public:
  explicit count_instance(std::optional<ast::expression> expr)
    : expr_{std::move(expr)} {
  }

  void update(const table_slice& input, session ctx) override {
    if (not expr_) {
      count_ += detail::narrow<int64_t>(input.rows());
      return;
    }
    auto arg = eval(*expr_, input, ctx);
    count_ += arg.array->length() - arg.array->null_count();
  }

  auto get() const -> data override {
    return count_;
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto fb_count = fbs::aggregation::CreateCount(fbb, count_);
    fbb.Finish(fb_count);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb = flatbuffer<fbs::aggregation::Count>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `count` aggregation instance")
        .emit(ctx);
      return;
    }
    count_ = (*fb)->result();
  }

private:
  std::optional<ast::expression> expr_;
  int64_t count_ = 0;
};

class count final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.count";
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = std::optional<ast::expression>{};
    TRY(
      argument_parser2::function("count").add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<count_instance>(std::move(expr));
  }
};

class quantile_instance final : public aggregation_instance {
public:
  quantile_instance(ast::expression expr, double quantile, uint32_t delta,
                    uint32_t buffer_size)
    : expr_{std::move(expr)}, quantile_{quantile}, digest_{delta, buffer_size} {
  }

  void update(const table_slice& input, session ctx) override {
    if (state_ == state::failed) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    auto f = detail::overload{
      [&]<concepts::one_of<double_type, int64_type, uint64_type> Type>(
        const Type& ty) {
        if (state_ != state::numeric and state_ != state::none) {
          diagnostic::warning("got incompatible types `number` and `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          state_ = state::failed;
          return;
        }
        state_ = state::numeric;
        auto& array = as<type_to_arrow_array_t<Type>>(*arg.array);
        for (auto value : values(ty, array)) {
          if (value) {
            digest_.NanAdd(*value);
          }
        }
      },
      [&](const duration_type& ty) {
        if (state_ != state::dur and state_ != state::none) {
          diagnostic::warning("got incompatible types `duration` and `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          state_ = state::failed;
          return;
        }
        state_ = state::dur;
        for (auto value : values(ty, as<arrow::DurationArray>(*arg.array))) {
          if (value) {
            digest_.Add(value->count());
          }
        }
      },
      [&](const null_type&) {
        // Silently ignore nulls, like we do above.
      },
      [&](const auto&) {
        diagnostic::warning("expected `int`, `uint`, `double` or `duration`, "
                            "got `{}`",
                            arg.type.kind())
          .primary(expr_)
          .emit(ctx);
        state_ = state::failed;
      },
    };
    match(arg.type, f);
  }

  auto get() const -> data override {
    switch (state_) {
      case state::none:
      case state::failed:
        return data{};
      case state::dur:
        return duration{
          static_cast<duration::rep>(digest_.Quantile(quantile_))};
      case state::numeric:
        return digest_.Quantile(quantile_);
    }
    TENZIR_UNREACHABLE();
  }

  auto save() const -> chunk_ptr override {
    return {};
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    TENZIR_UNUSED(chunk);
    diagnostic::warning(
      "restoring `quantile` aggregation instances is not implemented")
      .emit(ctx);
  }

private:
  ast::expression expr_;
  double quantile_;
  enum class state { none, failed, dur, numeric } state_{};
  arrow::internal::TDigest digest_;
};

class quantile final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.quantile";
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    // TODO: Reconsider whether we want a default here. Maybe positional?
    auto quantile_opt = std::optional<located<double>>{};
    auto delta_opt = std::optional<located<int64_t>>{};
    auto buffer_size_opt = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function("quantile")
          .add(expr, "expr")
          .add("q", quantile_opt)
          // TODO: This is a test for hidden parameters.
          .add("_delta", delta_opt)
          .add("_buffer_size", buffer_size_opt)
          .parse(inv, ctx));
    // TODO: Type conversion? Probably not necessary here, but maybe elsewhere.
    // TODO: This is too much manual labor.
    auto quantile = 0.5;
    if (quantile_opt) {
      if (quantile_opt->inner < 0.0 || quantile_opt->inner > 1.0) {
        diagnostic::error("expected quantile to be in [0.0, 1.0]")
          .primary(*quantile_opt)
          .emit(ctx);
      }
      quantile = quantile_opt->inner;
    }
    // TODO: This function probably already exists. If not, it should.
    auto try_narrow = [](int64_t x) -> std::optional<uint32_t> {
      if (0 <= x && x <= std::numeric_limits<uint32_t>::max()) {
        return static_cast<uint32_t>(x);
      }
      return std::nullopt;
    };
    auto delta = uint32_t{100};
    if (delta_opt) {
      if (auto narrowed = try_narrow(delta_opt->inner)) {
        delta = *narrowed;
      } else {
        diagnostic::error("expected delta to fit in a uint32")
          .primary(*delta_opt)
          .emit(ctx);
      }
    }
    auto buffer_size = uint32_t{500};
    if (buffer_size_opt) {
      if (auto narrowed = try_narrow(buffer_size_opt->inner)) {
        buffer_size = *narrowed;
      } else {
        diagnostic::error("expected buffer size to fit in a uint32")
          .primary(*buffer_size_opt)
          .emit(ctx);
      }
    }
    return std::make_unique<quantile_instance>(expr, quantile, delta,
                                               buffer_size);
  }
};

class median final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.median";
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    // TODO: Reconsider whether we want a default here. Maybe positional?
    auto delta_opt = std::optional<located<int64_t>>{};
    auto buffer_size_opt = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function("median")
          .add(expr, "expr")
          // TODO: This is a test for hidden parameters.
          .add("_delta", delta_opt)
          .add("_buffer_size", buffer_size_opt)
          .parse(inv, ctx));
    // TODO: This function probably already exists. If not, it should.
    auto try_narrow = [](int64_t x) -> std::optional<uint32_t> {
      if (0 <= x && x <= std::numeric_limits<uint32_t>::max()) {
        return static_cast<uint32_t>(x);
      }
      return std::nullopt;
    };
    auto delta = uint32_t{100};
    if (delta_opt) {
      if (auto narrowed = try_narrow(delta_opt->inner)) {
        delta = *narrowed;
      } else {
        diagnostic::error("expected delta to fit in a uint32")
          .primary(*delta_opt)
          .emit(ctx);
      }
    }
    auto buffer_size = uint32_t{500};
    if (buffer_size_opt) {
      if (auto narrowed = try_narrow(buffer_size_opt->inner)) {
        buffer_size = *narrowed;
      } else {
        diagnostic::error("expected buffer size to fit in a uint32")
          .primary(*buffer_size_opt)
          .emit(ctx);
      }
    }
    return std::make_unique<quantile_instance>(expr, 0.5, delta, buffer_size);
  }
};

} // namespace

} // namespace tenzir::plugins::numeric

TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::sqrt)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::random)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::count)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::quantile)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::median)
