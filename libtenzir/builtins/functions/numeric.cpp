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
#include <tenzir/detail/tdigest.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <cmath>
#include <memory>
#include <random>
#include <vector>

namespace tenzir::plugins::numeric {

namespace {
class sqrt final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sqrt";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("sqrt")
          .positional("x", expr, "number")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::DoubleBuilder{};
        check(b.Reserve(eval.length()));
        auto append_sqrt = [&](const arrow::DoubleArray& x) {
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
        };
        for (auto value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::DoubleArray& value) {
              append_sqrt(value);
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
              append_sqrt(*finish(b));
            },
            [&](const arrow::NullArray& value) {
              check(b.AppendNulls(value.length()));
            },
            [&](const auto&) {
              // TODO: Think about what we want and generalize this.
              diagnostic::warning("expected `number`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(value.length()));
            },
          };
          match(*value.array, f);
        }
        return series{double_type{}, finish(b)};
      });
  }
};

class random final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.random";
  }

  auto is_deterministic() const -> bool override {
    return false;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
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
  explicit count_instance(std::optional<ast::expression> expr,
                          std::optional<ast::lambda_expr> lambda)
    : expr_{std::move(expr)}, lambda_{std::move(lambda)} {
    if (lambda_) {
      // Aggregation functions do not evaluate their arguments for null values,
      // so we patch the lambda expression from `left => right` to `left =>
      // right if left != null else false`
      lambda_->right = ast::binary_expr{
        ast::binary_expr{
          lambda_->right,
          located{ast::binary_op::if_, location::unknown},
          ast::binary_expr{
            lambda_->left_as_field_path().inner(),
            located{ast::binary_op::neq, location::unknown},
            ast::constant{caf::none, location::unknown},
          },
        },
        located{ast::binary_op::else_, location::unknown},
        ast::constant{false, location::unknown},
      };
    }
  }

  void update(const table_slice& input, session ctx) override {
    const auto subject
      = expr_ ? eval(*expr_, input, ctx) : multi_series{series{input}};
    if (not lambda_) {
      for (const auto& part : subject) {
        count_ += part.array->length() - part.array->null_count();
      }
      return;
    }
    for (const auto& pred : eval(*lambda_, subject, ctx)) {
      const auto typed_pred = pred.as<bool_type>();
      if (not typed_pred) {
        diagnostic::warning("expected `bool`, got `{}`", pred.type.kind())
          .primary(lambda_->right)
          .emit(ctx);
        continue;
      }
      if (typed_pred->array->null_count() > 0) {
        diagnostic::warning("expected `bool`, got `null`")
          .primary(lambda_->right)
          .emit(ctx);
      }
      count_ += typed_pred->array->true_count();
    }
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

  auto reset() -> void override {
    count_ = {};
  }

private:
  std::optional<ast::expression> expr_;
  std::optional<ast::lambda_expr> lambda_;
  int64_t count_ = 0;
};

class count final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.count";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = std::optional<ast::expression>{};
    TRY(argument_parser2::function("count")
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<count_instance>(std::move(expr), std::nullopt);
  }
};

class count_if final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.count_if";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto lambda = ast::lambda_expr{};
    TRY(argument_parser2::function("count_if")
          .positional("x", expr, "any")
          .positional("predicate", lambda, "any => bool")
          .parse(inv, ctx));
    return std::make_unique<count_instance>(std::move(expr), std::move(lambda));
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
    for (auto& arg : eval(expr_, input, ctx)) {
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
              digest_.nan_add(*value);
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
              digest_.add(value->count());
            }
          }
        },
        [&](const null_type&) {
          // Silently ignore nulls, like we do above.
        },
        [&](const auto&) {
          diagnostic::warning("expected `int`, `uint`, `double` or "
                              "`duration`, "
                              "got `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          state_ = state::failed;
        },
      };
      match(arg.type, f);
    }
  }

  auto get() const -> data override {
    switch (state_) {
      case state::none:
      case state::failed:
        return data{};
      case state::dur:
        return duration{
          static_cast<duration::rep>(digest_.quantile(quantile_))};
      case state::numeric:
        return digest_.quantile(quantile_);
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

  auto reset() -> void override {
    quantile_ = {};
    state_ = state::none;
    digest_.reset();
  }

private:
  ast::expression expr_;
  double quantile_;
  enum class state { none, failed, dur, numeric } state_{};
  detail::tdigest digest_;
};

class quantile final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.quantile";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    // TODO: Reconsider whether we want a default here. Maybe positional?
    auto quantile_opt = std::optional<located<double>>{};
    auto delta_opt = std::optional<located<int64_t>>{};
    auto buffer_size_opt = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function("quantile")
          .positional("x", expr, "number|duration")
          .named("q", quantile_opt)
          // TODO: This is a test for hidden parameters.
          .named("_delta", delta_opt)
          .named("_buffer_size", buffer_size_opt)
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

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    // TODO: Reconsider whether we want a default here. Maybe positional?
    auto delta_opt = std::optional<located<int64_t>>{};
    auto buffer_size_opt = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function("median")
          .positional("value", expr, "number|duration")
          // TODO: This is a test for hidden parameters.
          .named("_delta", delta_opt)
          .named("_buffer_size", buffer_size_opt)
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
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::count_if)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::quantile)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::median)
