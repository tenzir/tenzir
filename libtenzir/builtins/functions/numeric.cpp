//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/arrow_utils.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/util/tdigest.h>
#include <caf/detail/is_one_of.hpp>

#include <random>

namespace tenzir::plugins::numeric {

namespace {

class round final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.round";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = located<series>{};
    auto success
      = function_argument_parser{"round"}.add(arg, "<number>").parse(inv, dh);
    if (not success) {
      return series::null(int64_type{}, inv.length);
    }
    auto f = detail::overload{
      [](const arrow::Int64Array& arg) {
        return std::make_shared<arrow::Int64Array>(arg.data());
      },
      [&](const arrow::DoubleArray& arg) {
        auto b = arrow::Int64Builder{};
        check(b.Reserve(inv.length));
        for (auto row = int64_t{0}; row < inv.length; ++row) {
          if (arg.IsNull(row)) {
            check(b.AppendNull());
          } else {
            // TODO: NaN, inf, ...
            auto result = std::llround(arg.Value(row));
            check(b.Append(result));
          }
        }
        return finish(b);
      },
      [&](const auto&) -> std::shared_ptr<arrow::Int64Array> {
        diagnostic::warning("`round` expects `int64` or `double`, got `{}`",
                            arg.inner.type.kind())
          // TODO: Wrong location.
          .primary(inv.self)
          .emit(dh);
        auto b = arrow::Int64Builder{};
        check(b.AppendNulls(detail::narrow<int64_t>(inv.length)));
        return finish(b);
      },
    };
    return series{int64_type{}, caf::visit(f, *arg.inner.array)};
  }
};

class sqrt final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sqrt";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = variant<basic_series<double_type>, basic_series<int64_type>>{};
    auto success
      = function_argument_parser{"sqrt"}.add(arg, "value").parse(inv, dh);
    if (not success) {
      return series::null(double_type{}, inv.length);
    }
    auto compute = [&](const arrow::DoubleArray& x) {
      auto b = arrow::DoubleBuilder{};
      check(b.Reserve(inv.length));
#if 0
      // TODO: This is probably UB.
      auto alloc = arrow::AllocateBuffer(x.length() * sizeof(double));
      TENZIR_ASSERT(alloc.ok());
      auto buffer = std::move(*alloc);
      TENZIR_ASSERT(buffer);
      auto target = new (buffer->mutable_data()) double[x.length()];
      auto begin = x.raw_values();
      auto end = begin + x.length();
      auto null_alloc = arrow::AllocateBuffer((x.length() + 7) / 8);
      TENZIR_ASSERT(null_alloc.ok());
      auto null_buffer = std::move(*null_alloc);
      TENZIR_ASSERT(null_buffer);
      auto null_target = null_buffer->mutable_data();
      if (auto nulls = x.null_bitmap()) {
        std::memcpy(null_target, nulls->data(), nulls->size());
      } else {
        std::memset(null_target, 0xFF, (x.length() + 7) / 8);
      }
      while (begin != end) {
        auto val = *begin;
        if (val < 0.0) [[unlikely]] {
          auto mask = 0xFF ^ (0x01 << ((begin - end) % 8));
          null_target[(begin - end) / 8] &= mask;
          // TODO: Emit warning/error?
        }
        *target = std::sqrt(*begin);
        ++begin;
        ++target;
      }
      return std::make_shared<arrow::DoubleArray>(x.length(), std::move(buffer),
                                                  std::move(null_buffer));
#else
      // TODO
      for (auto y : x) {
        if (not y) {
          // TODO: Warning?
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
#endif
      return finish(b);
    };
    auto result = arg.match(
      [&](basic_series<double_type>& x) {
        return compute(*x.array);
      },
      [&](basic_series<int64_type>& x) {
        // TODO: Conversation should be automatic (if not part of the kernel).
        auto b = arrow::DoubleBuilder{};
        check(b.Reserve(x.length()));
        for (auto y : *x.array) {
          if (y) {
            check(b.Append(static_cast<double>(*y)));
          } else {
            check(b.AppendNull());
          }
        }
        return compute(*finish(b));
      });
    return series{double_type{}, std::move(result)};
  }
};

class random final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.random";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto success = function_argument_parser{"random"}.parse(inv, dh);
    if (not success) {
      return series::null(double_type{}, inv.length);
    }
    if (not inv.args.empty()) {
      diagnostic::error("`random` expects no arguments")
        .primary(inv.self)
        .emit(dh);
    }
    auto b = arrow::DoubleBuilder{};
    check(b.Reserve(inv.length));
    auto engine = std::default_random_engine{std::random_device{}()};
    auto dist = std::uniform_real_distribution<double>{0.0, 1.0};
    for (auto i = int64_t{0}; i < inv.length; ++i) {
      check(b.Append(dist(engine)));
    }
    return {double_type{}, finish(b)};
  }
};

class sum_instance final : public aggregation_instance {
public:
  explicit sum_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  using sum_t = variant<int64_t, uint64_t, double>;

  void update(const table_slice& input, session ctx) override {
    // TODO: values.type
    auto arg = eval(expr_, input, ctx);
    auto f = detail::overload{
      [&](const arrow::Int64Array& array) {
        // Double => Double
        // UInt64 => Int64
        // Int64 => Int64
        sum_ = sum_.match(
          [&](double sum) -> sum_t {
            for (auto row = int64_t{0}; row < array.length(); ++row) {
              if (array.IsNull(row)) {
                // TODO: What do we do here?
              } else {
                sum += static_cast<double>(array.Value(row));
              }
            }
            return sum;
          },
          [&](auto previous) -> sum_t {
            static_assert(caf::detail::is_one_of<decltype(previous), int64_t,
                                                 uint64_t>::value);
            // TODO: Check narrowing.
            auto sum = static_cast<int64_t>(previous);
            for (auto row = int64_t{0}; row < array.length(); ++row) {
              if (array.IsNull(row)) {
                // TODO: What do we do here?
              } else {
                sum += array.Value(row);
              }
            }
            return sum;
          });
      },
      [&](const arrow::UInt64Array& array) {
        // Double => Double
        // UInt64 => UInt64Array
        // Int64 => Int64
        TENZIR_UNUSED(array);
        TENZIR_TODO();
      },
      [&](const arrow::DoubleArray& array) {
        // * => Double
        auto sum = sum_.match([](auto sum) {
          return static_cast<double>(sum);
        });
        for (auto row = int64_t{0}; row < array.length(); ++row) {
          if (array.IsNull(row)) {
            // TODO: What do we do here?
          } else {
            sum += array.Value(row);
          }
        }
        sum_ = sum;
      },
      [&](const arrow::NullArray&) {
        // do nothing
      },
      [&](auto&) {
        diagnostic::warning("expected integer or double, but got {}",
                            arg.type.kind())
          .primary(expr_)
          .emit(ctx);
      },
    };
    caf::visit(f, *arg.array);
  }

  auto finish() -> data override {
    return sum_.match([](auto sum) {
      return data{sum};
    });
  }

private:
  ast::expression expr_;
  sum_t sum_ = uint64_t{0};
};

class sum final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sum";
  }

  auto make_aggregation(ast::function_call call, session ctx) const
    -> std::unique_ptr<aggregation_instance> override {
    if (call.args.size() != 1) {
      diagnostic::error("expected exactly one argument, got {}",
                        call.args.size())
        .primary(call)
        .emit(ctx);
      return nullptr;
    }
    return std::make_unique<sum_instance>(std::move(call.args[0]));
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

  auto finish() -> data override {
    return count_;
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

  auto make_aggregation(ast::function_call call, session ctx) const
    -> std::unique_ptr<aggregation_instance> override {
    auto arg = std::optional<ast::expression>{};
    argument_parser2::fn("count").add(arg, "<expr>").parse(call, ctx);
    return std::make_unique<count_instance>(std::move(arg));
  }
};

namespace {

template <class... Ts>
concept one_of = caf::detail::is_one_of<Ts...>::value;

} // namespace

class quantile_instance final : public aggregation_instance {
public:
  quantile_instance(ast::expression expr, double quantile)
    : expr_{std::move(expr)}, quantile_{quantile} {
  }

  void update(const table_slice& input, session ctx) override {
    auto arg = eval(expr_, input, ctx);
    auto f = detail::overload{
      [&]<one_of<double_type, int64_type, uint64_type> Type>(const Type& ty) {
        auto& array = caf::get<type_to_arrow_array_t<Type>>(*arg.array);
        for (auto value : values(ty, array)) {
          if (value) {
            digest_.NanAdd(*value);
          }
        }
      },
      [&](const null_type&) {
        // Silently ignore nulls, like we do above.
      },
      [&](const auto&) {
        diagnostic::warning("expected number, got `{}`", arg.type.kind())
          .primary(expr_)
          .emit(ctx);
      },
    };
    caf::visit(f, arg.type);
  }

  auto finish() -> data override {
    return digest_.Quantile(quantile_);
  }

private:
  ast::expression expr_;
  double quantile_;
  arrow::internal::TDigest digest_;
};

class quantile final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.quantile";
  }

  auto make_aggregation(ast::function_call x, session ctx) const
    -> std::unique_ptr<aggregation_instance> override {
    // TODO: Improve.
    if (x.args.size() != 2) {
      diagnostic::error("expected exactly two arguments, got {}", x.args.size())
        .primary(x)
        .usage("quantile(<expr>, <quantile>)")
        .docs("https://docs.tenzir.com/functions/quantile")
        .emit(ctx);
      return nullptr;
    }
    auto& expr = x.args[0];
    auto& quantile_expr = x.args[1];
    auto quantile_opt = const_eval(quantile_expr, ctx);
    if (not quantile_opt) {
      return nullptr;
    }
    // TODO: Type conversion? Probably not necessary here, but maybe elsewhere.
    auto quantile = caf::get_if<double>(&*quantile_opt);
    if (not quantile) {
      diagnostic::error("expected double, got TODO")
        .primary(quantile_expr)
        .emit(ctx);
      return nullptr;
    }
    if (*quantile < 0.0 || *quantile > 1.0) {
      diagnostic::error("expected quantile to be in [0.0, 1.0]")
        .primary(quantile_expr)
        .emit(ctx);
      return nullptr;
    }
    return std::make_unique<quantile_instance>(std::move(expr), *quantile);
  }
};

} // namespace

} // namespace tenzir::plugins::numeric

TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::round)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::sqrt)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::random)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::sum)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::count)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::quantile)
