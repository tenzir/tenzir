//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/checked_math.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/aggregation.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <concepts>
#include <string_view>
#include <variant>

namespace tenzir::plugins::sum {

namespace {

struct SumArgs {
  nova::ValueArgument x;
};

/// The running nova sum: `int`s and `uint`s add with overflow checks (their
/// mix is a `uint`), any `float` turns the sum into a `float`, and `duration`s
/// add among themselves. Nulls are skipped. A type error or an overflow warns
/// and poisons the sum, which then stays `null`. Shared by the accumulator and
/// the list kernel.
class Summation {
public:
  template <class Tag>
  auto add(nova::RowView<Tag> value, location source, diagnostic_handler& dh)
    -> void {
    using namespace nova;
    if (poisoned()) {
      return;
    }
    if constexpr (std::same_as<Tag, Null>) {
      // Nulls neither contribute nor fix the type.
    } else if constexpr (concepts::one_of<Tag, Int, UInt, Float, Duration>) {
      if (not sum_) {
        sum_ = Tag{};
        type_name_ = Type<Tag>::static_name;
      }
      if (not add_value(*value, Type<Tag>::static_name, source, dh)) {
        sum_ = Null{};
      }
    } else {
      diagnostic::warning("expected `int`, `uint`, `float` or `duration`, "
                          "got `{}`",
                          Type<Tag>::static_name)
        .primary(source)
        .emit(dh);
      sum_ = Null{};
    }
  }

  auto poisoned() const -> bool {
    return sum_ and std::holds_alternative<nova::Null>(*sum_);
  }

  auto get() const -> nova::Data {
    if (not sum_) {
      return nova::Data{};
    }
    return sum_->match([](auto value) {
      return nova::Data{value};
    });
  }

private:
  using Sum
    = variant<nova::Null, nova::Int, nova::UInt, nova::Float, nova::Duration>;

  /// Adds one value to the sum. Returns false after warning if the value
  /// cannot be added.
  template <class Value>
  auto add_value(Value value, std::string_view type_name, location source,
                 diagnostic_handler& dh) -> bool {
    using namespace nova;
    auto const incompatible = [&] {
      diagnostic::warning("got incompatible types `{}` and `{}`", type_name_,
                          type_name)
        .primary(source)
        .emit(dh);
      return false;
    };
    // The two lambdas below are split by the value's kind so that each body
    // only contains expressions that are valid for it.
    if constexpr (std::same_as<Value, Duration>) {
      return sum_->match([&]<class Acc>(Acc acc) -> bool {
        if constexpr (std::same_as<Acc, Null>) {
          return false;
        } else if constexpr (std::same_as<Acc, Duration>) {
          auto checked = checked_add(acc.count(), value.count());
          if (not checked) {
            diagnostic::warning("duration overflow").primary(source).emit(dh);
            return false;
          }
          sum_ = Sum{Duration{*checked}};
          return true;
        } else {
          return incompatible();
        }
      });
    } else {
      return sum_->match([&]<class Acc>(Acc acc) -> bool {
        if constexpr (std::same_as<Acc, Null>) {
          return false;
        } else if constexpr (std::same_as<Acc, Duration>) {
          return incompatible();
        } else if constexpr (std::floating_point<Acc>
                             or std::floating_point<Value>) {
          sum_ = Sum{static_cast<Float>(acc) + static_cast<Float>(value)};
          return true;
        } else {
          auto checked = checked_add(acc, value);
          if (not checked) {
            diagnostic::warning("integer overflow").primary(source).emit(dh);
            return false;
          }
          sum_ = Sum{*checked};
          return true;
        }
      });
    }
  }

  /// `None` before the first value, `Null` once poisoned.
  Option<Sum> sum_;
  /// The type that fixed the sum's kind, for the incompatible-types warning.
  std::string_view type_name_;
};

class SumFunction final {
public:
  static auto eval(SumArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::aggregate_lists(args.x, frame,
                                 [&](nova::ListElements const& elements,
                                     nova::ArrayBuilder<nova::Data>& builder) {
                                   auto sum = Summation{};
                                   elements.for_each([&](auto value) {
                                     sum.add(value, args.x.source, frame);
                                   });
                                   nova::append_data(builder, sum.get());
                                 });
  }

  auto update(SumArgs const& args, nova::EvalFrame frame) -> void {
    using namespace nova;
    auto const add = [&]<data_type Tag>(Array<Tag> const& array,
                                        storage::BitMap const& rows) {
      if constexpr (not std::same_as<Tag, Null>) {
        for (auto row : storage::true_bits(rows)) {
          sum_.add(array.get(row), args.x.source, frame);
          if (sum_.poisoned()) {
            return;
          }
        }
      }
    };
    match(
      args.x.data,
      [&]<data_type Tag>(Array<Tag> const& array) {
        add(array, frame.mask());
      },
      [&](UnionArray const& u) {
        for (auto const& field : u.fields()) {
          auto const rows = frame.mask() & field.present;
          if (not rows.any()) {
            continue;
          }
          match(field.data, [&]<data_type Tag>(Array<Tag> const& array) {
            add(array, rows);
          });
        }
      });
  }

  auto get() const -> nova::Data {
    return sum_.get();
  }

  auto reset() -> void {
    sum_ = {};
  }

private:
  Summation sum_;
};

class sum_instance : public aggregation_instance {
public:
  using sum_t = variant<caf::none_t, int64_t, uint64_t, double, duration>;
  sum_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (sum_ and std::holds_alternative<caf::none_t>(sum_.value())) {
      return;
    }
    for (auto& s : eval(expr_, input, ctx)) {
      if (not type_) {
        type_ = s.type;
      }
      const auto warn = [&](const auto&) -> sum_t {
        diagnostic::warning("got incompatible types `{}` and `{}`",
                            type_.kind(), s.type.kind())
          .primary(expr_)
          .emit(ctx);
        return caf::none;
      };
      auto f = detail::overload{
        [](const arrow::NullArray&) {},
        [&]<class T>(const T& array)
          requires integral_type<type_from_arrow_t<T>>
        {
          using Type = T::value_type;
          // Int64 + UInt64 => UInt64
          // * + Double => Double
          if (not sum_) {
            sum_ = Type{};
          }
          sum_ = sum_->match(
            warn,
            [&](std::integral auto& self) -> sum_t {
              auto array_sum = Type{};
              for (auto i = int64_t{}; i < array.length(); ++i) {
                if (array.IsValid(i)) {
                  auto checked = checked_add(array_sum, array.Value(i));
                  if (not checked) {
                    diagnostic::warning("integer overflow")
                      .primary(expr_)
                      .emit(ctx);
                    return caf::none;
                  }
                  array_sum = *checked;
                }
              }
              auto checked = checked_add(self, array_sum);
              if (not checked) {
                diagnostic::warning("integer overflow").primary(expr_).emit(ctx);
                return caf::none;
              }
              return *checked;
            },
            [&](double self) -> sum_t {
              for (auto i = int64_t{}; i < array.length(); ++i) {
                if (array.IsValid(i)) {
                  self += static_cast<double>(array.Value(i));
                }
              }
              return self;
            });
        },
        [&](const arrow::DoubleArray& array) {
          // * => Double
          if (not sum_) {
            sum_ = double{};
          }
          sum_
            = sum_->match(warn, [&](concepts::arithmetic auto& self) -> sum_t {
                auto result = static_cast<double>(self);
                for (auto i = int64_t{}; i < array.length(); ++i) {
                  if (array.IsValid(i)) {
                    result += array.Value(i);
                  }
                }
                return result;
              });
        },
        [&](const arrow::DurationArray& array) {
          if (not sum_) {
            sum_ = duration{};
          }
          sum_ = sum_->match(warn, [&](duration self) -> sum_t {
            for (auto i = int64_t{}; i < array.length(); ++i) {
              if (array.IsValid(i)) {
                auto checked = checked_add(self.count(), array.Value(i));
                if (not checked) {
                  diagnostic::warning("duration overflow")
                    .primary(expr_)
                    .emit(ctx);
                  return caf::none;
                }
                self += duration{array.Value(i)};
              }
            }
            return self;
          });
        },
        [&](const auto&) {
          diagnostic::warning("expected `int`, `uint`, `double` or `duration`, "
                              "got `{}`",
                              s.type.kind())
            .primary(expr_)
            .emit(ctx);
          sum_ = caf::none;
        }};
      match(*s.array, f);
    }
  }

  auto get() const -> data override {
    if (sum_) {
      return sum_->match([](auto sum) {
        return data{sum};
      });
    }
    return data{};
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto result
      = not sum_ ? data{} : sum_->match<data>([](const auto& x) {
          return data{x};
        });
    const auto fb_result = pack(fbb, result);
    const auto type_bytes = as_bytes(type_);
    auto fb_type = fbb.CreateVector(
      reinterpret_cast<const uint8_t*>(type_bytes.data()), type_bytes.size());
    const auto fb_min_max
      = fbs::aggregation::CreateMinMaxSum(fbb, fb_result, fb_type);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    const auto fb
      = flatbuffer<fbs::aggregation::MinMaxSum>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN(
        "failed to restore `sum` aggregation instance: invalid FlatBuffer");
      return false;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      TENZIR_WARN(
        "failed to restore `sum` aggregation instance: missing field `result`");
      return false;
    }
    auto result = data{};
    if (auto err = unpack(*fb_result, result); err.valid()) {
      TENZIR_WARN("failed to restore `sum` aggregation instance: {}", err);
      return false;
    }
    auto ok = true;
    match(result, [&]<class T>(const T& x) {
      if constexpr (std::is_same_v<T, caf::none_t>) {
        sum_.reset();
      } else if constexpr (sum_t::can_have<T>) {
        sum_.emplace(x);
      } else {
        TENZIR_WARN("failed to restore `sum` aggregation instance: invalid "
                    "value for field `result`: `{}`",
                    result);
        ok = false;
      }
    });
    if (not ok) {
      return false;
    }
    const auto* fb_type = (*fb)->type();
    if (not fb_type) {
      TENZIR_WARN(
        "failed to restore `sum` aggregation instance: missing field `type`");
      return false;
    }
    const auto* fb_type_nested_root = (*fb)->type_nested_root();
    TENZIR_ASSERT(fb_type_nested_root);
    type_ = type{fb->slice(*fb_type_nested_root, *fb_type)};
    return true;
  }

  auto reset() -> void override {
    type_ = {};
    sum_ = None{};
  }

private:
  ast::expression expr_;
  type type_;
  Option<sum_t> sum_;
};

class plugin : public virtual aggregation_plugin,
               public virtual nova::AggregationPlugin {
  auto name() const -> std::string override {
    return "sum";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<SumArgs, SumFunction>{};
    d.positional("x", &SumArgs::x, "number|duration");
    return std::move(d).finish();
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("sum")
          .positional("x", expr, "number|duration")
          .parse(inv, ctx));
    return std::make_unique<sum_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::sum

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sum::plugin)
