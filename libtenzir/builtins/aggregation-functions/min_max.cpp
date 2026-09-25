//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/data.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/aggregation.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/api.h>
#include <arrow/compute/api.h>

namespace tenzir::plugins::min_max {

namespace {

enum class mode {
  max,
  min,
};

template <mode Mode>
class min_max_instance final : public aggregation_instance {
public:
  using result_t
    = variant<caf::none_t, int64_t, uint64_t, double, duration, time>;

  explicit min_max_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (result_ and std::holds_alternative<caf::none_t>(result_.value())) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      if (not type_) {
        type_ = arg.type;
      }
      const auto warn = [&](const auto&) -> result_t {
        diagnostic::warning("got incompatible types `{}` and `{}`",
                            type_.kind(), arg.type.kind())
          .primary(expr_)
          .emit(ctx);
        return caf::none;
      };
      // TODO: Matching on type of max_ might be better to reduce function calls
      auto f = detail::overload{
        [](const arrow::NullArray&) {},
        [&]<class T>(const T& array)
          requires numeric_type<type_from_arrow_t<T>>
                   {
                     for (auto i = int64_t{}; i < array.length(); ++i) {
                       if (array.IsValid(i)) {
                         const auto val = array.Value(i);
                         if (not result_) {
                           result_ = val;
                           continue;
                         }
                         result_ = result_->match(
                           warn,
                           [&](std::integral auto& self) -> result_t {
                             if constexpr (std::same_as<T, arrow::DoubleArray>) {
                               return Mode == mode::min
                                        ? std::min(static_cast<double>(self),
                                                   val)
                                        : std::max(static_cast<double>(self),
                                                   val);
                             } else {
                               if (Mode == mode::min
                                     ? std::cmp_less(val, self)
                                     : std::cmp_greater(val, self)) {
                                 return val;
                               }
                               return self;
                             }
                           },
                           [&](double self) -> result_t {
                             return Mode == mode::min
                                      ? std::min(self, static_cast<double>(val))
                                      : std::max(self,
                                                 static_cast<double>(val));
                           });
                         if (std::holds_alternative<caf::none_t>(
                               result_.value())) {
                           return;
                         }
                       }
                     }
                   },
                   [&]<class T>(const T& array)
                     requires concepts::one_of<type_from_arrow_t<T>,
                                               duration_type, time_type>
        {
          using Ty = type_from_arrow_t<T>;
          for (const auto& val : values(Ty{}, array)) {
            if (val) {
              if (not result_) {
                result_ = val;
              }
              result_ = result_->match(
                warn, [&](type_to_data_t<Ty> self) -> result_t {
                  return Mode == mode::min ? std::min(self, val.value())
                                           : std::max(self, val.value());
                });
              if (std::holds_alternative<caf::none_t>(result_.value())) {
                return;
              }
            }
          }
        },
        [&](const auto&) {
          diagnostic::warning("expected types `int`, `uint`, `double`, "
                              "`duration`, or `time`, but got `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          result_ = caf::none;
        }};
      match(*arg.array, f);
    }
  }

  auto get() const -> data override {
    if (result_) {
      return result_->match([](auto result) {
        return data{result};
      });
    }
    return {};
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto result
      = not result_ ? data{} : result_->match<data>([](const auto& x) {
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
    constexpr auto name = Mode == mode::min ? "min" : "max";
    const auto fb
      = flatbuffer<fbs::aggregation::MinMaxSum>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: invalid "
                  "FlatBuffer",
                  name);
      return false;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: missing field "
                  "`result`",
                  name);
      return false;
    }
    auto result = data{};
    if (auto err = unpack(*fb_result, result); err.valid()) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: {}", name, err);
      return false;
    }
    auto ok = true;
    match(result, [&]<class T>(const T& x) {
      if constexpr (std::is_same_v<T, caf::none_t>) {
        result_.reset();
      } else if constexpr (result_t::can_have<T>) {
        result_.emplace(x);
      } else {
        TENZIR_WARN("failed to restore `{}` aggregation instance: invalid "
                    "value for field `result`: `{}`",
                    name, result);
        ok = false;
      }
    });
    if (not ok) {
      return false;
    }
    const auto* fb_type = (*fb)->type();
    if (not fb_type) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: missing field "
                  "`type`",
                  name);
      return false;
    }
    const auto* fb_type_nested_root = (*fb)->type_nested_root();
    TENZIR_ASSERT(fb_type_nested_root);
    type_ = type{fb->slice(*fb_type_nested_root, *fb_type)};
    return true;
  }

  auto reset() -> void override {
    type_ = {};
    result_ = None{};
  }

private:
  ast::expression expr_ = {};
  type type_ = {};
  Option<result_t> result_ = None{};
};

struct MinMaxArgs {
  nova::ValueArgument x;
};

/// The running nova `min` or `max`. Numbers compare across `int`, `uint`,
/// and `float`, where a `float` on either side makes the result a `float`;
/// `duration`s and `time`s only compare among themselves. Nulls are skipped.
/// A type error warns and poisons the result, which then stays `null`.
template <mode Mode>
class Extremum {
public:
  template <class Tag>
  auto add(nova::RowView<Tag> view, location source, diagnostic_handler& dh)
    -> void {
    using namespace nova;
    if (poisoned()) {
      return;
    }
    if constexpr (std::same_as<Tag, Null>) {
      // Nulls neither contribute nor fix the type.
    } else if constexpr (concepts::one_of<Tag, Int, UInt, Float, Duration,
                                          Time>) {
      auto const value = Tag{*view};
      if (not result_) {
        result_ = Value{value};
        type_name_ = Type<Tag>::static_name;
        return;
      }
      auto const incompatible = [&] {
        diagnostic::warning("got incompatible types `{}` and `{}`", type_name_,
                            Type<Tag>::static_name)
          .primary(source)
          .emit(dh);
        return Value{Null{}};
      };
      // The two lambdas below are split by the value's kind so that each body
      // only contains expressions that are valid for it.
      if constexpr (concepts::one_of<Tag, Int, UInt, Float>) {
        result_ = result_->match([&]<class Acc>(Acc acc) -> Value {
          if constexpr (not concepts::one_of<Acc, Int, UInt, Float>) {
            return incompatible();
          } else if constexpr (std::floating_point<Acc>
                               or std::floating_point<Tag>) {
            auto const lhs = static_cast<Float>(acc);
            auto const rhs = static_cast<Float>(value);
            return Mode == mode::min ? std::min(lhs, rhs) : std::max(lhs, rhs);
          } else {
            auto const replace = Mode == mode::min
                                   ? std::cmp_less(value, acc)
                                   : std::cmp_greater(value, acc);
            return replace ? Value{value} : Value{acc};
          }
        });
      } else {
        result_ = result_->match([&]<class Acc>(Acc acc) -> Value {
          if constexpr (std::same_as<Acc, Tag>) {
            return Mode == mode::min ? std::min(acc, value)
                                     : std::max(acc, value);
          } else {
            return incompatible();
          }
        });
      }
    } else {
      diagnostic::warning("expected `int`, `uint`, `float`, `duration`, or "
                          "`time`, got `{}`",
                          Type<Tag>::static_name)
        .primary(source)
        .emit(dh);
      result_ = Null{};
    }
  }

  auto poisoned() const -> bool {
    return result_ and std::holds_alternative<nova::Null>(*result_);
  }

  auto get() const -> nova::Data {
    if (not result_) {
      return nova::Data{};
    }
    return result_->match([](auto value) {
      return nova::Data{value};
    });
  }

private:
  using Value = variant<nova::Null, nova::Int, nova::UInt, nova::Float,
                        nova::Duration, nova::Time>;

  /// `None` before the first value, `Null` once poisoned.
  Option<Value> result_;
  /// The type of the first value, for the incompatible-types warning.
  std::string_view type_name_;
};

template <mode Mode>
class MinMaxFunction final {
public:
  static auto eval(MinMaxArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::aggregate_lists(args.x, frame,
                                 [&](nova::ListElements const& elements,
                                     nova::ArrayBuilder<nova::Data>& builder) {
                                   auto extremum = Extremum<Mode>{};
                                   elements.for_each([&](auto value) {
                                     extremum.add(value, args.x.source, frame);
                                   });
                                   nova::append_data(builder, extremum.get());
                                 });
  }

  auto update(MinMaxArgs const& args, nova::EvalFrame frame) -> void {
    using namespace nova;
    auto const add = [&]<data_type Tag>(Array<Tag> const& array,
                                        storage::BitMap const& rows) {
      if constexpr (not std::same_as<Tag, Null>) {
        for (auto row : storage::true_bits(rows)) {
          extremum_.add(array.get(row), args.x.source, frame);
          if (extremum_.poisoned()) {
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
          if (rows.any() and not extremum_.poisoned()) {
            match(field.data, [&]<data_type Tag>(Array<Tag> const& array) {
              add(array, rows);
            });
          }
        }
      });
  }

  auto get() const -> nova::Data {
    return extremum_.get();
  }

  auto reset() -> void {
    extremum_ = {};
  }

private:
  Extremum<Mode> extremum_;
};

template <mode Mode>
class plugin : public virtual aggregation_plugin,
               public virtual nova::AggregationPlugin {
public:
  auto name() const -> std::string override {
    return Mode == mode::min ? "min" : "max";
  };

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<MinMaxArgs, MinMaxFunction<Mode>>{};
    d.positional("x", &MinMaxArgs::x, "number|duration|time");
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number|duration|time")
          .parse(inv, ctx));
    return std::make_unique<min_max_instance<Mode>>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::min_max

using namespace tenzir::plugins;

TENZIR_REGISTER_PLUGIN(min_max::plugin<min_max::mode::min>)
TENZIR_REGISTER_PLUGIN(min_max::plugin<min_max::mode::max>)
