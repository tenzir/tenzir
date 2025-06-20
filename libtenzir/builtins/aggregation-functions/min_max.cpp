//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/data.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
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

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::MinMaxSum>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `{}` aggregation instance",
              Mode == mode::min ? "min" : "max")
        .emit(ctx);
      return;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `{}` aggregation instance",
              Mode == mode::min ? "min" : "max")
        .emit(ctx);
      return;
    }
    auto result = data{};
    if (auto err = unpack(*fb_result, result)) {
      diagnostic::warning("{}", err)
        .note("failed to restore `{}` aggregation instance",
              Mode == mode::min ? "min" : "max")
        .emit(ctx);
      return;
    }
    match(result, [&]<class T>(const T& x) {
      if constexpr (std::is_same_v<T, caf::none_t>) {
        result_.reset();
      } else if constexpr (result_t::can_have<T>) {
        result_.emplace(x);
      } else {
        diagnostic::warning("invalid value for field `result`: `{}`", result)
          .note("failed to restore `{}` aggregation instance",
                Mode == mode::min ? "min" : "max")
          .emit(ctx);
      }
    });
    const auto* fb_type = (*fb)->type();
    if (not fb_type) {
      diagnostic::warning("missing field `type`")
        .note("failed to restore `{}` aggregation instance",
              Mode == mode::min ? "min" : "max")
        .emit(ctx);
      return;
    }
    const auto* fb_type_nested_root = (*fb)->type_nested_root();
    TENZIR_ASSERT(fb_type_nested_root);
    type_ = type{fb->slice(*fb_type_nested_root, *fb_type)};
  }

  auto reset() -> void override {
    type_ = {};
    result_ = {};
  }

private:
  ast::expression expr_ = {};
  type type_ = {};
  std::optional<result_t> result_ = {};
};

template <mode Mode>
class plugin : public virtual aggregation_plugin {
public:
  auto name() const -> std::string override {
    return Mode == mode::min ? "min" : "max";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
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
