//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::first_last {

namespace {

enum class mode {
  first,
  last,
};

template <mode Mode>
class first_last_instance final : public aggregation_instance {
public:
  explicit first_last_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (not caf::holds_alternative<caf::none_t>(result_)) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(arg.type)) {
      return;
    }
    if constexpr (Mode == mode::first) {
      for (int64_t i = 0; i < arg.array->length(); ++i) {
        if (arg.array->IsValid(i)) {
          result_ = materialize(value_at(arg.type, *arg.array, i));
          return;
        }
      }
    } else {
      for (int64_t i = arg.array->length() - 1; i >= 0; --i) {
        if (arg.array->IsValid(i)) {
          result_ = materialize(value_at(arg.type, *arg.array, i));
          return;
        }
      }
    }
  }

  auto get() const -> data override {
    return result_;
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto fb_result = pack(fbb, result_);
    const auto fb_min_max = fbs::aggregation::CreateFirstLast(fbb, fb_result);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::FirstLast>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `{}` aggregation instance",
              Mode == mode::first ? "first" : "last")
        .emit(ctx);
      return;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `{}` aggregation instance",
              Mode == mode::first ? "first" : "last")
        .emit(ctx);
      return;
    }
    if (auto err = unpack(*fb_result, result_)) {
      diagnostic::warning("{}", err)
        .note("failed to restore `{}` aggregation instance",
              Mode == mode::first ? "first" : "last")
        .emit(ctx);
      return;
    }
  }

private:
  ast::expression expr_ = {};
  data result_ = {};
};

template <mode Mode>
class plugin : public virtual aggregation_plugin {
public:
  auto name() const -> std::string override {
    return Mode == mode::first ? "first" : "last";
  };

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<first_last_instance<Mode>>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::first_last

using namespace tenzir::plugins;

TENZIR_REGISTER_PLUGIN(first_last::plugin<first_last::mode::first>)
TENZIR_REGISTER_PLUGIN(first_last::plugin<first_last::mode::last>)
