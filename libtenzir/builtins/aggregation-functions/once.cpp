//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::once {

namespace {

class once_instance final : public aggregation_instance {
public:
  explicit once_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    const auto warn = [&] {
      diagnostic::warning("`once` received more than one event")
        .primary(expr_)
        .hint("use an aggregation function to aggregate multiple values")
        .emit(ctx);
    };
    if (done_) {
      warn();
      return;
    }
    auto arg = eval(expr_, input, ctx);
    TENZIR_ASSERT(arg.length() != 0);
    if (arg.length() > 1) {
      warn();
    }
    result_ = materialize(arg.value_at(0));
    done_ = true;
  }

  auto get() const -> data override {
    return result_;
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto fb_result = pack(fbb, result_);
    const auto aggr = fbs::aggregation::CreateOnce(fbb, done_, fb_result);
    fbb.Finish(aggr);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb = flatbuffer<fbs::aggregation::Once>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `once` aggregation instance")
        .emit(ctx);
      return;
    }
    done_ = (*fb)->done();
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `once` aggregation instance")
        .emit(ctx);
      return;
    }
    if (auto err = unpack(*fb_result, result_); err.valid()) {
      diagnostic::warning("{}", err)
        .note("failed to restore `once` aggregation instance")
        .emit(ctx);
      return;
    }
  }

  auto reset() -> void override {
    done_ = false;
    result_ = {};
  }

private:
  ast::expression expr_;
  bool done_{false};
  data result_;
};

class plugin : public virtual aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "once";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<once_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::once

TENZIR_REGISTER_PLUGIN(tenzir::plugins::once::plugin)
