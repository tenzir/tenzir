//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/aggregation.hpp>
#include <tenzir/plugin/register.hpp>
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
    result_ = materialize(arg.view3_at(0));
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

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    const auto fb = flatbuffer<fbs::aggregation::Once>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN(
        "failed to restore `once` aggregation instance: invalid FlatBuffer");
      return false;
    }
    done_ = (*fb)->done();
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      TENZIR_WARN("failed to restore `once` aggregation instance: missing "
                  "field `result`");
      return false;
    }
    if (auto err = unpack(*fb_result, result_); err.valid()) {
      TENZIR_WARN("failed to restore `once` aggregation instance: {}", err);
      return false;
    }
    return true;
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

struct OnceArgs {
  nova::ValueArgument x;
};

auto warn_more_than_once(location source, diagnostic_handler& dh) -> void {
  diagnostic::warning("`once` received more than one event")
    .primary(source)
    .hint("use an aggregation function to aggregate multiple values")
    .emit(dh);
}

/// The nova `once`: the value of the only event, warning if there are more.
class OnceFunction final {
public:
  static auto eval(OnceArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::aggregate_lists(
      args.x, frame,
      [&](nova::ListElements const& elements,
          nova::ArrayBuilder<nova::Data>& builder) {
        if (elements.empty()) {
          builder.null();
          return;
        }
        if (elements.size() > 1) {
          warn_more_than_once(args.x.source, frame);
        }
        nova::append_row(builder, elements.values.get(elements.begin));
      });
  }

  auto update(OnceArgs const& args, nova::EvalFrame frame) -> void {
    if (result_ or frame.mask().true_count() > 1) {
      warn_more_than_once(args.x.source, frame);
    }
    if (result_) {
      return;
    }
    auto const first = *nova::storage::true_bits(frame.mask()).begin();
    result_ = nova::to_data(args.x.data.get(first));
  }

  auto get() const -> nova::Data {
    return result_ ? *result_ : nova::Data{};
  }

  auto reset() -> void {
    result_ = None{};
  }

private:
  Option<nova::Data> result_;
};

class plugin : public virtual aggregation_plugin,
               public virtual nova::AggregationPlugin {
public:
  auto name() const -> std::string override {
    return "once";
  };

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<OnceArgs, OnceFunction>{};
    d.positional("x", &OnceArgs::x, "any");
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
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
