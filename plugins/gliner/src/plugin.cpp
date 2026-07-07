//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "gliner/model.hpp"

#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view3.hpp>

#include <cmath>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::gliner {
namespace {

struct EntitiesArgs {
  ast::expression field;
  located<std::string> model;
  located<tenzir::data> labels;
  located<double> threshold{0.5, location::unknown};
  ast::field_path into = [] {
    auto expr = ast::expression{
      ast::root_field{ast::identifier{"ai", location::unknown}}};
    expr = ast::expression{ast::field_access{
      std::move(expr),
      location::unknown,
      false,
      ast::identifier{"entities", location::unknown},
    }};
    auto result = ast::field_path::try_from(std::move(expr));
    TENZIR_ASSERT(result);
    return std::move(*result);
  }();
  location operator_location = location::unknown;
};

class Entities final : public Operator<table_slice, table_slice> {
public:
  explicit Entities(EntitiesArgs args) : args_{std::move(args)} {
  }

  Entities(Entities const&) = delete;
  auto operator=(Entities const&) -> Entities& = delete;
  Entities(Entities&&) noexcept = default;
  auto operator=(Entities&&) noexcept -> Entities& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    for (const auto& label : as<list>(args_.labels.inner)) {
      const auto* str = try_as<std::string>(label);
      if (not str or str->empty()) {
        diagnostic::error("`labels` must be a list of non-empty strings")
          .primary(args_.labels)
          .emit(ctx);
        done_ = true;
        co_return;
      }
      labels_.push_back(*str);
    }
    if (labels_.empty()) {
      diagnostic::error("`labels` must not be empty")
        .primary(args_.labels)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto model = Model::make(args_.model.inner);
    if (not model) {
      diagnostic::error(model.error())
        .note("failed to load model")
        .primary(args_.model)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    model_ = std::move(*model);
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or input.rows() == 0) {
      co_return;
    }
    TENZIR_ASSERT(model_);
    auto values = eval(args_.field, input, ctx.dh());
    auto builder = series_builder{};
    auto warned_type = false;
    auto warned_truncation = false;
    for (auto value : values.values3()) {
      if (is<caf::none_t>(value)) {
        builder.null();
        continue;
      }
      const auto* text = try_as<view3<std::string>>(value);
      if (not text) {
        if (not warned_type) {
          diagnostic::warning("`field` must evaluate to `string`")
            .primary(args_.field)
            .emit(ctx);
          warned_type = true;
        }
        builder.null();
        continue;
      }
      auto detected = model_->detect(*text, labels_, args_.threshold.inner);
      if (not detected) {
        diagnostic::warning(detected.error())
          .note("entity detection failed")
          .primary(args_.operator_location)
          .emit(ctx);
        builder.null();
        continue;
      }
      if (detected->truncated and not warned_truncation) {
        diagnostic::warning("input exceeded the model window and was "
                            "truncated")
          .primary(args_.field)
          .note("only entities within the window are reported")
          .emit(ctx);
        warned_truncation = true;
      }
      auto spans = builder.list();
      for (const auto& span : detected->spans) {
        auto row = spans.record();
        row.field("text", span.text);
        row.field("label", span.label);
        row.field("start", span.start);
        row.field("end", span.end);
        row.field("score", span.score);
      }
    }
    auto slice_start = size_t{};
    for (auto&& part : builder.finish()) {
      auto slice_end = slice_start + detail::narrow<size_t>(part.length());
      auto output = assign(args_.into, std::move(part),
                           subslice(input, slice_start, slice_end), ctx.dh());
      co_await push(std::move(output));
      slice_start = slice_end;
    }
    TENZIR_ASSERT(slice_start == detail::narrow<size_t>(input.rows()));
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  EntitiesArgs args_;
  std::vector<std::string> labels_;
  std::unique_ptr<Model> model_;
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "ai::entities";
  }

  auto describe() const -> Description override {
    auto d = Describer<EntitiesArgs, Entities>{};
    d.named("field", &EntitiesArgs::field, "string");
    d.named("model", &EntitiesArgs::model);
    auto labels = d.named("labels", &EntitiesArgs::labels, "list");
    auto threshold = d.named_optional("threshold", &EntitiesArgs::threshold);
    d.named_optional("into", &EntitiesArgs::into);
    d.operator_location(&EntitiesArgs::operator_location);
    d.validate([labels, threshold](DescribeCtx& ctx) -> Empty {
      TRY(auto threshold_value, ctx.get(threshold));
      if (not std::isfinite(threshold_value.inner)
          or threshold_value.inner < 0.0 or threshold_value.inner > 1.0) {
        diagnostic::error("`threshold` must be between 0 and 1")
          .primary(threshold_value)
          .emit(ctx);
      }
      TRY(auto labels_value, ctx.get(labels));
      if (not is<list>(labels_value.inner)) {
        diagnostic::error("`labels` must be a list of strings")
          .primary(labels_value)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::gliner

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gliner::plugin)
