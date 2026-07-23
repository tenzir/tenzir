//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "gliner/model.hpp"

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/metrics.hpp>
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

#include <chrono>
#include <cmath>
#include <optional>
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
  located<uint64_t> batch_size{16, location::unknown};
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
    metrics_ = make_metric_handler(ctx, type{
                                          "tenzir.metrics.ai_entities",
                                          record_type{
                                            {"events", int64_type{}},
                                            {"chars", int64_type{}},
                                            {"entities", int64_type{}},
                                            {"truncated", int64_type{}},
                                            {"errors", int64_type{}},
                                            {"inference_time", duration_type{}},
                                          },
                                        });
    last_metrics_emit_ = std::chrono::steady_clock::now();
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or input.rows() == 0) {
      co_return;
    }
    TENZIR_ASSERT(model_);
    auto values = eval(args_.field, input, ctx.dh());
    // Pass 1: collect string rows; remember which rows have text.
    auto row_texts = std::vector<std::optional<std::string_view>>{};
    row_texts.reserve(detail::narrow<size_t>(input.rows()));
    auto warned_type = false;
    for (auto value : values.values3()) {
      if (is<caf::none_t>(value)) {
        row_texts.emplace_back(std::nullopt);
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
        row_texts.emplace_back(std::nullopt);
        continue;
      }
      row_texts.emplace_back(*text);
    }
    // Pass 2: run batched inference off the executor thread.
    auto batch = std::vector<std::string_view>{};
    auto batch_rows = std::vector<size_t>{};
    auto row_results
      = std::vector<std::optional<DetectResult>>{row_texts.size()};
    auto inference_error = std::optional<caf::error>{};
    auto flush_batch = [&]() -> Task<void> {
      if (batch.empty()) {
        co_return;
      }
      auto t0 = std::chrono::steady_clock::now();
      auto detected = co_await spawn_blocking([&] {
        return model_->detect_batch(batch, labels_, args_.threshold.inner);
      });
      inference_time_ += std::chrono::steady_clock::now() - t0;
      if (not detected) {
        if (not inference_error) {
          inference_error = std::move(detected.error());
        }
        errors_ += detail::narrow<int64_t>(batch.size());
      } else {
        TENZIR_ASSERT(detected->size() == batch.size());
        for (auto i = size_t{0}; i < batch.size(); ++i) {
          events_ += 1;
          chars_ += detail::narrow<int64_t>(batch[i].size());
          entities_ += detail::narrow<int64_t>((*detected)[i].spans.size());
          truncated_ += (*detected)[i].truncated ? 1 : 0;
          row_results[batch_rows[i]] = std::move((*detected)[i]);
        }
      }
      batch.clear();
      batch_rows.clear();
    };
    auto max_batch = detail::narrow<size_t>(args_.batch_size.inner);
    for (auto row = size_t{0}; row < row_texts.size(); ++row) {
      if (not row_texts[row]) {
        continue;
      }
      batch.push_back(*row_texts[row]);
      batch_rows.push_back(row);
      if (batch.size() >= max_batch) {
        co_await flush_batch();
      }
    }
    co_await flush_batch();
    if (inference_error) {
      diagnostic::warning(*inference_error)
        .note("entity detection failed")
        .primary(args_.operator_location)
        .emit(ctx);
    }
    // Pass 3: build the output series in row order.
    auto builder = series_builder{};
    auto warned_truncation = false;
    for (auto row = size_t{0}; row < row_texts.size(); ++row) {
      if (not row_results[row]) {
        builder.null();
        continue;
      }
      const auto& detected = *row_results[row];
      if (detected.truncated and not warned_truncation) {
        diagnostic::warning("input exceeded the model window and was "
                            "truncated")
          .primary(args_.field)
          .note("only entities within the window are reported")
          .emit(ctx);
        warned_truncation = true;
      }
      auto spans = builder.list();
      for (const auto& span : detected.spans) {
        auto record = spans.record();
        record.field("text", span.text);
        record.field("label", span.label);
        record.field("start", span.start);
        record.field("end", span.end);
        record.field("score", span.score);
      }
    }
    emit_metrics(false);
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
  auto emit_metrics(bool force) -> void {
    auto now = std::chrono::steady_clock::now();
    if (not force and now - last_metrics_emit_ < std::chrono::seconds{1}) {
      return;
    }
    if (events_ == 0 and errors_ == 0) {
      return;
    }
    last_metrics_emit_ = now;
    metrics_.emit({
      {"events", events_},
      {"chars", chars_},
      {"entities", entities_},
      {"truncated", truncated_},
      {"errors", errors_},
      {"inference_time", inference_time_},
    });
    events_ = 0;
    chars_ = 0;
    entities_ = 0;
    truncated_ = 0;
    errors_ = 0;
    inference_time_ = duration::zero();
  }

  EntitiesArgs args_;
  std::vector<std::string> labels_;
  std::unique_ptr<Model> model_;
  metric_handler metrics_;
  std::chrono::steady_clock::time_point last_metrics_emit_{};
  int64_t events_ = 0;
  int64_t chars_ = 0;
  int64_t entities_ = 0;
  int64_t truncated_ = 0;
  int64_t errors_ = 0;
  duration inference_time_ = duration::zero();
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
    auto batch_size = d.named_optional("batch_size", &EntitiesArgs::batch_size);
    d.named_optional("into", &EntitiesArgs::into);
    d.operator_location(&EntitiesArgs::operator_location);
    d.validate([labels, threshold, batch_size](DescribeCtx& ctx) -> Empty {
      TRY(auto threshold_value, ctx.get(threshold));
      if (not std::isfinite(threshold_value.inner)
          or threshold_value.inner < 0.0 or threshold_value.inner > 1.0) {
        diagnostic::error("`threshold` must be between 0 and 1")
          .primary(threshold_value)
          .emit(ctx);
      }
      TRY(auto batch_size_value, ctx.get(batch_size));
      if (batch_size_value.inner == 0) {
        diagnostic::error("`batch_size` must be greater than zero")
          .primary(batch_size_value)
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
