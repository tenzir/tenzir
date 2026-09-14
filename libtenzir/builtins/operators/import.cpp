//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/async/fetch_node.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/async/metrics.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>
#include <caf/actor_registry.hpp>

#include <deque>

namespace tenzir::plugins::import {

namespace {

struct ImportArgs {
  // No arguments.
};

class Import final : public Operator<table_slice, void> {
public:
  explicit Import(ImportArgs /*args*/) {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    write_bytes_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "import",
      },
      MetricsDirection::write, MetricsVisibility::internal_,
      MetricsUnit::bytes);
    write_events_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "import",
      },
      MetricsDirection::write, MetricsVisibility::internal_,
      MetricsUnit::events);
    import_metrics_ = make_metric_handler(ctx, {
                                                 "tenzir.metrics.import",
                                                 record_type{
                                                   {"schema", string_type{}},
                                                   {"schema_id", string_type{}},
                                                   {"events", uint64_type{}},
                                                 },
                                               });
    auto node = co_await fetch_node(ctx.actor_system(), ctx.dh());
    if (not node) {
      co_return;
    }
    auto components = co_await async_mail(atom::get_v, atom::label_v,
                                          std::vector<std::string>{"importer"})
                        .request(*node);
    if (not components) {
      diagnostic::error(components.error())
        .note("failed to retrieve importer component")
        .emit(ctx);
      co_return;
    }
    if (components->size() != 1) {
      diagnostic::error("expected exactly one importer component, got {}",
                        components->size())
        .emit(ctx);
      co_return;
    }
    auto importer = caf::actor_cast<importer_actor>(components->front());
    if (not importer) {
      diagnostic::error("failed to cast importer component").emit(ctx);
      co_return;
    }
    importer_ = std::move(importer);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    auto has_secrets = false;
    std::tie(has_secrets, input) = replace_secrets(std::move(input));
    if (has_secrets) {
      diagnostic::warning("`secret` cannot be imported as secrets")
        .note("fields will be `\"***\"`")
        .emit(ctx.dh());
    }
    // The current catalog assumes that all events have at least one field.
    // This check guards against that. We should remove it once we get to
    // rewriting our catalog.
    if (as<record_type>(input.schema()).num_fields() == 0) {
      co_return;
    }
    if (not importer_) {
      co_return;
    }
    write_bytes_counter_.add(input.approx_bytes());
    write_events_counter_.add(input.rows());
    if (not input.schema().attribute("internal").has_value()) {
      import_metrics_.emit({
        {"schema", std::string{input.schema().name()}},
        {"schema_id", input.schema().make_fingerprint()},
        {"events", input.rows()},
      });
    }
    auto* diagnostics = &ctx.dh();
    inflight_.push_back(
      ctx.spawn_task([importer = importer_, slice = std::move(input),
                      diagnostics]() mutable -> Task<void> {
        auto result = co_await async_mail(std::move(slice)).request(importer);
        if (not result) {
          diagnostic::error(result.error())
            .note("failed to import events")
            .emit(*diagnostics);
        }
      }));
    if (inflight_.size() >= max_inflight_batches) {
      auto handle = std::move(inflight_.front());
      inflight_.pop_front();
      co_await handle.join();
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (not importer_) {
      co_return FinalizeBehavior::done;
    }
    while (not inflight_.empty()) {
      auto handle = std::move(inflight_.front());
      inflight_.pop_front();
      co_await handle.join();
    }
    auto result = co_await async_mail(atom::flush_v).request(importer_);
    if (not result) {
      diagnostic::error(result.error()).note("failed to flush import").emit(ctx);
    }
    co_return FinalizeBehavior::done;
  }

private:
  static constexpr auto max_inflight_batches = size_t{20};

  importer_actor importer_;
  MetricsCounter write_bytes_counter_ = {};
  MetricsCounter write_events_counter_ = {};
  metric_handler import_metrics_ = {};
  std::deque<AsyncHandle<void>> inflight_ = {};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "import";
  }

  auto describe() const -> Description override {
    auto d = Describer<ImportArgs, Import>{};
    return d.invariant_order_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::import

TENZIR_REGISTER_PLUGIN(tenzir::plugins::import::plugin)
