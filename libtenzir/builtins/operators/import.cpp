//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/fetch_node.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/atoms.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
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
      MetricsDirection::write, MetricsVisibility::internal_);
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
    if (input.rows() == 0) {
      co_return;
    }
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
  std::deque<AsyncHandle<void>> inflight_ = {};
};

class import_operator final : public crtp_operator<import_operator> {
public:
  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    const auto start_time = std::chrono::steady_clock::now();
    const auto importer
      = ctrl.self().system().registry().get<importer_actor>("tenzir.importer");
    auto metric_handler = ctrl.metrics({
      "tenzir.metrics.import",
      record_type{
        {"schema", string_type{}},
        {"schema_id", string_type{}},
        {"events", uint64_type{}},
      },
    });
    auto total_events = size_t{0};
    auto inflight_batches = size_t{0};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto has_secrets = false;
      std::tie(has_secrets, slice) = replace_secrets(std::move(slice));
      if (has_secrets) {
        diagnostic::warning("`secret` cannot be imported as secrets")
          .note("fields will be `\"***\"`")
          .emit(ctrl.diagnostics());
      }
      // The current catalog assumes that all events have at least one field.
      // This check guards against that. We should remove it once we get to
      // rewriting our catalog.
      if (as<record_type>(slice.schema()).num_fields() == 0) {
        co_yield {};
        continue;
      }
      if (not slice.schema().attribute("internal").has_value()) {
        metric_handler.emit({
          {"schema", std::string{slice.schema().name()}},
          {"schema_id", slice.schema().make_fingerprint()},
          {"events", slice.rows()},
        });
      }
      total_events += slice.rows();
      inflight_batches += 1;
      ctrl.self()
        .mail(std::move(slice))
        .request(importer, caf::infinite)
        .then(
          [&]() {
            inflight_batches -= 1;
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to import events")
              .emit(ctrl.diagnostics());
          });
      // Limit to at most 20 in-flight batches.
      if (inflight_batches >= 20) {
        ctrl.set_waiting(true);
      }
      co_yield {};
    }
    while (inflight_batches > 0) {
      ctrl.set_waiting(true);
      co_yield {};
    }
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::flush_v)
      .request(importer, caf::infinite)
      .then(
        [&] {
          ctrl.set_waiting(false);
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to flush import")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    const auto elapsed = std::chrono::steady_clock::now() - start_time;
    const auto rate
      = static_cast<double>(total_events)
        / std::chrono::duration_cast<
            std::chrono::duration<double, std::chrono::seconds::period>>(
            elapsed)
            .count();
    TENZIR_DEBUG("imported {} events in {}{}", total_events, data{elapsed},
                 std::isfinite(rate)
                   ? fmt::format(" at a rate of {:.2f} events/s", rate)
                   : "");
  }

  auto name() const -> std::string override {
    return "import";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, import_operator& x) -> bool {
    return f.object(x).fields();
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto internal() const -> bool override {
    return true;
  }
};

class plugin final : public virtual operator_plugin<import_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.sink = true};
  }

  auto describe() const -> Description override {
    auto d = Describer<ImportArgs, Import>{};
    return d.without_optimize();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{
      "import",
      "https://docs.tenzir.com/operators/import",
    };
    parser.parse(p);
    return std::make_unique<import_operator>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("import").parse(inv, ctx).ignore();
    return std::make_unique<import_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::import

TENZIR_REGISTER_PLUGIN(tenzir::plugins::import::plugin)
