//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/diagnostics.hpp>
#include <tenzir/exec_pipeline.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/session.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/parser.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>

#include <ranges>
#include <string_view>

namespace tenzir {

namespace {

auto format_metric(const operator_metric& metric) -> std::string {
  auto result = std::string{};
  auto it = std::back_inserter(result);
  constexpr auto indent = std::string_view{"  "};
  it = fmt::format_to(it, "operator #{} ({})", metric.operator_index + 1,
                      metric.operator_name);
  if (metric.internal) {
    it = fmt::format_to(it, " (internal)");
  }
  it = fmt::format_to(it, "\n");
  it = fmt::format_to(it, "{}total: {}\n", indent, data{metric.time_total});
  it = fmt::format_to(it, "{}time-to-first-input: {}\n", indent,
                      data{metric.time_to_first_input});
  it = fmt::format_to(it, "{}scheduled: {} ({:.2f}%)\n", indent,
                      data{metric.time_scheduled},
                      100.0 * static_cast<double>(metric.time_scheduled.count())
                        / static_cast<double>(metric.time_total.count()));
  it
    = fmt::format_to(it, "{}processing: {} ({:.2f}%)\n", indent,
                     data{metric.time_processing},
                     100.0 * static_cast<double>(metric.time_processing.count())
                       / static_cast<double>(metric.time_total.count()));
  it = fmt::format_to(it, "{}running: {} ({:.2f}%)\n", indent,
                      data{metric.time_running},
                      100.0 * static_cast<double>(metric.time_running.count())
                        / static_cast<double>(metric.time_total.count()));
  it = fmt::format_to(it, "{}paused: {} ({:.2f}%)\n", indent,
                      data{metric.time_paused},
                      100.0 * static_cast<double>(metric.time_paused.count())
                        / static_cast<double>(metric.time_total.count()));
  it = fmt::format_to(
    it, "{}runs: {} ({:.2f}% processing / {:.2f}% input / {:.2f}% output)\n",
    indent, metric.num_runs,
    100.0 * metric.num_runs_processing / metric.num_runs,
    100.0 * metric.num_runs_processing_input / metric.num_runs,
    100.0 * metric.num_runs_processing_output / metric.num_runs);
  if (metric.inbound_measurement.unit != "void") {
    it = fmt::format_to(it, "{}inbound:\n", indent);
    it = fmt::format_to(
      it, "{}{}{}: {} at a rate of {:.2f}/s\n", indent, indent,
      metric.inbound_measurement.unit, metric.inbound_measurement.num_elements,
      static_cast<double>(metric.inbound_measurement.num_elements)
        / std::chrono::duration_cast<
            std::chrono::duration<double, std::chrono::seconds::period>>(
            metric.time_running)
            .count());
    if (metric.inbound_measurement.unit != operator_type_name<chunk_ptr>()) {
      it = fmt::format_to(
        it, "{}{}bytes: {} at a rate of {:.2f}/s (estimate)\n", indent, indent,
        metric.inbound_measurement.num_approx_bytes,
        static_cast<double>(metric.inbound_measurement.num_approx_bytes)
          / std::chrono::duration_cast<
              std::chrono::duration<double, std::chrono::seconds::period>>(
              metric.time_running)
              .count());
    }
    it = fmt::format_to(
      it, "{}{}batches: {} ({:.2f} {}/batch)\n", indent, indent,
      metric.inbound_measurement.num_batches,
      metric.inbound_measurement.num_batches == 0
        ? 0
        : static_cast<double>(metric.inbound_measurement.num_elements)
            / static_cast<double>(metric.inbound_measurement.num_batches),
      metric.inbound_measurement.unit);
  }
  if (metric.outbound_measurement.unit != "void") {
    it = fmt::format_to(it, "{}outbound:\n", indent);
    it = fmt::format_to(
      it, "{}{}{}: {} at a rate of {:.2f}/s\n", indent, indent,
      metric.outbound_measurement.unit,
      metric.outbound_measurement.num_elements,
      static_cast<double>(metric.outbound_measurement.num_elements)
        / std::chrono::duration_cast<
            std::chrono::duration<double, std::chrono::seconds::period>>(
            metric.time_running)
            .count());
    if (metric.outbound_measurement.unit != operator_type_name<chunk_ptr>()) {
      it = fmt::format_to(
        it, "{}{}bytes: {} at a rate of {:.2f}/s (estimate)\n", indent, indent,
        metric.outbound_measurement.num_approx_bytes,
        static_cast<double>(metric.outbound_measurement.num_approx_bytes)
          / std::chrono::duration_cast<
              std::chrono::duration<double, std::chrono::seconds::period>>(
              metric.time_running)
              .count());
    }
    it = fmt::format_to(
      it, "{}{}batches: {} ({:.2f} {}/batch)\n", indent, indent,
      metric.outbound_measurement.num_batches,
      static_cast<double>(metric.outbound_measurement.num_elements)
        / static_cast<double>(metric.outbound_measurement.num_batches),
      metric.outbound_measurement.unit);
  }
  return result;
}

auto add_implicit(std::string_view what, pipeline pipe, diagnostic_handler& dh,
                  std::string_view source) -> caf::expected<pipeline> {
  auto sp = session_provider::make(dh);
  auto maybe_ast = parse_pipeline_with_bad_diagnostics(source, sp.as_session());
  if (not maybe_ast) {
    return caf::make_error(ec::logic_error,
                           fmt::format("failed to parse implicit {}: `{}`",
                                       what, source));
  }
  auto compiled = compile(std::move(*maybe_ast), sp.as_session());
  if (not compiled) {
    return caf::make_error(ec::logic_error,
                           fmt::format("failed to compile implicit {}: `{}`",
                                       what, source));
  }
  if (what.ends_with("sink")) {
    for (auto&& op : std::move(*compiled).unwrap()) {
      pipe.append(std::move(op));
    }
  } else if (what.ends_with("source")) {
    for (auto&& op : std::move(*compiled).unwrap() | std::views::reverse) {
      pipe.prepend(std::move(op));
    }
  } else {
    TENZIR_UNREACHABLE();
  }
  return pipe;
}

auto add_implicit_source_and_sink(pipeline pipe, diagnostic_handler& dh,
                                  exec_config const& config)
  -> caf::expected<pipeline> {
  if (pipe.infer_type<void>()) {
    // Don't add implicit source.
  } else if (pipe.infer_type<chunk_ptr>()
             && ! config.implicit_bytes_source.empty()) {
    auto res = add_implicit("bytes source", std::move(pipe), dh,
                            config.implicit_bytes_source);
    if (not res) {
      return res.error();
    }
    pipe = std::move(*res);
  } else if (pipe.infer_type<table_slice>()
             && ! config.implicit_events_source.empty()) {
    auto res = add_implicit("events source", std::move(pipe), dh,
                            config.implicit_events_source);
    if (not res) {
      return res.error();
    }
    pipe = std::move(*res);
  } else {
    // Pipeline is ill-typed. We don't add implicit source or sink and continue,
    // as this is handled further down the line.
    return pipe;
  }
  auto out = pipe.infer_type<void>();
  if (not out) {
    return caf::make_error(ec::logic_error,
                           fmt::format("expected pipeline to accept void here, "
                                       "but: {}",
                                       out.error()));
  }
  if (out->is<void>()) {
    // Pipeline is already closed, nothing to do here.
  } else if (out->is<chunk_ptr>() && ! config.implicit_bytes_sink.empty()) {
    auto res = add_implicit("bytes sink", std::move(pipe), dh,
                            config.implicit_bytes_sink);
    if (not res) {
      return res.error();
    }
    pipe = std::move(*res);
  } else if (out->is<table_slice>() && ! config.implicit_events_sink.empty()) {
    auto res = add_implicit("events sink", std::move(pipe), dh,
                            config.implicit_events_sink);
    if (not res) {
      return res.error();
    }
    pipe = std::move(*res);
  }
  if (not pipe.is_closed()) {
    return caf::make_error(ec::logic_error,
                           fmt::format("expected pipeline to be closed after "
                                       "adding implicit source and sink"));
  }
  return pipe;
}

} // namespace

auto exec_pipeline(pipeline pipe, std::string definition,
                   diagnostic_handler& dh, const exec_config& cfg,
                   caf::actor_system& sys) -> caf::expected<void> {
  auto implicit_pipe = add_implicit_source_and_sink(std::move(pipe), dh, cfg);
  if (not implicit_pipe) {
    return std::move(implicit_pipe.error());
  }
  pipe = std::move(*implicit_pipe);
  pipe = pipe.optimize_if_closed();
  auto self = caf::scoped_actor{sys};
  auto result = caf::expected<void>{};
  auto metrics = std::vector<operator_metric>{};
  auto custom_metrics = std::vector<std::vector<record>>{};
  // TODO: This command should probably implement signal handling, and check
  // whether a signal was raised in every iteration over the executor. This
  // will likely be easier to implement once we switch to the actor-based
  // asynchronous executor, so we may as well wait until then.
  struct handler_state {
    pipeline_executor_actor executor = {};
  };
  auto dedup = diagnostic_deduplicator{};
  auto handler = self->spawn(
    [&](caf::stateful_actor<handler_state>* self) -> caf::behavior {
      self->state().executor
        = self->spawn(pipeline_executor, std::move(pipe), std::move(definition),
                      fmt::format("{}", uuid::random()),
                      caf::actor_cast<receiver_actor<diagnostic>>(self),
                      caf::actor_cast<metrics_receiver_actor>(self),
                      node_actor{}, true, true);
      self->monitor(self->state().executor, [&, self](caf::error err) {
        TENZIR_DEBUG("command received down message `{}`", err);
        if (err) {
          result = err == caf::exit_reason::user_shutdown or err == ec::silent
                     ? ec::silent
                     : diagnostic::error(std::move(err)).to_error();
        }
        self->quit();
      });
      self->mail(atom::start_v)
        .request(self->state().executor, caf::infinite)
        .then(
          []() {
            TENZIR_DEBUG("started pipeline successfully");
          },
          [&, self](caf::error& err) {
            result = diagnostic::error(std::move(err)).to_error();
            self->quit();
          });
      return {
        [&](diagnostic& d) {
          if (cfg.strict and d.severity >= severity::warning and result) {
            result = diagnostic::error("encountered warnings in strict mode")
                       .to_error();
          }
          if (dedup.insert(d)) {
            dh.emit(std::move(d));
          }
        },
        [&](uint64_t, uuid, type&) {
          // Don't register types here.
        },
        [&](uint64_t op_index, uuid, record& r) {
          if (cfg.dump_metrics) {
            if (op_index >= custom_metrics.size()) {
              custom_metrics.resize(op_index + 1);
            }
            custom_metrics[op_index].emplace_back(std::move(r));
          }
        },
        [&](operator_metric& m) {
          if (cfg.dump_metrics) {
            const auto idx = m.operator_index;
            if (idx >= metrics.size()) {
              metrics.resize(idx + 1);
            }
            metrics[idx] = std::move(m);
          }
        },
      };
    });
  self->wait_for(handler);
  TENZIR_DEBUG("command is done");
  if (cfg.dump_metrics) {
    for (auto i = size_t{0}; i < metrics.size(); ++i) {
      const auto& metric = metrics[i];
      fmt::print(stderr, "{}", format_metric(metric));
      if (i < custom_metrics.size()) {
        fmt::print(stderr, "custom metrics for operator #{} ({}):\n",
                   metric.operator_index + 1, metric.operator_name);
        for (const auto& custom_metric : custom_metrics[i]) {
          fmt::print(stderr, "{}\n", custom_metric);
        }
      }
    }
  }
  return result;
}

auto exec_pipeline(std::string content, diagnostic_handler& dh,
                   const exec_config& cfg, caf::actor_system& sys)
  -> caf::expected<void> {
  if (not cfg.legacy) {
    auto success = exec2(std::move(content), dh, cfg, sys);
    return success ? caf::expected<void>{} : ec::silent;
  }
  auto parsed = tql::parse(content, dh);
  if (not parsed) {
    return ec::silent;
  }
  if (cfg.dump_ast) {
    for (auto& op : *parsed) {
      fmt::print("{:#?}\n", op);
    }
    return {};
  }
  auto pipe = tql::to_pipeline(std::move(*parsed));
  return exec_pipeline(std::move(pipe), std::move(content), dh, cfg, sys);
}

} // namespace tenzir
