//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/env.hpp>
#include <tenzir/detail/load_contents.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/exec_pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <iostream>

namespace tenzir::plugins::exec {

namespace {

void dump_diagnostics_to_stdout(std::span<const diagnostic> diagnostics,
                                std::string filename, std::string content) {
  // Replay diagnostics to reconstruct `stderr` on `stdout`.
  auto printer = make_diagnostic_printer(location_origin{std::move(filename),
                                                         std::move(content)},
                                         color_diagnostics::no, std::cout);
  for (auto&& diag : diagnostics) {
    printer->emit(diag);
  }
}

class diagnostic_handler_ref final : public diagnostic_handler {
public:
  explicit diagnostic_handler_ref(diagnostic_handler& inner) : inner_{inner} {
  }

  void emit(diagnostic d) override {
    inner_.emit(std::move(d));
  }

private:
  diagnostic_handler& inner_;
};

auto exec_command(const invocation& inv, caf::actor_system& sys)
  -> caf::expected<void> {
  const auto& args = inv.arguments;
  if (args.size() != 1)
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("expected exactly one argument, but got {}", args.size()));
  auto cfg = exec_config{};
  cfg.dump_ast = caf::get_or(inv.options, "tenzir.exec.dump-ast", false);
  cfg.dump_diagnostics
    = caf::get_or(inv.options, "tenzir.exec.dump-diagnostics", false);
  cfg.dump_metrics
    = caf::get_or(inv.options, "tenzir.exec.dump-metrics", false);
  auto as_file = caf::get_or(inv.options, "tenzir.exec.file", false);
  cfg.implicit_bytes_sink = caf::get_or(
    inv.options, "tenzir.exec.implicit-bytes-sink", cfg.implicit_bytes_sink);
  cfg.implicit_events_sink = caf::get_or(
    inv.options, "tenzir.exec.implicit-events-sink", cfg.implicit_events_sink);
  cfg.implicit_bytes_source
    = caf::get_or(inv.options, "tenzir.exec.implicit-bytes-source",
                  cfg.implicit_bytes_source);
  cfg.implicit_events_source
    = caf::get_or(inv.options, "tenzir.exec.implicit-events-source",
                  cfg.implicit_events_source);
  auto filename = std::string{};
  auto content = std::string{};
  if (as_file) {
    filename = args[0];
    auto result = detail::load_contents(filename);
    if (!result) {
      // TODO: Better error message.
      return result.error();
    }
    content = std::move(*result);
  } else {
    filename = "<input>";
    content = args[0];
  }
  if (cfg.dump_diagnostics) {
    auto diag = collecting_diagnostic_handler{};
    auto result = exec_pipeline(
      content, std::make_unique<diagnostic_handler_ref>(diag), cfg, sys);
    dump_diagnostics_to_stdout(std::move(diag).collect(), filename,
                               std::move(content));
    return result;
  }
  auto color = isatty(STDERR_FILENO) == 1
               && detail::getenv("NO_COLOR").value_or("").empty();
  auto printer = make_diagnostic_printer(
    location_origin{filename, content},
    color ? color_diagnostics::yes : color_diagnostics::no, std::cerr);
  return exec_pipeline(std::move(content), std::move(printer), cfg, sys);
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  auto name() const -> std::string override {
    return "exec";
  }

  auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory> override {
    auto exec = std::make_unique<command>(
      "exec", "execute a pipeline locally",
      command::opts("?tenzir.exec")
        .add<bool>("file,f", "load the pipeline definition from a file")
        .add<bool>("dump-ast",
                   "print a textual description of the AST and then exit")
        .add<bool>("dump-diagnostics",
                   "print all diagnostics to stdout before exiting")
        .add<bool>("dump-metrics",
                   "print all diagnostics to stderr before exiting")
        .add<std::string>("implicit-bytes-sink",
                          "implicit sink for pipelines ending in bytes "
                          "(default: 'save file -')")
        .add<std::string>("implicit-events-sink",
                          "implicit sink for pipelines ending in events "
                          "(default: 'to stdout write json'")
        .add<std::string>("implicit-bytes-source",
                          "implicit source for pipelines starting with bytes "
                          "(default: 'load file -')")
        .add<std::string>("implicit-events-source",
                          "implicit source for pipelines starting with events "
                          "(default: 'from stdin read json'"));
    auto factory = command::factory{
      {"exec",
       [=](const invocation& inv, caf::actor_system& sys) -> caf::message {
         auto result = exec_command(inv, sys);
         if (not result) {
           if (result != ec::diagnostic and result != ec::silent) {
             auto diag = make_diagnostic_printer(
               std::nullopt, color_diagnostics::yes, std::cerr);
             diag->emit(diagnostic::error(result.error())
                          .note("pipeline execution failed")
                          .done());
           }
           return caf::make_message(ec::silent);
         }
         return {};
       }},
    };
    return {std::move(exec), std::move(factory)};
  };
};

} // namespace

} // namespace tenzir::plugins::exec

TENZIR_REGISTER_PLUGIN(tenzir::plugins::exec::plugin)
