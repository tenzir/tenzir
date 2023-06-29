//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/connect_to_node.hpp>
#include <vast/detail/load_contents.hpp>
#include <vast/diagnostics.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/pipeline_executor.hpp>
#include <vast/plugin.hpp>
#include <vast/tql/parser.hpp>

#include <caf/detail/stringification_inspector.hpp>
#include <caf/json_writer.hpp>
#include <caf/scoped_actor.hpp>

namespace vast::plugins::exec {

namespace {

auto exec_pipeline(pipeline pipe, caf::actor_system& sys,
                   std::unique_ptr<diagnostic_handler> diag)
  -> caf::expected<void> {
  // If the pipeline ends with events, we implicitly write the output as JSON
  // to stdout, and if it ends with bytes, we implicitly write those bytes to
  // stdout.
  if (pipe.check_type<void, table_slice>()) {
    auto op = pipeline::internal_parse_as_operator("write json --pretty");
    if (not op) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("failed to append implicit 'write "
                                         "json --pretty': {}",
                                         op.error()));
    }
    pipe.append(std::move(*op));
  } else if (pipe.check_type<void, chunk_ptr>()) {
    auto op = pipeline::internal_parse_as_operator("save file -");
    if (not op) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("failed to append implicit 'save "
                                         "file -': {}",
                                         op.error()));
    }
    pipe.append(std::move(*op));
  }
  auto self = caf::scoped_actor{sys};
  auto executor = self->spawn<caf::monitored>(
    pipeline_executor, std::move(pipe), std::move(diag), node_actor{});
  auto result = caf::expected<void>{};
  // TODO: This command should probably implement signal handling, and check
  // whether a signal was raised in every iteration over the executor. This
  // will likely be easier to implement once we switch to the actor-based
  // asynchronous executor, so we may as well wait until then.
  self->send(executor, atom::start_v);
  auto running = true;
  self->receive_while(running)(
    []() {
      VAST_DEBUG("pipeline was succcesfully started");
    },
    [&](caf::error& err) {
      VAST_DEBUG("failed to start pipeline: {}", err);
      result = err;
      running = false;
    },
    [&](caf::down_msg& msg) {
      VAST_DEBUG("pipeline execution finished: {}", msg.reason);
      running = false;
      result = msg.reason;
    });
  return result;
}

void dump_diagnostics_to_stdout(std::span<const diagnostic> diagnostics,
                                std::string filename, std::string content) {
  // Replay diagnostics to reconstruct `stderr` on `stdout`.
  auto printer = make_diagnostic_printer(std::move(filename),
                                         std::move(content), false, std::cout);
  for (auto&& diag : diagnostics) {
    printer->emit(diag);
  }
}

auto exec_impl(std::string content, std::unique_ptr<diagnostic_handler> diag,
               bool dump_ast, caf::actor_system& sys) -> caf::expected<void> {
  auto parsed = tql::parse(std::move(content), *diag);
  if (not parsed) {
    if (not diag->has_seen_error()) {
      return caf::make_error(ec::unspecified,
                             "internal error: parsing failed without an error");
    }
    return ec::silent;
  }
  if (diag->has_seen_error()) {
    return caf::make_error(ec::unspecified,
                           "internal error: parsing successful with error");
  }
  if (dump_ast) {
    for (auto& op : *parsed) {
      fmt::print("{}\n", op.inner);
    }
    fmt::print("-----\n");
    for (auto& op : *parsed) {
      auto str = std::string{};
      auto writer = caf::detail::stringification_inspector{str};
      if (writer.apply(op)) {
        fmt::print("{}\n", str);
      } else {
        fmt::print("<error: {}>\n", writer.get_error());
      }
    }
    return {};
  }
  return exec_pipeline(tql::to_pipeline(std::move(*parsed)), sys,
                       std::move(diag));
}

class diagnostic_handler_ref final : public diagnostic_handler {
public:
  explicit diagnostic_handler_ref(diagnostic_handler& inner) : inner_{inner} {
  }

  void emit(diagnostic d) override {
    inner_.emit(std::move(d));
  }

  auto has_seen_error() const -> bool override {
    return inner_.has_seen_error();
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
  auto dump_ast = caf::get_or(inv.options, "tenzir.exec.dump-ast", false);
  auto dump_diagnostics
    = caf::get_or(inv.options, "tenzir.exec.dump-diagnostics", false);
  auto as_file = caf::get_or(inv.options, "tenzir.exec.file", false);
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
  if (dump_diagnostics) {
    auto diag = collecting_diagnostic_handler{};
    auto result = exec_impl(
      content, std::make_unique<diagnostic_handler_ref>(diag), dump_ast, sys);
    dump_diagnostics_to_stdout(std::move(diag).collect(), filename,
                               std::move(content));
    return result;
  }
  auto printer = make_diagnostic_printer(filename, content, true, std::cerr);
  return exec_impl(std::move(content), std::move(printer), dump_ast, sys);
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
                   "print all diagnostics to stdout before exiting"));
    auto factory = command::factory{
      {"exec",
       [=](const invocation& inv, caf::actor_system& sys) -> caf::message {
         auto result = exec_command(inv, sys);
         if (not result)
           return caf::make_message(result.error());
         return {};
       }},
    };
    return {std::move(exec), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::exec

VAST_REGISTER_PLUGIN(vast::plugins::exec::plugin)
