//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/os.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::processes {

namespace {

auto make_processes(diagnostic_handler& dh) -> Option<table_slice> {
  auto system = os::make();
  if (not system) {
    diagnostic::error("failed to create OS shim").emit(dh);
    return None{};
  }
  return system->processes();
}

struct ProcessesArgs {
  // No arguments.
};

class Processes final : public Operator<void, table_slice> {
public:
  explicit Processes(ProcessesArgs /*args*/) {
  }

  auto start(OpCtx&) -> Task<void> override {
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result);
    if (auto output = make_processes(ctx.dh())) {
      co_await push(std::move(*output));
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class ProcessesEvents final : public Operator<void, nova::Events> {
public:
  explicit ProcessesEvents(ProcessesArgs /*args*/) {
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await spawn_blocking([] -> Option<std::vector<process>> {
      auto system = os::make();
      if (not system) {
        return None{};
      }
      return system->process_data();
    });
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto processes = std::move(result).as<Option<std::vector<process>>>();
    if (not processes) {
      diagnostic::error("failed to create OS shim").emit(ctx.dh());
      done_ = true;
      co_return;
    }
    auto builder = nova::ArrayBuilder<nova::Record>{};
    for (const auto& proc : *processes) {
      auto event = builder.record();
      event.field("name").data(proc.name);
      if (proc.command_line.empty()) {
        event.field("command_line").null();
      } else {
        auto command_line = event.field("command_line").list();
        for (const auto& argument : proc.command_line) {
          command_line.data(argument);
        }
      }
      event.field("pid").data(static_cast<uint64_t>(proc.pid));
      event.field("ppid").data(static_cast<uint64_t>(proc.ppid));
      event.field("uid").data(static_cast<uint64_t>(proc.uid));
      event.field("gid").data(static_cast<uint64_t>(proc.gid));
      event.field("ruid").data(static_cast<uint64_t>(proc.ruid));
      event.field("rgid").data(static_cast<uint64_t>(proc.rgid));
      event.field("priority").data(proc.priority);
      event.field("startup").data(proc.startup);
      auto optional_field = [&](std::string_view name, auto const& value) {
        auto field = event.field(name);
        if (value) {
          field.data(*value);
        } else {
          field.null();
        }
      };
      optional_field("vsize", proc.vsize);
      optional_field("rsize", proc.rsize);
      optional_field("swap", proc.swap);
      optional_field("peak_mem", proc.peak_mem);
      optional_field("open_fds", proc.open_fds);
      optional_field("utime", proc.utime);
      optional_field("stime", proc.stime);
    }
    auto output = builder.finish();
    auto rows = output.length();
    co_await push(
      nova::Events{std::move(output), nova::storage::BitMap{rows, true},
                   nova::Events::Meta::make_empty(rows, "tenzir.process")});
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }
  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "processes";
  }

  auto describe() const -> Description override {
    auto d = Describer<ProcessesArgs, Processes, ProcessesEvents>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::processes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::processes::Plugin)
