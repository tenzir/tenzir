//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::fork {

namespace {

class fork_operator final : public crtp_operator<fork_operator> {
public:
  fork_operator() = default;

  fork_operator(located<pipeline> pipe, operator_location location)
    : pipe_{std::move(pipe)}, location_{location} {
  }

  auto operator()(generator<table_slice> input, exec_ctx ctx) const
    -> generator<table_slice> {
    auto fork_input = std::optional<table_slice>{table_slice{}};
    auto make_input = [&fork_input]() -> generator<table_slice> {
      while (fork_input) {
        co_yield std::exchange(*fork_input, {});
      }
    };
    auto instance = pipe_.inner.instantiate(make_input(), ctrl);
    if (not instance) {
      diagnostic::error(instance.error()).emit(ctrl.diagnostics());
      co_return;
    }
    auto fork = std::get_if<generator<std::monostate>>(&*instance);
    if (not fork) {
      diagnostic::error("expected sink").primary(pipe_).emit(ctrl.diagnostics());
      co_return;
    }
    auto it = fork->unsafe_current();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      fork_input = slice;
      // TODO: How do we know that the pipeline is done when the input has been
      // consumed? Probably we don't. This needs proper nested pipelines.
      while (fork_input->rows() > 0 and it != fork->end()) {
        ++it;
      }
      co_yield slice;
    }
    fork_input = std::nullopt;
    while (it != fork->end()) {
      ++it;
    }
  }

  auto location() const -> operator_location override {
    return location_;
  }

  auto name() const -> std::string override {
    return "tql2.fork";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, fork_operator& x) -> bool {
    return f.object(x).fields(f.field("pipe", x.pipe_),
                              f.field("location", x.location_));
  }

private:
  located<pipeline> pipe_;
  operator_location location_;
};

class plugin final : public virtual operator_plugin2<fork_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto pipe = located<pipeline>{};
    TRY(argument_parser2::operator_("fork")
          .add(pipe, "<pipeline>")
          .parse(inv, ctx));
    auto loc = operator_location::anywhere;
    for (auto& op : pipe.inner.operators()) {
      auto op_loc = op->location();
      if (op_loc != operator_location::anywhere) {
        if (loc != operator_location::anywhere && loc != op_loc) {
          diagnostic::error("TODO: could not decide location")
            .primary(inv.self)
            .emit(ctx);
          return nullptr;
        }
        loc = op_loc;
      }
    }
    return std::make_unique<fork_operator>(std::move(pipe), loc);
  }
};

} // namespace

} // namespace tenzir::plugins::fork

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork::plugin)
