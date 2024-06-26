//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/croncpp.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::every_cron {

namespace {

using alarm_clock_actor = caf::typed_actor<
  // Waits for `delay` before returning.
  auto(duration delay)->caf::result<void>>;

auto make_alarm_clock(alarm_clock_actor::pointer self)
  -> alarm_clock_actor::behavior_type {
  return {
    [self](duration delay) -> caf::result<void> {
      auto rp = self->make_response_promise<void>();
      detail::weak_run_delayed(self, delay, [rp]() mutable {
        rp.deliver();
      });
      return rp;
    },
  };
}

template <typename T>
concept scheduler_concept
  = requires(const T t, time::clock::time_point now, parser_interface& p) {
      { t.next_after(now) } -> std::same_as<time::clock::time_point>;
      { T::parse(p) } -> std::same_as<T>;
      { T::name } -> std::convertible_to<std::string_view>;
      { T::immediate } -> std::same_as<const bool&>;
    };

/// This is the base template for all kinds of scheduled execution operators,
/// such as the `every` and `cron` operators. The actual scheduling logic, CAF
/// serialization and name are handled by the `Scheduler` template parameter
template <scheduler_concept Scheduler>
class scheduled_execution_operator final : public operator_base {
public:
  scheduled_execution_operator() = default;

  scheduled_execution_operator(operator_ptr op, Scheduler scheduler)
    : op_{std::move(op)}, scheduler_{std::move(scheduler)} {
    if (auto* op = dynamic_cast<scheduled_execution_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    TENZIR_ASSERT(
      not dynamic_cast<const scheduled_execution_operator*>(op_.get()));
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    auto result = op_->optimize(filter, order);
    if (not result.replacement) {
      return result;
    }
    if (auto* pipe = dynamic_cast<pipeline*>(result.replacement.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<scheduled_execution_operator>(
          std::move(result.replacement), scheduler_);
        // Only the first operator can be a source and needs to be replaced.
        break;
      }
      result.replacement = std::make_unique<pipeline>(std::move(ops));
      return result;
    }
    result.replacement = std::make_unique<scheduled_execution_operator>(
      std::move(result.replacement), scheduler_);
    return result;
  }

  template <class Input, class Output>
  auto run(operator_input input,
           operator_control_plane& ctrl) const -> generator<Output> {
    auto alarm_clock = ctrl.self().spawn(make_alarm_clock);
    auto next_run = scheduler_.next_after(time::clock::now());
    auto done = false;
    co_yield {};
    auto make_input = [&, input = std::move(input)]() mutable {
      if constexpr (std::is_same_v<Input, std::monostate>) {
        (void)next_run;
        TENZIR_ASSERT(std::holds_alternative<std::monostate>(input));
        return []() -> std::monostate {
          return {};
        };
      } else {
        TENZIR_ASSERT(std::holds_alternative<generator<Input>>(input));
        auto typed_input = std::move(std::get<generator<Input>>(input));
        // We prime the generator's coroutine manually so that we can use
        // `unsafe_current()` in the adapted generator.
        typed_input.begin();
        return
          [&, input = std::move(typed_input)]() mutable -> generator<Input> {
            auto it = input.unsafe_current();
            while (time::clock::now() < next_run and it != input.end()) {
              co_yield std::move(*it);
              ++it;
            }
            done = it == input.end();
          };
      }
    }();
    bool generate_output = Scheduler::immediate;
    while (true) {
      if (generate_output) {
        auto gen = op_->instantiate(make_input(), ctrl);
        if (not gen) {
          diagnostic::error(gen.error()).emit(ctrl.diagnostics());
          co_return;
        }
        auto typed_gen = std::get_if<generator<Output>>(&*gen);
        TENZIR_ASSERT(typed_gen);
        for (auto&& result : *typed_gen) {
          co_yield std::move(result);
        }
        if (done) {
          break;
        }
      }
      generate_output = true;
      const auto now = time::clock::now();
      const duration delta = next_run - now;
      if (delta < duration::zero()) {
        next_run = scheduler_.next_after(now);
        continue;
      }
      next_run = scheduler_.next_after(next_run);
      ctrl.self()
        .request(alarm_clock, caf::infinite, delta)
        .await([]() { /*nop*/ },
               [&](const caf::error& err) {
                 diagnostic::error(err)
                   .note("failed to wait for {} timeout", data{delta})
                   .emit(ctrl.diagnostics());
               });
      co_yield {};
    }
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    auto f = [&]<class Input>(const Input&) -> caf::expected<operator_output> {
      using generator_type
        = std::conditional_t<std::is_same_v<Input, std::monostate>,
                             generator<std::monostate>, Input>;
      using input_type = typename generator_type::value_type;
      using tag_type
        = std::conditional_t<std::is_same_v<input_type, std::monostate>,
                             tag<void>, tag<input_type>>;
      auto output = infer_type_impl(tag_type{});
      if (not output) {
        return std::move(output.error());
      }
      if (output->template is<table_slice>()) {
        return run<input_type, table_slice>(std::move(input), ctrl);
      }
      if (output->template is<chunk_ptr>()) {
        return run<input_type, chunk_ptr>(std::move(input), ctrl);
      }
      TENZIR_ASSERT(output->template is<void>());
      return run<input_type, std::monostate>(std::move(input), ctrl);
    };
    return std::visit(f, input);
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<scheduled_execution_operator>(op_->copy(),
                                                          scheduler_);
  };

  auto location() const -> operator_location override {
    return op_->location();
  }

  auto detached() const -> bool override {
    return op_->detached();
  }

  auto internal() const -> bool override {
    return op_->internal();
  }

  auto input_independent() const -> bool override {
    return op_->input_independent();
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    return op_->infer_type(input);
  }

  auto name() const -> std::string override {
    return std::string{scheduler_.name};
  }

  friend auto inspect(auto& f, scheduled_execution_operator& x) -> bool {
    return f.object(x).fields(f.field("op", x.op_),
                              f.field("scheduler", x.scheduler_));
  }

private:
  operator_ptr op_;
  Scheduler scheduler_;
};

/// This is the base plugin template for scheduled execution operators.
/// The actual parsing is handled by the `Scheduler` type.
template <scheduler_concept Scheduler>
class scheduled_execution_plugin
  : public virtual operator_plugin<scheduled_execution_operator<Scheduler>> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    using operator_type = scheduled_execution_operator<Scheduler>;
    auto scheduler = Scheduler::parse(p);

    auto result = p.parse_operator();
    if (not result.inner) {
      diagnostic::error("failed to parse operator")
        .primary(result.source)
        .throw_();
    }
    if (auto* pipe = dynamic_cast<pipeline*>(result.inner.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<operator_type>(std::move(op),
                                             std::move(scheduler));
        // Only the first operator can be a source and needs to be replaced.
        break;
      }
      return std::make_unique<pipeline>(std::move(ops));
    }
    return std::make_unique<operator_type>(std::move(result.inner),
                                           std::move(scheduler));
  }
};

class every_scheduler {
public:
  constexpr static std::string_view name = "every";
  constexpr static bool immediate = true;

  every_scheduler() = default;
  explicit every_scheduler(duration interval) : interval_{interval} {
  }

  friend auto inspect(auto& f, every_scheduler& x) -> bool {
    return f.object(x).fields(f.field("interval", x.interval_));
  }

  auto
  next_after(time::clock::time_point now) const -> time::clock::time_point {
    return std::chrono::time_point_cast<time::clock::time_point::duration>(
      now + interval_);
  }

  static auto parse(parser_interface& p) -> every_scheduler {
    auto interval_data = p.parse_data();
    const auto* interval = caf::get_if<duration>(&interval_data.inner);
    if (not interval) {
      diagnostic::error("interval must be a duration")
        .primary(interval_data.source)
        .throw_();
    }
    if (*interval <= duration::zero()) {
      diagnostic::error("interval must be a positive duration")
        .primary(interval_data.source)
        .throw_();
    }
    return every_scheduler{*interval};
  }

private:
  duration interval_;
};

using every_plugin = scheduled_execution_plugin<every_scheduler>;

class cron_scheduler {
public:
  constexpr static std::string_view name = "cron";
  constexpr static bool immediate = false;

  cron_scheduler() = default;
  explicit cron_scheduler(detail::cron::cronexpr expr)
    : cronexpr_{std::move(expr)} {
  }

  auto
  next_after(time::clock::time_point now) const -> time::clock::time_point {
    const auto tt = time::clock::to_time_t(now);
    return time::clock::from_time_t(detail::cron::cron_next(cronexpr_, tt));
  }

  friend auto inspect(auto& f, cron_scheduler& x) -> bool {
    const auto get = [&x]() {
      return detail::cron::to_cronstr(x.cronexpr_);
    };
    const auto set = [&x](std::string_view text) {
      x.cronexpr_ = detail::cron::make_cron(text);
    };
    return f.object(x).fields(f.field("cronexpr", get, set));
  }

  static cron_scheduler parse(parser_interface& p) {
    auto cronexpr_string = p.accept_shell_arg();
    if (not cronexpr_string) {
      diagnostic::error("expected cron expression")
        .primary(p.current_span())
        .throw_();
    }
    try {
      return cron_scheduler{detail::cron::make_cron(cronexpr_string->inner)};
    } catch (const detail::cron::bad_cronexpr& ex) {
      // The croncpp library re-throws the exception message from the
      // `std::stoul` call on failure. This happens for most cases of invalid
      // expressions, i.e. ones that do not contain unsigned integers or allowed
      // literals. libstdc++ and libc++ exception messages both contain the
      // string "stoul" in their what() strings. We can check for this and
      // provide a slightly better error message back to the user.
      if (std::string_view{ex.what()}.find("stoul") != std::string_view::npos) {
        diagnostic::error(
          "bad cron expression: invalid value for at least one field")
          .primary(cronexpr_string->source)
          .throw_();
      } else {
        diagnostic::error("bad cron expression: \"{}\"", ex.what())
          .primary(cronexpr_string->source)
          .throw_();
      }
    }
  }

private:
  detail::cron::cronexpr cronexpr_;
};
using cron_plugin = scheduled_execution_plugin<cron_scheduler>;

class every_plugin2 final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.every";
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto interval = located<duration>{};
    auto pipe = pipeline{};
    argument_parser2::op("every")
      .add(interval, "<duration>")
      .add(pipe, "<pipeline>") // TODO: Improve meta.
      .parse(inv, ctx);
    if (interval.inner <= duration::zero()) {
      diagnostic::error("expected a positive duration, got {}", interval.inner)
        .primary(interval)
        .emit(ctx);
      return nullptr;
    }
    auto ops = std::move(pipe).unwrap();
    // TODO: How do we know whether `pipe` was set? This looks hacky.
    if (ops.empty()) {
      return nullptr;
    }
    if (ops.size() > 1) {
      // TODO: Lift this limitation.
      // TODO: Is this safe?
      diagnostic::error("expected exactly one operator, found {}", ops.size())
        .primary(inv.args[1])
        .emit(ctx);
      return nullptr;
    }
    return std::make_unique<scheduled_execution_operator<every_scheduler>>(
      std::move(ops[0]), every_scheduler{interval.inner});
  }
};

} // namespace

} // namespace tenzir::plugins::every_cron

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::cron_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::every_plugin2)
