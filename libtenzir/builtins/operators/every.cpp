//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/type.h>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::every {

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

class every_operator final : public operator_base {
public:
  every_operator() = default;

  every_operator(operator_ptr op, duration interval)
    : op_{std::move(op)}, interval_{interval} {
    if (auto* op = dynamic_cast<every_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    TENZIR_ASSERT(not dynamic_cast<const every_operator*>(op_.get()));
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    auto result = op_->optimize(filter, order);
    if (not result.replacement) {
      return result;
    }
    if (auto* pipe = dynamic_cast<pipeline*>(result.replacement.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<every_operator>(std::move(result.replacement),
                                              interval_);
        // Only the first operator can be a source and needs to be replaced.
        break;
      }
      result.replacement = std::make_unique<pipeline>(std::move(ops));
      return result;
    }
    result.replacement = std::make_unique<every_operator>(
      std::move(result.replacement), interval_);
    return result;
  }

  template <class Input, class Output>
  static auto run(operator_ptr op, duration interval, operator_input input,
                  operator_control_plane& ctrl) -> generator<Output> {
    auto alarm_clock = ctrl.self().spawn(make_alarm_clock);
    auto now = time::clock::now();
    auto next_run = now + interval;
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
    while (true) {
      auto gen = op->instantiate(make_input(), ctrl);
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
      now = time::clock::now();
      const auto delta = next_run - now;
      if (delta < duration::zero()) {
        next_run = now + interval;
        continue;
      }
      next_run += interval;
      ctrl.self()
        .request(alarm_clock, caf::infinite, delta)
        .await([]() { /*nop*/ },
               [&](const caf::error& err) {
                 diagnostic::error(err)
                   .note("failed to wait for {} timeout", data{interval})
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
        return run<input_type, table_slice>(op_->copy(), interval_,
                                            std::move(input), ctrl);
      }
      if (output->template is<chunk_ptr>()) {
        return run<input_type, chunk_ptr>(op_->copy(), interval_,
                                          std::move(input), ctrl);
      }
      TENZIR_ASSERT(output->template is<void>());
      return run<input_type, std::monostate>(op_->copy(), interval_,
                                             std::move(input), ctrl);
    };
    return std::visit(f, input);
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<every_operator>(op_->copy(), interval_);
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
    return "every";
  }

  friend auto inspect(auto& f, every_operator& x) -> bool {
    return f.object(x).fields(f.field("op", x.op_),
                              f.field("interval", x.interval_));
  }

private:
  operator_ptr op_;
  duration interval_;
};

class every_plugin final : public virtual operator_plugin<every_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
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
    auto result = p.parse_operator();
    if (not result.inner) {
      diagnostic::error("failed to parse operator")
        .primary(result.source)
        .throw_();
    }
    if (auto* pipe = dynamic_cast<pipeline*>(result.inner.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<every_operator>(std::move(op), *interval);
        // Only the first operator can be a source and needs to be replaced.
        break;
      }
      return std::make_unique<pipeline>(std::move(ops));
    }
    return std::make_unique<every_operator>(std::move(result.inner), *interval);
  }
};

} // namespace

} // namespace tenzir::plugins::every

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every::every_plugin)
