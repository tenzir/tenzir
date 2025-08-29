//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/exec/actors.hpp"
#include "tenzir/exec/checkpoint.hpp"
#include "tenzir/exec/operator_base.hpp"
#include "tenzir/plan/operator_spawn_args.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/event_based_mail.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_stream.hpp>

namespace tenzir::exec {

#if 0
// FIXME: This should just be called operator_
class operator_impl
  : public trampoline<message<table_slice>, message<table_slice>> {
public:
  static constexpr auto name = "tenzir.exec.operator";

  operator_impl(
    exec::operator_actor::pointer self,
    detail::unique_function<auto(actor_state&)->std::unique_ptr<stateless_base>>
      impl,
    plan::operator_spawn_args args)
    : self_{self},
      state_{args.ctx, self, std::move(args.checkpoint_receiver),
             std::move(args.shutdown_handler), std::move(args.stop_handler)},
      impl_{impl(state_)} {
    TENZIR_ASSERT(impl_);
  }

  auto make_behavior() -> exec::operator_actor::behavior_type {
    self_->set_exception_handler([this](
                                   std::exception_ptr exception) -> caf::error {
      try {
        std::rethrow_exception(exception);
      } catch (const panic_exception& panic) {
        auto has_node
          = self_->system().registry().get("tenzir.node") != nullptr;
        auto diagnostic = to_diagnostic(panic);
        if (has_node) {
          auto buffer = std::stringstream{};
          buffer << "internal error in operator\n";
          auto printer = make_diagnostic_printer(std::nullopt,
                                                 color_diagnostics::no, buffer);
          printer->emit(diagnostic);
          auto string = std::move(buffer).str();
          if (not string.empty() and string.back() == '\n') {
            string.pop_back();
          }
          TENZIR_ERROR(string);
        }
        // TODO: How do we synchronize this?
        std::move(diagnostic).modify().emit(state_.ctx);
        return ec::silent;
      } catch (const std::exception& err) {
        // TODO
        return diagnostic::error("{}", err.what())
          .note("unhandled exception")
          .to_error();
      } catch (...) {
        return diagnostic::error("unhandled exception").to_error();
      }
    });
    return {
      [this](exec::handshake hs) -> caf::result<exec::handshake_response> {
        return start(std::move(hs));
      },
      [](exec::checkpoint checkpoint) -> caf::result<void> {
        (void)checkpoint;
        TENZIR_WARN("got post-commit callback");
        return {};
      },
      [](atom::stop) -> caf::result<void> {
        TENZIR_TODO();
      },
    };
  }

  auto parent() const -> caf::flow::coordinator& override {
    return *self_;
  }

  void request(size_t n) override {
    impl_->request(n);
  }

  void on_next(const exec::message<table_slice>& what) override {
    if (not state_.ready) {
      TENZIR_WARN("=> got message while not ready");
      buffer_.push_back(what);
      return;
    }
    match(
      what,
      [&](const table_slice& slice) {
        TENZIR_WARN("=> got table_slice");
        state_.ready = false;
        impl_->next(slice);
        // TODO: Wait for ready before processing next message?
      },
      [&](exec::exhausted) {
        TENZIR_WARN("=> got exhausted");
        impl_->set_input_ended();
        if (impl_->should_stop()) {
          impl_->stop();
        }
      },
      [&](exec::checkpoint checkpoint) {
        TENZIR_WARN("=> got checkpoint");
        TENZIR_ASSERT(state_.ready);
        auto chunk = impl_->serialize();
        state_.ready = false;
        self_->mail(checkpoint, std::move(chunk))
          .request(state_.checkpoint_receiver, caf::infinite)
          .then([this, checkpoint]() {
            TENZIR_WARN("checkpoint successfully saved");
            state_.output.on_next(checkpoint);
            state_.ready = true;
            state_.input.request(1);
          });
      });
  }

  void on_subscribe(caf::flow::subscription sub) override {
    TENZIR_ASSERT(not state_.input);
    state_.input = std::move(sub);
    // TODO
    state_.input.request(1);
  }

  auto disposed() const noexcept -> bool override {
    return state_.output.valid();
  }

  void do_dispose(bool from_external) override {
    TENZIR_ASSERT(not disposed());
    if (from_external) {
      state_.output.on_error(caf::make_error(caf::sec::disposed));
    } else {
      state_.output.release_later();
    }
  }

private:
  void activate(caf::flow::observer<exec::message<table_slice>> out) override {
    TENZIR_ASSERT(not state_.output);
    state_.output = std::move(out);
  }

  auto start(handshake hs) -> handshake_response {
    // TODO
    auto input = as<exec::stream<table_slice>>(hs.input);
    auto output
      = start(self_->observe(std::move(input), 10, 30))
          .to_typed_stream("output-stream", std::chrono::milliseconds{1}, 1);
    return handshake_response{std::move(output)};
  }

  auto start(observable<table_slice> input) -> observable<table_slice> {
    auto op = caf::make_counted<
      trampoline_op<message<table_slice>, message<table_slice>>>(
      *this, std::move(input));
    return caf::flow::observable<message<table_slice>>{std::move(op)};
  }

  void on_complete() override {
    TENZIR_WARN("=> on complete");
    state_.output.on_complete();
  }

  void on_error(const caf::error& what) override {
    TENZIR_ERROR("=> on error: {}", what);
  }

  operator_actor::pointer self_;
  actor_state state_;
  std::unique_ptr<stateless_base> impl_;
  std::vector<exec::message<table_slice>> buffer_;
};

template <class T, class... Ts>
auto spawn_operator(plan::operator_spawn_args args,
                    typename T::state_type state, Ts&&... xs)
  -> operator_actor {
  if (args.restore) {
    // TODO: Not assert?
    TENZIR_ASSERT(*args.restore);
    auto bytes = as_bytes(*args.restore);
    auto deserializer = caf::binary_deserializer{
      caf::const_byte_span{bytes.data(), bytes.size()}};
    auto ok = deserializer.apply(state);
    TENZIR_ASSERT(ok);
  }
  TENZIR_TODO();
  // return args.sys.spawn(
  //   caf::actor_from_state<exec::operator_impl>,
  //   [&](exec::actor_state& actor_state) {
  //     return std::make_unique<T>(
  //       typename T::initializer{
  //         actor_state,
  //         std::move(state),
  //       },
  //       std::forward<Ts>(xs)...);
  //   },
  //   std::move(args));
}
#endif

} // namespace tenzir::exec
