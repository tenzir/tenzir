//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/exec/pipeline.hpp>
#include <tenzir/exec/trampoline.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>

namespace tenzir::exec {

// TODO: Use actor struct directly.
struct actor_state {
  actor_state(base_ctx ctx, exec::operator_actor::pointer self,
              exec::checkpoint_receiver_actor checkpoint_receiver,
              exec::operator_shutdown_actor operator_shutdown,
              exec::operator_stop_actor operator_stop)
    : ctx{ctx},
      self{self},
      checkpoint_receiver{std::move(checkpoint_receiver)},
      operator_shutdown{std::move(operator_shutdown)},
      operator_stop{std::move(operator_stop)} {
  }

  base_ctx ctx;
  exec::operator_actor::pointer self;
  exec::checkpoint_receiver_actor checkpoint_receiver{};
  exec::operator_shutdown_actor operator_shutdown{};
  exec::operator_stop_actor operator_stop{};
  bool ready = true;
  caf::flow::subscription input;
  caf::flow::observer<exec::message<table_slice>> output;
};

class stateless_base {
public:
  virtual ~stateless_base() = default;

  /// Called after the stream has been set up.
  virtual void init() = 0;

  /// Called when there is new input.
  virtual void next(const table_slice& slice) = 0;

  /// Called when the input ends.
  virtual void set_input_ended() = 0;

  /// Called before and after every input.
  virtual auto should_stop() -> bool = 0;

  /// Call this when the operator is done.
  virtual void stop() = 0;

  virtual void request(size_t n) = 0;
  virtual auto serialize() -> chunk_ptr = 0;
};

template <class State>
class operator_base : public stateless_base {
public:
  using state_type = State;

  struct initializer {
    actor_state& actor_state;
    State state;
  };

  explicit operator_base(initializer init)
    : state_{std::move(init.state)}, actor_state_{init.actor_state} {
  }

  void init() override {
  }

  // TODO: Quite bad. Maybe shouldn't have this state here.
  auto get_input_ended() -> bool {
    return input_ended_;
  }

  void set_input_ended() override {
    input_ended_ = true;
  }

  auto should_stop() -> bool override {
    return input_ended_;
  }

  void request(size_t n) override {
    // TODO: This assumes that we have a 1:1 transformation.
    input().request(n);
  }

  auto serialize() -> chunk_ptr override {
    auto buffer = std::vector<std::byte>{};
    auto serializer = caf::binary_serializer{buffer};
    auto ok = serializer.apply(state_);
    TENZIR_ASSERT(ok);
    return chunk::make(std::move(buffer));
  }

  /// Call this when processing above is finished.
  void ready() {
    TENZIR_WARN("=> marking as ready");
    TENZIR_ASSERT(not actor_state_.ready);
    actor_state_.ready = true;
    // TODO: Allow actor to walk queue next schedule?
  }

  /// Call this when the operator is done.
  void stop() override {
    // TODO: Protect against stopping twice?
    TENZIR_WARN("=> stopping");
    actor_state_.self->mail(atom::done_v)
      .request(actor_state_.operator_shutdown, caf::infinite)
      .then([] {});
    actor_state_.self->mail(atom::stop_v)
      .request(actor_state_.operator_stop, caf::infinite)
      .then([] {});
    actor_state_.output.on_next(exec::exhausted{});
  }

  /// Call this to provide a new output.
  void push(table_slice slice) {
    actor_state_.output.on_next(std::move(slice));
  }

  auto self() -> exec::operator_actor::pointer {
    return actor_state_.self;
  }

  auto state() -> State& {
    return state_;
  }

  auto ctx() const -> base_ctx {
    return actor_state_.ctx;
  }

  auto input() -> caf::flow::subscription& {
    return actor_state_.input;
  }

private:
  bool input_ended_ = false;
  State state_;
  actor_state& actor_state_;
};

class operator_impl
  : public trampoline<exec::message<table_slice>, exec::message<table_slice>> {
public:
  operator_impl(
    exec::operator_actor::pointer self,
    detail::unique_function<auto(actor_state&)->std::unique_ptr<stateless_base>>
      impl,
    bp::operator_base::spawn_args args)
    : self_{self},
      state_{args.ctx, self, std::move(args.checkpoint_receiver),
             std::move(args.operator_shutdown), std::move(args.operator_stop)},
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

  auto start(exec::handshake hs) -> exec::handshake_response {
    // TODO
    auto input = as<exec::stream<table_slice>>(hs.input);
    auto output
      = start(self_->observe(std::move(input), 10, 30))
          .to_typed_stream("output-stream", std::chrono::milliseconds{1}, 1);
    return exec::handshake_response{std::move(output)};
  }

  auto start(exec::observable<table_slice> input)
    -> exec::observable<table_slice> {
    auto op = caf::make_counted<
      trampoline_op<exec::message<table_slice>, exec::message<table_slice>>>(
      *this, std::move(input));
    return caf::flow::observable<exec::message<table_slice>>{std::move(op)};
  }

  void on_complete() override {
    TENZIR_WARN("=> on complete");
    state_.output.on_complete();
  }

  void on_error(const caf::error& what) override {
    TENZIR_ERROR("=> on error: {}", what);
  }

  exec::operator_actor::pointer self_;
  actor_state state_;
  std::unique_ptr<stateless_base> impl_;
  std::vector<exec::message<table_slice>> buffer_;
};

template <class T, class... Ts>
auto spawn_operator(bp::operator_base::spawn_args args,
                    typename T::state_type state, Ts&&... xs)
  -> exec::operator_actor {
  if (args.restore) {
    // TODO: Not assert?
    TENZIR_ASSERT(*args.restore);
    auto deserializer = caf::binary_deserializer{as_bytes(*args.restore)};
    auto ok = deserializer.apply(state);
    TENZIR_ASSERT(ok);
  }
  return args.sys.spawn(
    caf::actor_from_state<exec::operator_impl>,
    [&](exec::actor_state& actor_state) {
      return std::make_unique<T>(
        typename T::initializer{
          actor_state,
          std::move(state),
        },
        std::forward<Ts>(xs)...);
    },
    std::move(args));
}

} // namespace tenzir::exec
