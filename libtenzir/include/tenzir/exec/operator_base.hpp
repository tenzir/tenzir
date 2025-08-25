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
#include "tenzir/base_ctx.hpp"
#include "tenzir/exec/actors.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/event_based_mail.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_stream.hpp>

#if 0
namespace tenzir::exec {

// TODO: Use actor struct directly.
struct actor_state {
  actor_state(base_ctx ctx, operator_actor::pointer self,
              checkpoint_receiver_actor checkpoint_receiver,
              shutdown_handler_actor shutdown_handler,
              stop_handler_actor stop_handler)
    : ctx{ctx},
      self{self},
      checkpoint_receiver{std::move(checkpoint_receiver)},
      shutdown_handler{std::move(shutdown_handler)},
      stop_handler{std::move(stop_handler)} {
  }

  base_ctx ctx;
  operator_actor::pointer self;
  checkpoint_receiver_actor checkpoint_receiver{};
  shutdown_handler_actor shutdown_handler{};
  stop_handler_actor stop_handler{};
  bool ready = true;
  caf::flow::subscription input;
  caf::flow::observer<message<table_slice>> output;
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

  /// Called when the downstream operator requires further input.
  virtual void request(size_t n) = 0;

  /// Called when the operator needs to serialize its state, e.g., because it
  /// was asked to create a checkpoint.
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
      .request(actor_state_.shutdown_handler, caf::infinite)
      .then([] {});
    actor_state_.self->mail(atom::stop_v)
      .request(actor_state_.stop_handler, caf::infinite)
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

} // namespace tenzir::exec
#endif
