//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/argument_parser2.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/parser_interface.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/flow/observable_builder.hpp>
#include <caf/flow/observer.hpp>
#include <caf/fwd.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <queue>
#include <random>
#include <utility>

namespace tenzir {

struct checkpoint {
  int64_t id;
};

struct timed_checkpoint {
  struct checkpoint checkpoint = {};
  std::chrono::steady_clock::time_point created_at
    = std::chrono::steady_clock::now();
};

template <class T>
struct message {
  variant<checkpoint, T> kind;
};

struct operator_id {
  friend auto inspect(auto& f, operator_id& x) -> bool {
    return f.object(x).fields();
  }
};

struct rollback_manager_actor_traits {
  using signatures = caf::type_list<
    //
    auto(atom::write, operator_id, checkpoint, caf::typed_stream<chunk_ptr>)
      ->caf::result<void>,
    // load
    auto(atom::read, operator_id)->caf::result<caf::typed_stream<chunk_ptr>>>;
};

// - Need to be able to restore subpipelines, e.g., TCP connections after a
//   crash, which means they must be uniquely identified.
// - Must be known to all pipelines that wish to preserve state.
using rollback_manager_actor = caf::typed_actor<rollback_manager_actor_traits>;

// - store `operator_ptr` somewhere somehow
class logical_pipeline {
public:
  // let $stmt = 1
  // dasd …
  // dasdas $stm, yo=42
  //          ?      ^

  // let $dassa = 2

  // let $stmt = 1
  // let $dassa = 2

  // std::vector<variant<invocation2, let2>>

  //    / X \
  // S -- Y -- D
  //    \ Z /

  // s { x }, { y }, { z }
  // d
};

using stream_t = variant<
  caf::typed_stream<message<table_slice>>, caf::typed_stream<message<chunk_ptr>>,
  caf::typed_stream<message<chunk_ptr>>, caf::typed_stream<checkpoint>>;

struct done_handler_actor_traits {
  using signatures = caf::type_list<
    //
    auto(atom::done)->caf::result<void>>;
};

using done_handler_actor = caf::typed_actor<done_handler_actor_traits>;

struct intermediate_pipeline_representation {
  static auto make(ast::pipeline def, session ctx)
    -> failure_or<intermediate_pipeline_representation> {
    // FIXME
    return failure::promise();
  }
};

struct logical_operator {
  struct magic_args {};

  std::string plugin_name;
  std::variant<magic_args, std::string> args_or_restore_id;
};

struct handshake {
  stream_t input;
  rollback_manager_actor rollback_manager;
  std::vector<logical_operator> remaining_operators;
  operator_id id;
  done_handler_actor done_handler;
};

struct handshake_result {
  caf::typed_stream<checkpoint> output;
};

} // namespace tenzir

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::handshake);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::operator_id);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::handshake_result);

namespace tenzir {

// -- old

struct physical_operator_actor_traits {
  using signatures = caf::type_list<
    // Add an input to the operator. Must be called exactly once. Returns when
    // operator can be considered running.
    auto(handshake)->caf::result<handshake_result>>;
};

using physical_operator_actor = caf::typed_actor<physical_operator_actor_traits>
  // TODO: check
  ::extend_with<done_handler_actor>;

struct stuff_needed_to_spawn_an_operator {};

struct expert_operator_plugin : public virtual plugin {
  virtual auto spawn_operator(stuff_needed_to_spawn_an_operator)
    -> physical_operator_actor
    = 0;
  virtual auto restore_operator(chunk_ptr data) -> physical_operator_actor = 0;
};

struct head_state {
  explicit head_state(physical_operator_actor::pointer self) : self{self} {
  }

  auto do_handshake(handshake hs) -> caf::result<handshake_result> {
    // We need to spawn the next operator. For this, we require one of the
    // following:
    // 1. Arguments required to spawn the next operator.
    // 2. The plugin name for the next operator and a way to spawn it.
    const auto* next_operator = plugins::find<expert_operator_plugin>("FIXME");
    TENZIR_ASSERT(next_operator); // FIXME

    return ec::unimplemented;
  }

  auto done() const -> caf::result<void> {
    // FIXME
    return ec::unimplemented;
  }

  auto make_behavior() -> physical_operator_actor::behavior_type {
    // Startup logic can be put here.
    return {
      [this](handshake input) -> caf::result<handshake_result> {
        return do_handshake(std::move(input));
      },
      [this](atom::done) -> caf::result<void> {
        return done();
      },
    };
  }

  friend auto inspect(auto& f, head_state& x) {
    return f.object(x).fields(f.field("remaining", x.remaining));
  }

  physical_operator_actor::pointer self;
  size_t remaining = 42;
};

// -- pipeline actor

struct pipeline_actor_traits {
  using signatures = caf::type_list<
    // Start the closed pipeline.
    auto(atom::start)->caf::result<void>>;
};
using pipeline_actor = caf::typed_actor<pipeline_actor_traits>;

class pipeline_actor_state {
public:
  // FIXME: What representation of the pipeline do we pass in at construction
  // time (or rollback info)?
  explicit pipeline_actor_state(pipeline_actor::pointer self) : self{self} {
  }

  void commit(checkpoint chk) {
    TENZIR_ASSERT(chk.id == pending_checkpoints.front().checkpoint.id);
    const auto elapsed_time = std::chrono::steady_clock::now()
                              - pending_checkpoints.front().created_at;
    TENZIR_UNUSED(elapsed_time); // FIXME
    pending_checkpoints.pop();
    // FIXME: Actually commit something, then trigger post-commits.
  }

  auto start() -> caf::result<void> {
    using namespace std::chrono_literals;
    // Generate a stream of checkpoints.
    auto checkpoints
      = self->make_observable()
          .interval(30s)
          .skip(1)
          .map([this](int64_t id) {
            return pending_checkpoints.emplace(checkpoint{id}).checkpoint;
          })
          .to_typed_stream<checkpoint>("checkpoints", 0s, 1);
    // FIXME: Spawn the first operator.
    auto first_op = physical_operator_actor{};

    // FIXME: Create initial handshake.
    auto hs = handshake{
      .input = std::move(checkpoints),
    };

    // Send handshake to first operator.
    auto rp = self->make_response_promise<void>();
    self->mail(std::move(hs))
      .request(first_op, caf::infinite)
      .as_observable()
      .flat_map([this, rp](handshake_result hr) mutable {
        // Signal that the startup sequence has completed.
        TENZIR_ASSERT(rp.pending());
        rp.deliver();
        // Then continue working on the checkpoint stream.
        return self->observe(std::move(hr.output), 30, 10);
      })
      .do_finally([this](caf::error err) {
        // FIXME: Need to only quit _after_ all commits are done.
        self->quit(std::move(err));
      })
      .for_each([this](checkpoint chk) {
        commit(chk);
      });
    return rp;
  }

  auto exit(caf::exit_msg msg) {
    self->quit(std::move(msg.reason));
  }

  auto make_behavior() -> pipeline_actor::behavior_type {
    return {
      [this](atom::start) {
        return start();
      },
      [this](caf::exit_msg msg) {
        return exit(std::move(msg));
      },
    };
  }

private:
  pipeline_actor::pointer self = {};

  std::queue<timed_checkpoint> pending_checkpoints = {};
};

// What even is this???
struct operator_base2 {};

struct compile_ctx {};

auto compile(ast::pipeline&& pipe,
             std::vector<std::pair<std::string, data>> bindings,
             compile_ctx ctx) -> logical_pipeline;

} // namespace tenzir
