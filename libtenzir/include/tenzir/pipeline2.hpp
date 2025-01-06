//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"

#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

struct checkpoint {};

template <class T>
struct message {
  variant<checkpoint, T> kind;
};

struct handshake {
  std::optional<variant<caf::typed_stream<message<table_slice>>,
                        caf::typed_stream<message<chunk_ptr>>,
                        caf::typed_stream<message<chunk_ptr>>>>
    stream;

  int rollback_manager;
  ast::pipeline remaining;
};

} // namespace tenzir

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::handshake);

namespace tenzir {

using physical_operator = caf::typed_actor<
  // Add an input to the operator. Must be called exactly once. Returns when
  // operator can be considered running.
  auto(handshake)->caf::result<handshake>>;

auto magic_spawn(ast::statement&&) -> physical_operator;

auto start(ast::pipeline pipe, handshake hs) -> caf::result<handshake> {
  // (), (every { … }), ()
  std::vector<bool> operators;
  std::vector<std::monostate> exec_nodes;
  // A -> A' -> B -> C
  //                 ^ spawned by B
  // remote_spawn();

  hs.remaining = std::move(pipe);
  auto op = magic_spawn(std::move(pipe.body[0]));
  hs.remaining.body.erase(hs.remaining.body.erase(hs.remaining.body.begin()));
  return self->mail(std::move(hs)).delegate(op);
}

struct physical_operator_state {
  physical_operator_state(physical_operator::pointer self) : self_{self} {
  }

  auto make_behavior() -> physical_operator::behavior_type {
    // Startup logic can be put here.
    return {
      [this](handshake input) -> caf::result<handshake> {
        TENZIR_ASSERT(input.stream);
        auto& stream
          = as<caf::typed_stream<message<table_slice>>>(*input.stream);
        input.stream = self_->observe(stream, 30, 10)
                         .map([this](message<table_slice> msg) {
                           match(
                             msg.kind,
                             [this](table_slice& x) {
                               // no op
                               auto c = std::min(x.rows(), remaining_);
                               remaining_ -= c;
                               x = head(x, c);
                             },
                             [this](checkpoint x) {
                               auto s = caf::binary_serializer{};
                               s(remaining_);
                               self_->mail(s.finish())
                                 .request(rollback_manager_)
                                 .then(
                                   []() {
                                     // Commit.
                                   },
                                   [this](caf::error err) {
                                     // hard failure
                                     self_->quit(std::move(err));
                                   });
                             });
                           return msg;
                         })
                         .take_while([this](message<table_slice>&) {
                           return remaining_ > 0;
                         })
                         .to_typed_stream("no name", duration::zero(), 1);
        if (input.remaining.body.empty()) {
          return input;
        }
        auto next_op = std::move(input.remaining.body[0]);
        input.remaining.body.erase(input.remaining.body.begin());
        auto next = magic_spawn(std::move(next_op));
        return self_->mail(std::move(input)).delegate(next);

        // message -> done | message

        // self_->observe(input, 30, 10)
        //   .map([](message input) {
        //     return f(input);
        //   })
        //   .take_while([](variant<...>) {
        //     return done;
        //   })
        //   .map([](variant<...>) {
        //     return std::get<...>
        //   })
        //   .to;

        self_->observe(input, 30, 10)
          .flat_map([this](message x) {
            return self_->mail(x).request(foo, caf::infinite).as_observable();
          })
          .merge(self_->observe(input, 30, 10))
          .to_typed_stream("no name", caf::timespan::zero(), 1);
        return {};
      },
    };
  }

  physical_operator::pointer self_;
  size_t remaining_ = 42;
  int rollback_manager_ = 1337;
};

// What even is this???
struct operator_base2 {};

struct compile_ctx {};

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

  std::vector<variant<invocation2, let2>>

  //    / X \
  // S -- Y -- D
  //    \ Z /

  // s { x }, { y }, { z }
  // d
};

auto compile(ast::pipeline&& pipe,
             std::vector<std::pair<std::string, data>> bindings,
             compile_ctx ctx) -> logical_pipeline;

} // namespace tenzir
