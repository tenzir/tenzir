//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/atoms.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/flow/observable_builder.hpp>
#include <caf/flow/observer.hpp>
#include <caf/fwd.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

struct checkpoint {};

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

struct handshake {
  stream_t input;
  rollback_manager_actor rollback_manager;
  std::optional<logical_pipeline> self_and_remainder;
  operator_id id;
  done_handler_actor done_handler;

  // ast::pipeline remaining;
  // operator_id previous_id;
  // operator_id current_id;
};

struct handshake_result {
  stream_t output;
};

} // namespace tenzir

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::handshake);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::operator_id);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::handshake_result);

namespace tenzir {

struct pipeline_executor_actor_traits {
  using signatures = caf::type_list<
    // Start a pipeline.
    auto(ast::pipeline, handshake)->caf::result<handshake>,
    // Start a closed pipeline.
    auto(ast::pipeline)->caf::result<void>>;
};

// FIXME:
using pipeline_executor_actor
  = caf::typed_actor<pipeline_executor_actor_traits>;

struct physical_operator_actor_traits {
  using signatures = caf::type_list<
    // Add an input to the operator. Must be called exactly once. Returns when
    // operator can be considered running.
    auto(handshake)->caf::result<handshake_result>>;
};

using physical_operator_actor = caf::typed_actor<physical_operator_actor_traits>
  // TODO: check
  ::extend_with<done_handler_actor>;

auto magic_spawn(ast::statement&&) -> physical_operator_actor;

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

// struct operator_plugin_expert {
//   virtual auto make_physical_operator(...) -> physical_operator = 0;
// };

// template <class Input, class Output>
// struct operator_plugin_intermediate {
//   virtual auto start() -> void;
//   virtual auto make_flow(caf::flow::observable<message<Input>>)
//     -> caf::flow::observable<message<Output>>
//     = 0;
// };

// template <class Input, class Output>
// struct flow_operator {
//   virtual auto on_init() -> void = 0;
//   virtual auto on_next(Input, flow_operator& next) -> bool = 0;
//   virtual auto on_complete() -> void = 0;
//   virtual auto on_error(caf::error) -> void = 0;
// };

// template <class Input, class Output>
// struct stateless_operator {
//   virtual auto map(Input) -> Output = 0;
// };

// template <class Input, class Output>
// struct generator_operator {
//   virtual auto make_generator(generator<message<Input>>)
//     -> generator<message<Output>>
//     = 0;
// };

// template <class Input, class Output>
// struct mvo {
//   virtual auto process(Input) -> generator<Output>;
//   virtual auto done() -> bool;

//   auto inspect(auto& f, mvo& x) -> bool {
//     return f.object(x).fields();
//   }
// };

// template <class Input, class Output>
// struct mvo2 {
//   virtual auto process(caf::flow::observer<Input>)
//     -> caf::flow::observer<Output>;
//   virtual auto done() -> bool;

//   auto inspect(auto& f, mvo2& x) -> bool {
//     return f.object(x).fields();
//   }
// };

// struct head_operator {
//   auto make_flow(auto flow) -> caf::flow::observer<table_slice> {
//     return std::move(flow)
//       .map([this](table_slice x) {
//         const auto remaining = std::exchange(
//           remaining_, remaining_ - std::min(x.rows(), remaining_));
//         return head(std::move(x), remaining);
//       })
//       .take_while([](const table_slice& x) -> bool {
//         return x.rows() > 0;
//       });
//   }

//   auto inspect(auto& f, head_operator& x) {
//     return f.object(x).fields(f.field(remaining_));
//   }

//   uint64_t remaining_;
// };

auto spawn_and_init_or_restore(auto* self, handshake hs){return };

struct head_state {
  explicit head_state(physical_operator_actor::pointer self) : self{self} {
  }

  auto do_handshake(handshake hs) -> caf::result<handshake_result> {
    auto& stream = as<caf::typed_stream<message<table_slice>>>(hs.input);
    if (hs.self_and_remainder) {
      //
    } else {
      self->mail(atom::read_v, hs.id)
        .request(hs.rollback_manager, caf::infinite)
        .as_observable()
        .flat_map([this](caf::typed_stream<chunk_ptr> stream) {
          return self->observe(stream, 30, 10);
        })
        .reduce(std::vector<std::byte>{},
                [](chunk_ptr chunk, std::vector<std::byte> acc) {
                  acc.insert(acc.end(), chunk->begin(), chunk->end());
                  return acc;
                })
        .for_each([this](std::vector<std::byte> buffer) {
          auto f = caf::binary_deserializer{nullptr, buffer};
          auto success = f.apply(*this);
          TENZIR_ASSERT(success);
        });
    }

    input.input
      = self_->observe(stream, 30, 10)
          .concat_map([this](message<table_slice> msg) {
            return match(
              msg.kind,
              [this](
                checkpoint x) -> caf::flow::observable<message<table_slice>> {
                return self_->make_observable()
                  .just(message<table_slice>{x})
                  .as_observable();
              },
              [this](table_slice x)
                -> caf::flow::
                  observable<message<table_slice>> {
                    const auto dest = caf::typed_actor<
                      auto(table_slice)->caf::result<message<table_slice>>>{};
                    return self_->mail(std::move(x))
                      .request(dest, caf::infinite)
                      .as_observable();
                  });
          })
          .to_typed_stream("no name", duration::zero(), 1);
    input.input = self_->observe(stream, 30, 10)
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
    return self->mail(std::move(input)).delegate(next);

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
  }

  auto make_behavior() -> physical_operator_actor::behavior_type {
    // Startup logic can be put here.
    return {
      [this](handshake input) -> caf::result<handshake_result> {
        return do_handshake(std::move(input));
      },
    };
  }

  friend auto inspect(auto& f, head_state& x) {
    return f.object(x).fields(f.field("remaining", x.remaining));
  }

  physical_operator_actor::pointer self;
  size_t remaining = 42;
};

// What even is this???
struct operator_base2 {};

struct compile_ctx {};

auto compile(ast::pipeline&& pipe,
             std::vector<std::pair<std::string, data>> bindings,
             compile_ctx ctx) -> logical_pipeline;

} // namespace tenzir
