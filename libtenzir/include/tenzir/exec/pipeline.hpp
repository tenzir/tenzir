//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/bp.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/operator_actor.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_stream.hpp>

namespace tenzir::exec {

struct pipeline_actor_traits {
  using signatures = caf::type_list<
    //
    auto(atom::start)->caf::result<void>,
    //
    auto(atom::start, handshake hs)->caf::result<handshake_response>>;
};

using pipeline_actor = caf::typed_actor<pipeline_actor_traits>;

enum class restore { yes, no };

class checkpoint_receiver {
public:
  explicit checkpoint_receiver(checkpoint_receiver_actor::pointer self, uuid id,
                               uint64_t index)
    : self_{self}, id_{id}, index_{index} {
  }

  auto make_behavior() -> checkpoint_receiver_actor::behavior_type {
    return {
      [this](checkpoint, chunk_ptr) -> caf::result<void> {
        TENZIR_WARN("got checkpoint for {}/{}", id_, index_);
        return {};
      },
    };
  }

private:
  checkpoint_receiver_actor::pointer self_;
  uuid id_;
  uint64_t index_;
};

struct checkpoint_reader_traits {
  using signatures
    = caf::type_list<auto(uuid id, uint64_t index)->caf::result<chunk_ptr>>;
};

using checkpoint_reader_actor = caf::typed_actor<checkpoint_reader_traits>;

class pipeline {
public:
  pipeline(pipeline_actor::pointer self, bp::pipeline pipe,
           std::optional<checkpoint_reader_actor> checkpoint_reader,
           base_ctx ctx)
    : self_{self},
      pipe_{std::move(pipe)},
      checkpoint_reader_{std::move(checkpoint_reader)},
      ctx_{ctx} {
  }

  auto make_behavior() -> pipeline_actor::behavior_type {
    return {
      [this](atom::start) -> caf::result<void> {
        return start();
      },
      [this](atom::start, handshake hs) -> caf::result<handshake_response> {
        return start(std::move(hs));
      },
    };
  }

private:
  void continue_start(
    handshake hs, size_t next,
    std::function<void(caf::expected<handshake_response>)> callback) {
    TENZIR_ASSERT(next <= operators_.size());
    if (next == operators_.size()) {
      TENZIR_WARN("start complete");
      return callback(handshake_response{std::move(hs.input)});
    }
    TENZIR_WARN("handshaking with {}", pipe_[next]->name());
    self_->mail(std::move(hs))
      .request(operators_[next], caf::infinite)
      .then(
        // TODO: uncopy.
        [this, next, callback](handshake_response hr) mutable {
          continue_start(handshake{std::move(hr.output)}, next + 1,
                         std::move(callback));
        },
        [callback](caf::error err) mutable {
          // TODO: Additional info? Diagnostics?
          callback(std::move(err));
        });
  }

  void continue_spawn_with_chunk(std::optional<chunk_ptr> chunk,
                                 detail::unique_function<void()> callback) {
    auto index = operators_.size();
    TENZIR_ASSERT(index < pipe_.size());
    auto checkpointer = self_->spawn(caf::actor_from_state<checkpoint_receiver>,
                                     pipe_.id(), index);
    // TODO: Remote spawn.
    TENZIR_WARN("spawning {} operator", pipe_[index]->name());
    operators_.push_back(pipe_[index]->spawn(bp::operator_base::spawn_args{
      self_->system(),
      ctx_,
      std::move(checkpointer),
      std::move(chunk),
    }));
    continue_spawn(std::move(callback));
  }

  void continue_spawn(detail::unique_function<void()> callback) {
    TENZIR_ASSERT(operators_.size() <= pipe_.size());
    if (operators_.size() == pipe_.size()) {
      TENZIR_WARN("spawning complete");
      callback();
      return;
    }
    if (checkpoint_reader_) {
      auto index = operators_.size();
      self_->mail(pipe_.id(), index)
        .request(*checkpoint_reader_, caf::infinite)
        .then(
          [this, callback = std::move(callback)](chunk_ptr chunk) mutable {
            continue_spawn_with_chunk(std::move(chunk), std::move(callback));
          },
          [this](caf::error error) {
            // TODO
            self_->quit(std::move(error));
          });
    }
    continue_spawn_with_chunk(std::nullopt, std::move(callback));
  }

  void spawn(detail::unique_function<void()> callback) {
    TENZIR_WARN("begin spawning");
    continue_spawn(std::move(callback));
  }

  auto start() -> caf::result<void> {
    auto rp = self_->make_response_promise<void>();
    spawn([this, rp]() mutable {
      auto initial = self_->make_observable()
                       .just(message<void>{})
                       .to_typed_stream("initial", duration::zero(), 1);
      continue_start(handshake{std::move(initial)}, 0,
                     [this, rp](caf::expected<handshake_response> hr) mutable {
                       if (not hr) {
                         rp.deliver(std::move(hr.error()));
                         return;
                       }
                       auto output
                         = try_as<caf::typed_stream<message<void>>>(hr->output);
                       if (not output) {
                         // TODO: ERROR?
                         TENZIR_TODO();
                       }
                       self_->observe(*output, 30, 10)
                         .do_on_error([this](caf::error err) {
                           TENZIR_WARN("error");
                           self_->quit(std::move(err));
                         })
                         .do_on_complete([this] {
                           TENZIR_WARN("complete");
                           self_->quit();
                         })
                         .for_each([](message<void>) {
                           TENZIR_WARN("checkpoint arrived at exec");
                           // TODO: Do we trigger post-commit here?
                         });
                       rp.deliver();
                     });
    });
    return rp;
  }

  auto start(handshake hs) -> caf::result<handshake_response> {
    auto rp = self_->make_response_promise<handshake_response>();
    spawn([this, rp, hs = std::move(hs)]() mutable {
      continue_start(handshake{std::move(hs)}, 0,
                     [rp](caf::expected<handshake_response> hr) mutable {
                       rp.deliver(std::move(hr));
                     });
    });
    return rp;
  }

  pipeline_actor::pointer self_;
  bp::pipeline pipe_;
  std::optional<checkpoint_reader_actor> checkpoint_reader_;
  base_ctx ctx_;
  std::vector<operator_actor> operators_;
};

} // namespace tenzir::exec
