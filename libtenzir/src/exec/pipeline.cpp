//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/exec/pipeline.hpp"

#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/exec/operator.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/async/producer_adapter.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_stream.hpp>

namespace tenzir::exec {

namespace {

class checkpoint_receiver {
public:
  static constexpr auto name = "tenzir.exec.checkpoint-receiver";

  explicit checkpoint_receiver(checkpoint_receiver_actor::pointer self, uuid id,
                               uint64_t index)
    : self_{self}, id_{id}, index_{index} {
    (void)self_;
  }

  auto make_behavior() -> checkpoint_receiver_actor::behavior_type {
    return {
      [this](checkpoint, chunk_ptr chunk) -> caf::result<void> {
        TENZIR_ASSERT(chunk);
        TENZIR_WARN("checkpoint receiver got {} bytes from {}/{}",
                    chunk->size(), id_, index_);
        return {};
      },
    };
  }

private:
  checkpoint_receiver_actor::pointer self_;
  uuid id_;
  uint64_t index_;
};

struct internal_pipeline_actor_traits {
  using signatures = caf::type_list<>
    //
    ::append_from<pipeline_actor::signatures, shutdown_actor::signatures>;
};

using internal_pipeline_actor
  = caf::typed_actor<internal_pipeline_actor_traits>;

class pipeline {
public:
  static constexpr auto name = "tenzir.exec.pipeline";

  pipeline(internal_pipeline_actor::pointer self, plan::pipeline pipe,
           pipeline_settings settings,
           std::optional<checkpoint_reader_actor> checkpoint_reader,
           base_ctx ctx)
    : self_{self},
      pipe_{std::move(pipe)},
      checkpoint_reader_{std::move(checkpoint_reader)},
      ctx_{ctx},
      settings_{std::move(settings)} {
  }

  auto make_behavior() -> internal_pipeline_actor::behavior_type {
    return {
      [this](connect_t connect) -> caf::result<void> {

      },
      [this](atom::start) -> caf::result<void> {
        return start();
      },
      [this](atom::start, handshake hs) -> caf::result<handshake_response> {
        return start(std::move(hs));
      },
      [this](atom::shutdown) -> caf::result<void> {
        // TODO: Could this come before we are fully spawned?
        TENZIR_ASSERT(shutdown_count_ < operators_.size());
        shutdown_count_ += 1;
        TENZIR_WARN("got ready to shutdown from {} operators", shutdown_count_);
        check_for_shutdown();
        return {};
      },
      [](atom::stop) -> caf::result<void> {
        // TODO: We probably need to forward this signal for nested pipelines.
        TENZIR_WARN("executor got stop???");
        return {};
      },
    };
  }

private:
  auto all_operators_are_ready_to_shutdown() const -> bool {
    return shutdown_count_ == operators_.size();
  }

  /// This returns true if the post-commits for all checkpoints are completed.
  auto no_in_flight_checkpoints() const -> bool {
    return checkpoints_in_flight_ == 0;
  }

  /// To begin shutdown, all operators must declare that they are ready for it.
  /// This is because once we start the shutdown, we lose the ability to emit
  /// checkpoints, so all longer-running computations should already be
  /// completed at that point. Furthermore, we ensure that all post-commit steps
  /// were executed, because we can still fail during shutdown and don't want to
  /// have partial checkpoints for that.
  void check_for_shutdown() {
    if (all_operators_are_ready_to_shutdown() and no_in_flight_checkpoints()) {
      TENZIR_WARN("BEGINNING SHUTDOWN");
      TENZIR_ASSERT(producer_);
      producer_.close();
    }
  }

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
    // TODO: Maybe not one actor per operator. Can inspect sender instead.
    auto checkpointer = self_->spawn(caf::actor_from_state<checkpoint_receiver>,
                                     pipe_.id(), index);
    // TODO: Remote spawn.
    TENZIR_WARN("spawning {} operator", pipe_[index]->name());
    auto previous = std::invoke([&]() -> stop_handler_actor {
      if (operators_.empty()) {
        // TODO: Do we even need this?
        return self_;
      } else {
        return operators_.back();
      }
    });
    operators_.push_back(pipe_[index]->spawn(plan::operator_spawn_args{
      self_->system(),
      ctx_,
      std::move(checkpointer),
      self_,
      std::move(previous),
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

  auto finish_start(caf::expected<handshake_response> hr,
                    caf::typed_response_promise<void> rp) {
    if (not hr) {
      rp.deliver(std::move(hr.error()));
      return;
    }
    auto output = try_as<caf::typed_stream<message<void>>>(hr->output);
    if (not output) {
      // TODO: Can we always detect this before?
      diagnostic::error("pipeline is not closed").emit(ctx_);
      rp.deliver(ec::silent);
      self_->quit(ec::silent);
      return;
    }
    self_->observe(*output, 30, 10)
      .do_on_error([this](caf::error err) {
        TENZIR_ERROR("from stream: {}", err);
        self_->quit(std::move(err));
      })
      .do_on_complete([this] {
        TENZIR_WARN("complete");
        self_->quit();
      })
      .for_each([this](message<void> msg) {
        match(
          msg,
          [&](checkpoint checkpoint) {
            // TODO: We first have to actually commit. Do we notify the
            // checkpoint receiver actor?
            TENZIR_WARN("checkpoint arrived at end: performing post-commit");
            auto remaining = std::make_shared<size_t>(operators_.size());
            for (auto& op : operators_) {
              self_->mail(checkpoint)
                .request(op, caf::infinite)
                .then(
                  [this, op, remaining]() {
                    TENZIR_WARN("post-commit for {} succeeded", op);
                    TENZIR_ASSERT(*remaining > 0);
                    *remaining -= 1;
                    if (*remaining == 0) {
                      TENZIR_WARN("all post-commits completed");
                      checkpoints_in_flight_ -= 1;
                      check_for_shutdown();
                    }
                  },
                  [this](caf::error error) {
                    // TODO: How exactly do we handle errors?
                    TENZIR_ERROR("oh no: {}", error);
                    self_->quit(std::move(error));
                  });
            }
          },
          [](exhausted) {
            TENZIR_WARN("exhausted arrived at end");
          });
      });
    rp.deliver();
  }

  auto start() -> caf::result<void> {
#if 1
    for (auto& op : pipe_) {
      // Note: This could need to be async if we spawn the actor somewhere else.
      operators_.push_back(op->spawn(
        plan::operator_spawn_args{self_->system(), ctx_, std::nullopt}));
    }
    for (auto& op : operators_) {
      auto upstream
        = upstream_actor{&op == &operators_.front() ? self_ : *(&op - 1)};
      auto downstream
        = downstream_actor{&op == &operators_.back() ? self_ : *(&op + 1)};
      self_
        ->mail(connect_t{
          std::move(upstream),
          std::move(downstream),
          // TODO
          caf::actor_cast<checkpoint_receiver_actor>(nullptr),
          shutdown_actor{self_},
        })
        .request(op, caf::infinite)
        .then(
          [this] {
            connected_ += 1;
            TENZIR_ASSERT(connected_ <= operators_.size());
            if (connected_ == operators_.size()) {
              // Inform operators that everything has been connected.
              // TODO: Is this necessary?
              for (auto& op : operators_) {
                self_->mail(atom::start_v)
                  .request(op, caf::infinite)
                  .then(
                    [] {

                    },
                    [](caf::error) {

                    });
              }
            }
          },
          [this](caf::error err) {
            // TODO
            TENZIR_ERROR("{}", err);
            self_->quit(err);
          });
    }
#else
    auto rp = self_->make_response_promise<void>();
    spawn([this, rp]() mutable {
      auto [c, p] = caf::async::make_spsc_buffer_resource<message<void>>();
      auto producer = caf::async::make_producer_adapter(
        std::move(p), self_, caf::make_action([] { /* on resume */ }),
        caf::make_action([] { /* on cancel */ }));
      TENZIR_ASSERT(producer);
      producer_ = std::move(*producer);
      if (settings_.checkpoints_in_flight > 0) {
        TENZIR_ASSERT(settings_.checkpoint_interval > duration::zero());
        TENZIR_WARN("beginning checkpoint stream");
        detail::weak_run_delayed_loop(
          self_, settings_.checkpoint_interval,
          [this] {
            // TODO: How exactly does shutdown interact with checkpointing?
            if (all_operators_are_ready_to_shutdown()) {
              return;
            }
            if (checkpoints_in_flight_ >= settings_.checkpoints_in_flight) {
              TENZIR_WARN("skipping checkpoint because too many in flight");
              return;
            }
            // TODO: How does this interact with backpressure? Do we even get
            // backpressure here?
            TENZIR_WARN("emitting checkpoint");
            checkpoints_in_flight_ += 1;
            producer_.push(checkpoint{});
          },
          true);
      }
      auto initial = c.observe_on(self_).to_typed_stream(
        "initial", std::chrono::milliseconds{1}, 1);
      continue_start(handshake{std::move(initial)}, 0,
                     [this, rp](caf::expected<handshake_response> hr) mutable {
                       finish_start(std::move(hr), std::move(rp));
                     });
    });
    return rp;
#endif
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

  internal_pipeline_actor::pointer self_;
  plan::pipeline pipe_;
  std::optional<checkpoint_reader_actor> checkpoint_reader_;
  base_ctx ctx_;
  std::vector<operator_actor> operators_;
  size_t connected_ = 0;
  size_t shutdown_count_ = 0;
  size_t checkpoints_in_flight_ = 0;
  caf::async::producer_adapter<exec::message<void>> producer_;
  pipeline_settings settings_;
};

} // namespace

auto make_pipeline(plan::pipeline pipe, pipeline_settings settings,
                   std::optional<checkpoint_reader_actor> checkpoint_reader,
                   base_ctx ctx) -> pipeline_actor {
  return ctx.system().spawn(caf::actor_from_state<pipeline>, std::move(pipe),
                            std::move(settings), std::move(checkpoint_reader),
                            ctx);
}

} // namespace tenzir::exec
