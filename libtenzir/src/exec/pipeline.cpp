//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/exec/pipeline.hpp"

#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/exec/checkpoint.hpp"

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

  explicit checkpoint_receiver(checkpoint_receiver_actor::pointer self)
    : self_{self} {
  }

  auto make_behavior() -> checkpoint_receiver_actor::behavior_type {
    return {
      [this](checkpoint, chunk_ptr chunk) -> caf::result<void> {
        TENZIR_ASSERT(chunk);
        TENZIR_INFO("checkpoint receiver got {} bytes", chunk->size());
        return {};
      },
    };
  }

private:
  checkpoint_receiver_actor::pointer self_;
};

struct internal_subpipeline_actor_traits {
  using signatures = caf::type_list<>
    //
    ::append_from<subpipeline_actor::signatures, shutdown_actor::signatures>;
};

using internal_subpipeline_actor
  = caf::typed_actor<internal_subpipeline_actor_traits>;

struct internal_pipeline_actor_traits {
  using signatures = caf::type_list<>
    //
    ::append_from<pipeline_actor::signatures, shutdown_actor::signatures,
                  upstream_actor::signatures, downstream_actor::signatures>;
};

using internal_pipeline_actor
  = caf::typed_actor<internal_pipeline_actor_traits>;

class subpipeline {
public:
  [[maybe_unused]] static constexpr auto name = "tenzir.exec.subpipeline";

  subpipeline(internal_subpipeline_actor::pointer self, plan::pipeline pipe,
              std::optional<checkpoint_reader_actor> checkpoint_reader,
              base_ctx ctx)
    : self_{self},
      plan_{std::move(pipe)},
      checkpoint_reader_{std::move(checkpoint_reader)},
      ctx_{ctx} {
  }

  auto make_behavior() -> internal_subpipeline_actor::behavior_type {
    self_->attach_functor([this] {
      for (auto& op : operators_) {
        if (op) {
          self_->send_exit(op, caf::exit_reason::kill);
        }
      }
    });
    begin_spawn();
    return {
      /// @see operator_actor
      [this](connect_t connect) -> caf::result<void> {
        // TODO: Only return once we also connected all our operators.
        TENZIR_WARN("connecting subpipeline with outer");
        connect_rp_ = self_->make_response_promise<void>();
        connect_ = std::move(connect);
        connect_operators_if_ready();
        return connect_rp_;
      },
      [this](atom::start) -> caf::result<void> {
        // We know that this only happens after connection (unless the caller
        // made a mistake...)
        TENZIR_ASSERT(started_ == 0);
        start_rp = self_->make_response_promise<void>();
        for (auto& op : operators_) {
          self_->mail(atom::start_v)
            .request(op, caf::infinite)
            .then(
              [this] {
                started_ += 1;
                TENZIR_ASSERT(started_ <= operators_.size());
                if (started_ == operators_.size()) {
                  start_rp.deliver();
                }
              },
              [](caf::error) {
                TENZIR_ERROR("start failed");
                TENZIR_TODO();
              });
        }
        return start_rp;
      },
      [this](atom::commit) -> caf::result<void> {
        TENZIR_INFO("subpipeline received commit notification");
        auto remaining = std::make_shared<size_t>(operators_.size());
        for (auto& op : operators_) {
          self_->mail(atom::commit_v)
            .request(op, caf::infinite)
            .then([this, remaining] {
              TENZIR_ASSERT(*remaining > 0);
              *remaining -= 1;
              if (*remaining == 0) {
                TENZIR_INFO("commit for subpipeline completed");
                TENZIR_ASSERT(checkpoints_in_flight_ > 0);
                checkpoints_in_flight_ -= 1;
                check_for_shutdown();
              }
            });
        }
        return {};
      },
      /// @see upstream_actor
      [this](atom::pull, uint64_t items) -> caf::result<void> {
        return self_->mail(atom::pull_v, items).delegate(connect_.upstream);
      },
      [this](atom::stop) -> caf::result<void> {
        // TODO: Anything else?
        return self_->mail(atom::stop_v).delegate(connect_.upstream);
      },
      /// @see downstream_actor
      [this](atom::push, table_slice slice) -> caf::result<void> {
        return self_->mail(atom::push_v, std::move(slice))
          .delegate(connect_.downstream);
      },
      [this](atom::push, chunk_ptr chunk) -> caf::result<void> {
        return self_->mail(atom::push_v, std::move(chunk))
          .delegate(connect_.downstream);
      },
      [this](atom::persist, checkpoint check) -> caf::result<void> {
        // TODO: What do we do here?
        // TODO: Inspecting sender is probably bad.
        if (self_->current_sender() == operators_.back()) {
          TENZIR_INFO("got back checkpoint from last operator");
          self_->mail(atom::persist_v, check)
            .request(connect_.downstream, caf::infinite)
            .then([] {});
        } else {
          // TODO: Don't do this if we already try to shut down!
          TENZIR_INFO("checkpointing subpipeline");
          checkpoints_in_flight_ += 1;
          self_->mail(atom::persist_v, check)
            .request(operators_.front(), caf::infinite)
            .then([]() {},
                  [this](caf::error err) {
                    self_->quit(std::move(err));
                  });
        }
        return {};
      },
      [this](atom::done) -> caf::result<void> {
        return self_->mail(atom::done_v).delegate(connect_.downstream);
      },
      /// @see shutdown_actor
      [this](atom::shutdown) -> caf::result<void> {
        // TODO: Could this come before we are fully spawned?
        TENZIR_ASSERT(shutdown_count_ < operators_.size());
        shutdown_count_ += 1;
        TENZIR_WARN("got ready to shutdown from {} operators", shutdown_count_);
        check_for_shutdown();
        return {};
      },
    };
  }

private:
  void check_connected() {
    if (connected_ == operators_.size()) {
      if (connect_rp_.pending()) {
        TENZIR_WARN("all operators connected, delivered connection rp");
        connect_rp_.deliver();
      } else {
        TENZIR_WARN("all operators connected, waiting for connection rp");
      }
    }
  }

  auto all_operators_are_ready_to_shutdown() const -> bool {
    return shutdown_count_ == operators_.size();
  }

  /// To begin shutdown, all operators must declare that they are ready for it.
  /// This is because once we start the shutdown, we lose the ability to emit
  /// checkpoints, so all longer-running computations should already be
  /// completed at that point. Furthermore, we ensure that all post-commit steps
  /// were executed, because we can still fail during shutdown and don't want to
  /// have partial checkpoints for that.
  void check_for_shutdown() {
    if (not all_operators_are_ready_to_shutdown()) {
      return;
    }
    if (checkpoints_in_flight_ > 0) {
      TENZIR_WARN("postponing shutdown because of {} in-flight checkpoints",
                  checkpoints_in_flight_);
      return;
    }
    TENZIR_ERROR("beginning subpipeline shutdown");
    asked_for_exit = true;
    for (auto& op : operators_) {
      self_->send_exit(op, caf::exit_reason::user_shutdown);
    }
  }

  // TODO: This should be async if we spawn remote.
  void spawn_operator(uint64_t index, std::optional<chunk_ptr> restore) {
    TENZIR_ASSERT(operators_.size() == plan_.size());
    TENZIR_ASSERT(index < operators_.size());
    TENZIR_ASSERT(not operators_[index]);
    TENZIR_WARN("spawning operator {} with {}", index, restore);
    operators_[index] = plan_[index]->spawn(
      plan::operator_spawn_args{self_->system(), ctx_, std::move(restore)});
    self_->monitor(operators_[index], [this](caf::error err) {
      if (not err) {
        err = caf::make_error(ec::logic_error, "no reason");
      }
      if (not asked_for_exit or err != caf::exit_reason::user_shutdown) {
        TENZIR_WARN("operator exited unexpectedly: {}", err);
        self_->quit(err);
        return;
      }
      exit_count_ += 1;
      TENZIR_ASSERT(exit_count_ <= operators_.size());
      if (exit_count_ == operators_.size()) {
        TENZIR_WARN("all operators exited");
        // TODO: Why can't we directly quit here?
        self_->mail(atom::shutdown_v)
          .request(connect_.shutdown, caf::infinite)
          .then(
            [] {

            },
            [](caf::error) {
              TENZIR_TODO();
            });
      }
    });
    TENZIR_WARN("spawned operator {}", index);
    connect_operators_if_ready();
  }

  void connect_operators_if_ready() {
    if (std::ranges::contains(operators_, operator_actor{})) {
      // Not all operators are spawned.
      return;
    }
    if (not connect_.checkpoint_receiver) {
      // We didn't get the checkpoint receiver yet, which we need for connecting.
      return;
    }
    TENZIR_ERROR("connecting subpipeline operators");
    for (auto& op : operators_) {
      TENZIR_ASSERT(op);
      // TODO: Do we really need to use ourselves here?
      auto upstream
        = upstream_actor{&op == &operators_.front() ? self_ : *(&op - 1)};
      auto downstream
        = downstream_actor{&op == &operators_.back() ? self_ : *(&op + 1)};
      self_
        ->mail(connect_t{
          std::move(upstream),
          std::move(downstream),
          connect_.checkpoint_receiver,
          shutdown_actor{self_},
        })
        .request(op, caf::infinite)
        .then(
          [this] mutable {
            connected_ += 1;
            TENZIR_ASSERT(connected_ <= operators_.size());
            check_connected();
          },
          [this](caf::error err) {
            // TODO
            TENZIR_ERROR("{}", err);
            self_->quit(err);
          });
    }
  }

  void begin_spawn() {
    TENZIR_WARN("spawning operators for subpipeline");
    TENZIR_ASSERT(plan_.size() > 0);
    operators_.resize(plan_.size());
    for (auto [index, op] : detail::enumerate<uint64_t>(plan_)) {
      if (checkpoint_reader_) {
        // Restore.
        self_->mail(atom::get_v, plan_.id(), index)
          .request(*checkpoint_reader_, caf::infinite)
          .then(
            [this, index](chunk_ptr restore) {
              spawn_operator(index, std::move(restore));
            },
            [](caf::error) {
              TENZIR_ERROR("oops");
              TENZIR_TODO();
            });
      } else {
        // Fresh spawn.
        spawn_operator(index, std::nullopt);
      }
    }
  }

  internal_subpipeline_actor::pointer self_;
  connect_t connect_;
  caf::typed_response_promise<void> connect_rp_;

  plan::pipeline plan_;
  std::optional<checkpoint_reader_actor> checkpoint_reader_;
  base_ctx ctx_;
  std::vector<operator_actor> operators_;
  size_t connected_ = 0;
  size_t started_ = 0;
  caf::typed_response_promise<void> start_rp;
  size_t shutdown_count_ = 0;
  bool asked_for_exit = false;

  size_t exit_count_ = 0;

  size_t checkpoints_in_flight_ = 0;
};

class pipeline {
public:
  [[maybe_unused]] static constexpr auto name = "pipeline";

  pipeline(internal_pipeline_actor::pointer self, subpipeline_actor sub,
           pipeline_settings settings)
    : self_{self}, sub_{std::move(sub)}, settings_{std::move(settings)} {
    self_->monitor(sub_, [this](caf::error err) {
      if (not err) {
        err = caf::make_error(ec::logic_error, "no reason given");
      }
      if (not asked_for_exit or err != caf::exit_reason::user_shutdown) {
        TENZIR_ERROR("pipeline error: {}", err);
        self_->quit(std::move(err));
        return;
      }
      TENZIR_ERROR("pipeline exited successfully");
      self_->quit();
    });
    self_->attach_functor([this] {
      self_->send_exit(sub_, caf::exit_reason::kill);
    });
  }

  auto make_behavior() -> internal_pipeline_actor::behavior_type {
    TENZIR_WARN("connecting outermost subpipeline");
    // TODO
    auto checkpoint_rec
      = self_->spawn(caf::actor_from_state<checkpoint_receiver>);
    self_
      ->mail(connect_t{
        upstream_actor{self_},
        downstream_actor{self_},
        std::move(checkpoint_rec),
        shutdown_actor{self_},
      })
      .request(sub_, caf::infinite)
      .then(
        [this] {
          TENZIR_WARN("outermost subpipeline connected");
          connected = true;
          check_start();
        },
        [](caf::error error) {
          TENZIR_TODO();
        });
    return {
      /// ---------- @see pipeline_actor ----------
      [this](atom::start) -> caf::result<void> {
        TENZIR_WARN("pipeline received start");
        start_rp = self_->make_response_promise<void>();
        check_start();
        return start_rp;
      },
      /// ---------- @see shutdown_actor ----------
      [this](atom::shutdown) -> caf::result<void> {
        // TODO: Does it really need to ask?
        TENZIR_WARN("subpipeline is ready to shutdown");
        asked_for_exit = true;
        self_->send_exit(sub_, caf::exit_reason::user_shutdown);
        return {};
      },
      /// ---------- @see upstream_actor ----------
      [this](atom::pull, uint64_t items) -> caf::result<void> {
        TENZIR_WARN("?");
        TENZIR_TODO();
      },
      [this](atom::stop) -> caf::result<void> {
        // TODO: Just ignore this?
        return {};
      },
      /// ---------- @see downstream_actor ----------
      [this](atom::push, table_slice slice) -> caf::result<void> {
        TENZIR_WARN("?");
        TENZIR_TODO();
      },
      [this](atom::push, chunk_ptr chunk) -> caf::result<void> {
        TENZIR_WARN("?");
        TENZIR_TODO();
      },
      [this](atom::persist, checkpoint check) -> caf::result<void> {
        TENZIR_INFO("checkpoint completed! (pretending commit)");
        self_->mail(atom::commit_v).request(sub_, caf::infinite).then([] {});
        return {};
      },
      [this](atom::done) -> caf::result<void> {
        // TODO: Just ignore this?
        return {};
      },
    };
  }

private:
  void check_start() {
    // Wait for connection and start signal.
    if (not connected or not start_rp.pending()) {
      return;
    }
    TENZIR_ERROR("fully starting pipeline now");
    self_->mail(atom::start_v)
      .request(sub_, caf::infinite)
      .then(
        [this] {
          TENZIR_ERROR("successfully started pipeline");
          start_rp.deliver();
          detail::weak_run_delayed_loop(self_, std::chrono::seconds{3}, [this] {
            TENZIR_INFO("emitting checkpoint");
            self_->mail(atom::persist_v, checkpoint{})
              .request(sub_, caf::infinite)
              .then(
                [] {

                },
                [](caf::error) {
                  TENZIR_TODO();
                });
          });
        },
        [](caf::error) {
          TENZIR_TODO();
        });
  }

  internal_pipeline_actor::pointer self_;
  subpipeline_actor sub_;
  pipeline_settings settings_;
  bool connected = false;
  bool asked_for_exit = false;
  caf::typed_response_promise<void> start_rp;
};

} // namespace

auto make_pipeline(plan::pipeline pipe, pipeline_settings settings,
                   std::optional<checkpoint_reader_actor> checkpoint_reader,
                   base_ctx ctx) -> pipeline_actor {
  TENZIR_ERROR("spawning subpipeline");
  auto sub
    = ctx.system().spawn(caf::actor_from_state<subpipeline>, std::move(pipe),
                         std::move(checkpoint_reader), ctx);
  TENZIR_ERROR("spawning outer pipeline");
  return ctx.system().spawn(caf::actor_from_state<pipeline>, std::move(sub),
                            std::move(settings));
}

} // namespace tenzir::exec
