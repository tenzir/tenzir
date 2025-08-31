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
#include "tenzir/exec/checkpoint.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/event_based_mail.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_stream.hpp>

namespace tenzir::exec {

template <class Actor>
class basic_operator {
public:
  explicit basic_operator(Actor::pointer self) : self_{self} {
    self_->set_error_handler([this](caf::error& err) {
      TENZIR_ERROR("operator quits because of error: {}", err);
      self_->quit(err);
    });
  }

  virtual ~basic_operator() = default;

  virtual auto on_start() -> caf::result<void> {
    return {};
  }

  virtual void on_commit() {
  }

  virtual auto on_connect() -> caf::result<void> {
    return {};
  }

  // downstream_actor
  virtual void on_push(table_slice slice) = 0;
  virtual void on_push(chunk_ptr chunk) = 0;
  virtual auto serialize() -> chunk_ptr = 0;
  virtual void on_persist(checkpoint checkpoint) {
    auto forward_checkpoint = [this, checkpoint] {
      persist(checkpoint);
    };
    auto result = serialize();
    if (result and result->size() > 0) {
      self_->mail(checkpoint, std::move(result))
        .request(connect_.checkpoint_receiver, caf::infinite)
        .then(forward_checkpoint);
    } else {
      forward_checkpoint();
    }
  }
  virtual void on_done() = 0;

  // upstream_actor
  virtual void on_pull(uint64_t items) = 0;
  virtual void on_stop() {
    finish();
  }

  template <class... Fs>
  auto extend_behavior(std::tuple<Fs...> fs) -> Actor::behavior_type {
    return std::apply(
      [this](auto... fs) -> Actor::behavior_type {
        return {
          std::move(fs)...,
          [this](connect_t connect) -> caf::result<void> {
            connect_ = std::move(connect);
            return on_connect();
          },
          [this](atom::start) -> caf::result<void> {
            return on_start();
          },
          [this](atom::commit) -> caf::result<void> {
            on_commit();
            return {};
          },
          [this](atom::push, payload payload) -> caf::result<void> {
            match(
              std::move(payload),
              [&](table_slice slice) {
                TENZIR_ASSERT(slice.rows() > 0);
                on_push(std::move(slice));
              },
              [&](chunk_ptr chunk) {
                TENZIR_ASSERT(chunk);
                TENZIR_ASSERT(chunk->size() > 0);
                on_push(std::move(chunk));
              });
            return {};
          },
          [this](atom::persist, checkpoint checkpoint) -> caf::result<void> {
            on_persist(checkpoint);
            return {};
          },
          [this](atom::done) -> caf::result<void> {
            upstream_finished_ = true;
            on_done();
            return {};
          },
          [this](atom::pull, uint64_t items) -> caf::result<void> {
            on_pull(items);
            return {};
          },
          [this](atom::stop) -> caf::result<void> {
            downstream_finished_ = true;
            on_stop();
            return {};
          },
        };
      },
      std::move(fs));
  }

  auto make_behavior() -> Actor::behavior_type
    requires std::same_as<Actor, exec::operator_actor>
  {
    return extend_behavior({});
  }

  void no_more_input() {
    if (not upstream_finished_) {
      self_->mail(atom::stop_v)
        .request(connect_.upstream, caf::infinite)
        .then([] {},
              [](caf::error) {
                TENZIR_WARN("??");
                TENZIR_TODO();
              });
      upstream_finished_ = true;
    }
  }

  void no_more_output() {
    if (not downstream_finished_) {
      self_->mail(atom::done_v)
        .request(connect_.downstream, caf::infinite)
        .then([] {},
              [](caf::error) {
                TENZIR_WARN("??");
                TENZIR_TODO();
              });
      downstream_finished_ = true;
    }
  }

  auto has_finished() const -> bool {
    return sent_shutdown_;
  }

  void finish() {
    no_more_input();
    no_more_output();
    if (not sent_shutdown_) {
      self_->mail(atom::shutdown_v)
        .request(connect_.shutdown, caf::infinite)
        .then([] {},
              [](caf::error) {
                TENZIR_WARN("??");
                TENZIR_TODO();
              });
      sent_shutdown_ = true;
    }
  }

  void push(table_slice slice) {
    self_->mail(atom::push_v, payload{std::move(slice)})
      .request(connect_.downstream, caf::infinite)
      .then([] {},
            [](caf::error) {
              TENZIR_WARN("??");
              TENZIR_TODO();
            });
  }

  void push(chunk_ptr chunk) {
    self_->mail(atom::push_v, payload{std::move(chunk)})
      .request(connect_.downstream, caf::infinite)
      .then([] {},
            [](caf::error) {
              TENZIR_WARN("??");
              TENZIR_TODO();
            });
  }

  void persist(checkpoint checkpoint) {
    self_->mail(atom::persist_v, checkpoint)
      .request(connect_.downstream, caf::infinite)
      .then([] {},
            [](caf::error) {
              TENZIR_WARN("??");
              TENZIR_TODO();
            });
  }

  void pull(size_t items) {
    self_->mail(atom::pull_v, items)
      .request(connect_.upstream, caf::infinite)
      .then([] {},
            [](caf::error) {
              TENZIR_WARN("??");
              TENZIR_TODO();
            });
  }

protected:
  Actor::pointer self_;

  auto upstream() const -> const upstream_actor& {
    return connect_.upstream;
  }

  auto downstream() const -> const downstream_actor& {
    return connect_.downstream;
  }

  auto checkpoint_receiver() const -> const checkpoint_receiver_actor& {
    return connect_.checkpoint_receiver;
  }

private:
  connect_t connect_;
  bool downstream_finished_ = false;
  bool upstream_finished_ = false;
  bool sent_shutdown_ = false;
};

using operator_base = basic_operator<operator_actor>;

} // namespace tenzir::exec
