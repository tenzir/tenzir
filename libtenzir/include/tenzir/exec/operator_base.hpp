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

class operator_base {
public:
  operator_base(operator_actor::pointer self) : self_{self} {
  }

  virtual ~operator_base() = default;

  virtual auto on_start() -> caf::result<void> {
    return {};
  }

  virtual void on_commit() {
  }

  // downstream_actor_traits handlers
  virtual void on_push(table_slice slice) = 0;
  virtual void on_push(chunk_ptr chunk) = 0;
  virtual auto persist() -> chunk_ptr = 0;
  virtual void on_done() = 0;

  // upstream_actor_traits handlers
  virtual void on_pull(uint64_t items) = 0;
  virtual void on_stop() {
    finish();
  }

  auto make_behavior() -> operator_actor::behavior_type {
    return {
      [this](connect_t connect) -> caf::result<void> {
        connect_ = std::move(connect);
        return {};
      },
      [this](atom::start) -> caf::result<void> {
        return this->on_start();
      },
      [this](atom::commit) -> caf::result<void> {
        this->on_commit();
        return {};
      },
      [this](atom::push, table_slice slice) -> caf::result<void> {
        this->on_push(std::move(slice));
        return {};
      },
      [this](atom::push, chunk_ptr chunk) -> caf::result<void> {
        this->on_push(std::move(chunk));
        return {};
      },
      [this](atom::persist, checkpoint checkpoint) -> caf::result<void> {
        auto forward_checkpoint = [this, checkpoint] {
          self_->mail(atom::persist_v, checkpoint)
            .request(connect_.downstream, caf::infinite)
            .then([] {});
        };
        auto result = persist();
        if (result and result->size() > 0) {
          self_->mail(checkpoint, std::move(result))
            .request(connect_.checkpoint_receiver, caf::infinite)
            .then(forward_checkpoint);
        } else {
          forward_checkpoint();
        }
        return {};
      },
      [this](atom::done) -> caf::result<void> {
        upstream_finished_ = true;
        this->on_done();
        return {};
      },
      [this](atom::pull, uint64_t items) -> caf::result<void> {
        this->on_pull(items);
        return {};
      },
      [this](atom::stop) -> caf::result<void> {
        downstream_finished_ = true;
        this->on_stop();
        return {};
      },
    };
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
    self_->mail(atom::push_v, std::move(slice))
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
  operator_actor::pointer self_;

private:
  connect_t connect_;
  bool downstream_finished_ = false;
  bool upstream_finished_ = false;
  bool sent_shutdown_ = false;
};

} // namespace tenzir::exec
