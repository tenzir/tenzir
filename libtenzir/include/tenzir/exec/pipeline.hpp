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
#include "tenzir/operator_actor.hpp"

#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_stream.hpp>

namespace tenzir::exec {

struct pipeline_actor_traits {
  using signatures = caf::type_list<
    //
    auto(atom::start)->caf::result<void>,
    //
    auto(atom::start, handshake hs)->caf::result<handshake_response>
    >;
};

using pipeline_actor = caf::typed_actor<pipeline_actor_traits>;

class pipeline {
public:
  pipeline(pipeline_actor::pointer self, bp::pipeline pipe, base_ctx ctx)
    : self_{self}, pipe_{std::move(pipe)}, ctx_{ctx} {
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
  void
  recurse(handshake hs, std::vector<operator_actor> next,
          std::function<void(caf::expected<handshake_response>)> callback) {
    if (next.empty()) {
      return callback(handshake_response{std::move(hs.input)});
    }
    auto head = std::move(next.front());
    next.erase(next.begin());
    self_->mail(std::move(hs))
      .request(head, caf::infinite)
      .then(
        // TODO: uncopy.
        [this, next = std::move(next),
         callback](handshake_response hr) mutable {
          if (next.empty()) {
            callback(std::move(hr));
            return;
          }
          recurse(handshake{hr.output}, std::move(next), std::move(callback));
        },
        [callback](caf::error err) mutable {
          // TODO: Additional info? Diagnostics?
          callback(std::move(err));
        });
  }

  void spawn(std::function<void(std::vector<operator_actor>)> callback) {
    auto ops = std::vector<operator_actor>{};
    for (auto& op : pipe_) {
      ops.push_back(op->spawn(bp::operator_base::spawn_args{self_->system(), ctx_}));
    }
    callback(std::move(ops));
  }

  auto start() -> caf::result<void> {
    auto rp = self_->make_response_promise<void>();
    spawn([this, rp](std::vector<operator_actor> ops) mutable {
      auto initial
        = self_->make_observable().never<message<void>>().to_typed_stream(
          "initial", duration::zero(), 1);
      recurse(handshake{std::move(initial)}, std::move(ops),
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
                self_->observe(*output, 30, 10).for_each([](message<void>) {
                  // TODO: Checkpoints??
                });
                rp.deliver();
              });
    });
    return rp;
  }

  auto start(handshake hs) -> caf::result<handshake_response> {
    auto rp = self_->make_response_promise<handshake_response>();
    spawn(
      [this, rp, hs = std::move(hs)](std::vector<operator_actor> ops) mutable {
        recurse(handshake{std::move(hs)}, std::move(ops),
                [rp](caf::expected<handshake_response> hr) mutable {
                  rp.deliver(std::move(hr));
                });
      });
    return rp;
  }

  pipeline_actor::pointer self_;
  bp::pipeline pipe_;
  base_ctx ctx_;
};

} // namespace tenzir::exec
