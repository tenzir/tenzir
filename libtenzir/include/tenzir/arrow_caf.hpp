//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_utils.hpp"

#include <arrow/filesystem/filesystem.h>
#include <arrow/util/thread_pool.h>
#include <caf/scheduled_actor.hpp>

namespace tenzir {

class caf_executor final : public arrow::internal::Executor {
public:
  explicit caf_executor(caf::scheduled_actor* self)
    : self_{self}, weak_{self_->ctrl()} {
  }

  ~caf_executor() override = default;

  auto GetCapacity() -> int override {
    return 1;
  };

  auto OwnsThisThread() -> bool override {
    return false;
  }

  void KeepAlive(std::shared_ptr<Resource> resource) override {
    self_->attach_functor([resource = std::move(resource)]() {
      (void)resource;
    });
  }

private:
  auto
  SpawnReal(arrow::internal::TaskHints, arrow::internal::FnOnce<void()> task,
            arrow::StopToken, StopCallback&&) -> arrow::Status override {
    if (auto strong = weak_.lock()) {
      // We need to wrap it because `task` must be moved for the call.
      self_->schedule_fn([task = std::move(task)] mutable {
        std::move(task)();
      });
      return arrow::Status::OK();
    }
    return arrow::Status::Cancelled("actor is no longer alive");
  }

  caf::scheduled_actor* self_;
  caf::weak_actor_ptr weak_;
};

#define USE_EXECUTOR 1

template <class T>
void add_actor_callback(caf::scheduled_actor* self, arrow::Future<T> fut,
                        auto&& fn) {
  using result_type
    = std::conditional_t<std::same_as<T, arrow::internal::Empty>, arrow::Status,
                         arrow::Result<T>>;
  std::move(fut).AddCallback([self, fn = std::forward<decltype(fn)>(fn)](
                               const result_type& result) mutable {
    TENZIR_WARN("in inner callback");
#if USE_EXECUTOR
    std::move(fn)(result);
#else
    self->schedule_fn([fn = std::move(fn), result]() mutable -> void {
      return std::move(fn)(std::move(result));
    });
#endif
  });
}

template <class F>
void async_iter(caf::scheduled_actor* self, arrow::fs::FileInfoGenerator gen,
                F&& f) {
#if 0
  (void)self;
  auto next = gen();
  next.AddCallback([gen = std::move(gen), f = std::forward<F>(f)](
                     arrow::Result<arrow::fs::FileInfoVector> infos_result) {
    // TODO: Don't check.
    auto infos = check(infos_result);
    auto done = infos.empty();
    f(std::move(infos));
    if (done) {
      return;
    }
    async_iter(std::move(gen), std::move(f));
  });
#else
  add_actor_callback(self, gen(),
                     [self, gen = std::move(gen), f = std::forward<F>(f)](
                       arrow::Result<arrow::fs::FileInfoVector> infos_result) {
                       // TODO: Don't check.
                       auto infos = check(infos_result);
                       auto done = infos.empty();
                       f(std::move(infos));
                       if (done) {
                         return;
                       }
                       async_iter(self, std::move(gen), std::move(f));
                     });
#endif
}

} // namespace tenzir
