//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/scheduled_actor.hpp>
#include <arrow/util/thread_pool.h>
#include <arrow/filesystem/filesystem.h>

#include "tenzir/arrow_utils.hpp"

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
      static_cast<void>(resource);
    });
  }

private:
  auto
  SpawnReal(arrow::internal::TaskHints, arrow::internal::FnOnce<void()> task,
            arrow::StopToken, StopCallback&&) -> arrow::Status override {
    if (weak_.lock()) {
      // We need to wrap it because `task` must be moved for the call.
      self_->schedule_fn([task = std::move(task)] mutable {
        std::move(task)();
      });
    }
    return arrow::Status::OK();
  }

  caf::scheduled_actor* self_;
  caf::weak_actor_ptr weak_;
};

template <class F>
void async_iter(arrow::fs::FileInfoGenerator gen, F&& f) {
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
}

} // namespace tenzir
