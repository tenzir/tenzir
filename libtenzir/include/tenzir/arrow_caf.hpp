//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/util/future.h>
#include <caf/scheduled_actor.hpp>

namespace tenzir {

/// Add a callback to an `arrow::Future` that shall run inside an actor context.
template <class T, class F>
void add_actor_callback(caf::scheduled_actor* self, arrow::Future<T> future,
                        F&& f) {
  // TODO: Shouldn't this capture a weak/strong ref to `self`?
  using result_type
    = std::conditional_t<std::same_as<T, arrow::internal::Empty>, arrow::Status,
                         arrow::Result<T>>;
  std::move(future).AddCallback(
    [self, f = std::forward<F>(f)](const result_type& result) mutable {
      self->schedule_fn([f = std::move(f), result]() mutable -> void {
        return std::invoke(std::move(f), std::move(result));
      });
    });
}

/// Iterate asynchronously over an `arrow::fs::FileInfoGenerator`.
template <class F>
void iterate_files(caf::scheduled_actor* self, arrow::fs::FileInfoGenerator gen,
                   F&& f) {
  add_actor_callback(self, gen(),
                     [self, gen = std::move(gen), f = std::forward<F>(f)](
                       arrow::Result<arrow::fs::FileInfoVector> infos) {
                       auto more = infos.ok() and not infos->empty();
                       f(std::move(infos));
                       if (more) {
                         iterate_files(self, std::move(gen), std::move(f));
                       }
                     });
}

} // namespace tenzir
