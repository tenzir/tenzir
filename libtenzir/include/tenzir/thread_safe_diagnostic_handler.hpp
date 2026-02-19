//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"

#include <mutex>

namespace tenzir {

/// A thread-safe wrapper around a diagnostic_handler for use in parallel
/// worker coroutines.
class ThreadSafeDiagnosticHandler final : public diagnostic_handler {
public:
  explicit ThreadSafeDiagnosticHandler(diagnostic_handler& inner)
    : inner_{inner} {
  }
  void emit(diagnostic d) override {
    auto lock = std::scoped_lock{mutex_};
    inner_.emit(std::move(d));
  }

private:
  diagnostic_handler& inner_;
  std::mutex mutex_;
};

} // namespace tenzir
