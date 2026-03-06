//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/option.hpp"

namespace tenzir {

/// A type-erased, asynchronous sender.
template <class T>
class Push {
public:
  /// Destruction must eventually lead the associated `Pull` to return `None`.
  virtual ~Push() = default;

  /// Unless documented otherwise, this is not safe to call concurrently.
  virtual auto operator()(T output) -> Task<void> = 0;
};

/// A type-erased, asynchronous receiver.
template <class T>
class Pull {
public:
  /// Destruction must NOT wake up the associated `Push` (similar to Golang).
  virtual ~Pull() = default;

  /// Unless documented otherwise, this is not safe to call concurrently.
  virtual auto operator()() -> Task<Option<T>> = 0;
};

/// A pair of a type-erased, asynchronous sender and receiver.
///
/// Unless documented otherwise, this represents an SPSC channel that does not
/// allow concurrent usage of the sender, and same for the receiver.
template <class T>
struct PushPull {
  Box<Push<T>> push;
  Box<Pull<T>> pull;
};

} // namespace tenzir
