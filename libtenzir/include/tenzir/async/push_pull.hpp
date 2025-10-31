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

namespace tenzir {

/// A type-erased, asynchronous sender.
template <class T>
class Push {
public:
  virtual ~Push() = default;

  virtual auto operator()(T output) -> Task<void> = 0;
};

/// A type-erased, asynchronous receiver.
template <class T>
class Pull {
public:
  virtual ~Pull() = default;

  virtual auto operator()() -> Task<T> = 0;
};

/// A pair of a type-erased, asynchronous sender and receiver.
template <class T>
struct PushPull {
  Box<Push<T>> push;
  Box<Pull<T>> pull;
};

} // namespace tenzir
