//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/option.hpp"

#include <cstddef>
#include <vector>

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

/// A type-erased, asynchronous sender on multiple ports.
///
/// An operator with a single output port (the common case) uses the plain
/// `Push<Output>&` interface. Operators with multiple output ports (`if`,
/// `match`, `fork`, `fork_merge`) opt in to the multi-output `process` overload
/// and address ports via `outs[k]` or `outs(k, value)`. Each logical port is
/// backed by its own routing push provided by the runner (scatter/shuffle over
/// the port's downstream lanes); `PushPorts` itself only routes to the right
/// port. Both the referenced pushes and the pointer vector are owned by the
/// caller (the runner) and outlive the `PushPorts` handed to a `process` call.
template <class Output>
class PushPorts {
public:
  explicit PushPorts(std::vector<Push<Output>*>& ports) : ports_{ports} {
  }

  /// The number of logical output ports.
  auto size() const -> size_t {
    return ports_.size();
  }

  /// Access the push for logical port `port`.
  auto operator[](size_t port) -> Push<Output>& {
    return *ports_[port];
  }

  /// Push to the default logical port (port 0).
  auto operator()(Output value) -> Task<void> {
    return (*ports_[0])(std::move(value));
  }

  /// Push to logical port `port`.
  auto operator()(size_t port, Output value) -> Task<void> {
    return (*ports_[port])(std::move(value));
  }

private:
  std::vector<Push<Output>*>& ports_;
};

} // namespace tenzir
