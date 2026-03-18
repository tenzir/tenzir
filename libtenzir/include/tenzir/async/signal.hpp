//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/variant.hpp"

#include <tenzir/uuid.hpp>

namespace tenzir {

/// No more data will come after this signal. Will never be sent over `void`.
struct EndOfData {
  friend auto inspect(auto& f, EndOfData& x) -> bool {
    return f.object(x).fields();
  }
};

/// Request to perform a checkpoint. To be forwarded downstream afterwards.
struct Checkpoint {
  uuid id;

  friend auto inspect(auto& f, Checkpoint& x) -> bool {
    return f.apply(x);
  }
};

/// A non-data message sent to an operator by its upstream.
using Signal = variant<EndOfData, Checkpoint>;

template <class T>
struct OperatorMsg : variant<T, Signal> {
  using variant<T, Signal>::variant;
};

template <>
struct OperatorMsg<void> : variant<Signal> {
  using variant<Signal>::variant;
};

template <class T>
inline constexpr auto enable_default_formatter<OperatorMsg<T>> = true;

} // namespace tenzir
