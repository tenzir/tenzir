//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/table_slice.hpp"

#include <cstddef>
#include <vector>

namespace tenzir {

class diagnostic_handler;

/// A subset of an input slice's rows routed to a single downstream lane of a
/// `Split` channel.
struct SplitRun {
  /// The index of the downstream lane that receives `slice`.
  size_t lane;
  /// The rows routed to `lane`. May be any subset of the input's rows, not
  /// necessarily contiguous.
  table_slice slice;
};

/// Classifies the rows of a slice into the lanes of a `Split` channel.
///
/// A splitter is the row-routing policy behind a `Split` channel: given an
/// input slice, it returns lane-tagged subslices that together cover all rows
/// exactly once. `if` uses a boolean condition (two lanes); `match` uses
/// pattern and guard evaluation (one lane per arm).
class Splitter {
public:
  virtual ~Splitter() = default;

  /// A virtual copy constructor so a `Box<Splitter>` is copyable.
  virtual auto copy() const -> Box<Splitter> = 0;

  /// The number of downstream lanes this splitter routes into.
  virtual auto lanes() const -> size_t = 0;

  /// Partition `slice` into lane-tagged runs covering all rows. Diagnostics
  /// (such as non-boolean conditions) are emitted through `dh`.
  virtual auto split(table_slice slice, diagnostic_handler& dh) const
    -> std::vector<SplitRun>
    = 0;
};

} // namespace tenzir
