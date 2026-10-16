// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/events.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/ref.hpp"

#include <span>
#include <vector>

namespace tenzir::nova {

/// Concatenates physical row layouts column by column. Only present values
/// are read. The result owns its payloads, including nested list children.
auto concatenate(std::span<MaskedArray<Array<Data>> const> arrays)
  -> Array<Data>;

/// Concatenates dense event batches, preserving metadata and field order.
auto concatenate(std::span<Events const> events) -> Events;

/// Collects selected rows in order, coalescing consecutive rows of a source.
/// Sources are borrowed until finish; the result owns its selected payloads.
class EventsAssembler {
public:
  auto append(Events const& input, storage::Index row) -> void;
  auto length() const -> storage::Index;
  auto finish() && -> Events;

private:
  struct Source {
    Ref<Events const> input;
    std::vector<storage::Span> ranges;
  };
  std::vector<Source> sources_;
  storage::Index length_ = 0;
};

} // namespace tenzir::nova
