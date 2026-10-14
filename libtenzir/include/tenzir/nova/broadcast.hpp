//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/union_array.hpp"

#include <span>

namespace tenzir::nova {

// Broadcasting spreads each row of a column that `rows` selects over its span
// in `spans`, so that every element of a list sees the value of the row it
// belongs to. The column, `spans` and `rows` share a row layout, and the
// result has `length` elements. The spans may come in any order, but those of
// the selected rows must lie within `length`, and to broadcast values rather
// than a mask, they must not overlap.
//
// Every selected row is written as one run. Variable-length payloads are not
// copied: strings, blobs and secrets share their byte buffer and lists share
// their values, only the spans into them are new.

/// The elements that belong to the rows `rows` selects. Their spans may
/// overlap.
auto broadcast_mask(std::span<storage::Span const> spans,
                    storage::BitMap const& rows, storage::Index length)
  -> storage::BitMap;

/// Broadcasts the selected rows of `column`. The other elements hold
/// placeholders, which are only meaningful together with `broadcast_mask`:
/// unset bits, the empty record shape, no union alternative, an empty span,
/// or a default value.
template <data_type Tag>
auto broadcast(Array<Tag> const& column, std::span<storage::Span const> spans,
               storage::BitMap const& rows, storage::Index length)
  -> Array<Tag>;

/// Broadcasts the rows of `column` that are both selected and present.
/// Exactly their elements are present in the result; the others hold
/// placeholders. Rows that are not present are never read.
auto broadcast(MaskedArray<Array<Data>> const& column,
               std::span<storage::Span const> spans,
               storage::BitMap const& rows, storage::Index length)
  -> MaskedArray<Array<Data>>;

/// The rows of a column within each of `ranges`, one range after another.
/// The ranges may come in any order and overlap, which repeats their rows.
/// Variable-length payloads are shared as when broadcasting.
auto concatenate_ranges(storage::BitMap const& bits,
                        std::span<storage::Span const> ranges)
  -> storage::BitMap;

auto concatenate_ranges(Array<Data> const& column,
                        std::span<storage::Span const> ranges) -> Array<Data>;

} // namespace tenzir::nova
