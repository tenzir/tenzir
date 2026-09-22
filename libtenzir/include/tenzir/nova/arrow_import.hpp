//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array.hpp"
#include "tenzir/result.hpp"

#include <arrow/type_fwd.h>

#include <memory>
#include <string>

namespace tenzir::nova {

/// Imports an Arrow column directly into owned columnar storage. Preserves
/// nested nulls and honors sliced-array offsets. Returns an error message on
/// failure; emitting diagnostics and format-specific conversions, such as
/// decimal formatting, belong to readers.
auto import_arrow_array(arrow::Array const& input)
  -> Result<Array<Data>, std::string>;

/// Consumes an Arrow array, adopting uniquely owned mutable buffers when their
/// layouts match Nova storage. Shared, immutable, or unaligned buffers are
/// copied. The caller must not use borrowed pointers or views into adopted
/// buffers after transferring ownership.
auto import_arrow_array(std::shared_ptr<arrow::Array> input)
  -> Result<Array<Data>, std::string>;

} // namespace tenzir::nova
