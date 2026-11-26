//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/nova/events.hpp>
#include <tenzir/result.hpp>

#include <arrow/type_fwd.h>

#include <memory>
#include <string>
#include <vector>

namespace tenzir::plugins::iceberg {

enum class DecimalFormat {
  string,
  float_,
};

struct ArrowReadOptions {
  DecimalFormat decimal_format = DecimalFormat::string;
  std::string schema_name;
};

struct ArrowReadBatch {
  nova::Events events;
  /// Non-fatal conversion warnings (e.g. out-of-range timestamps/dates set to
  /// null).
  std::vector<std::string> warnings;
};

/// Imports a RecordBatch from iceberg-cpp's reader into events.
/// Normalizes Iceberg-specific Arrow types that `nova::import_arrow_array`
/// does not handle directly while adopting untouched Arrow buffers zero-copy.
auto import_iceberg_batch(std::shared_ptr<arrow::RecordBatch> batch,
                          ArrowReadOptions const& options)
  -> tenzir::Result<ArrowReadBatch, std::string>;

} // namespace tenzir::plugins::iceberg
