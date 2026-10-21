//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/secret.hpp>

#include <arrow/util/secure_string.h>

namespace tenzir::plugins::parquet {

/// Prepares a raw AES key without retaining ordinary string copies.
auto prepare_encryption_key(located<secret> const& encryption_key, OpCtx& ctx)
  -> Task<failure_or<arrow::util::SecureString>>;

} // namespace tenzir::plugins::parquet
