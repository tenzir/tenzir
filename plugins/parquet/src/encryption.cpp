//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/encryption.hpp"

#include <tenzir/secret_resolution.hpp>

#include <cstring>

namespace tenzir::plugins::parquet {

auto prepare_encryption_key(located<secret> const& encryption_key, OpCtx& ctx)
  -> Task<failure_or<arrow::util::SecureString>> {
  auto resolved = resolved_secret_value{};
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(encryption_key, resolved);
  CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
  auto bytes = resolved.blob();
  if (bytes.size() != 16 and bytes.size() != 24 and bytes.size() != 32) {
    diagnostic::error("encryption key must contain 16, 24, or 32 bytes")
      .primary(encryption_key.source)
      .emit(ctx);
    co_return failure::promise();
  }
  auto key = arrow::util::SecureString{bytes.size(), '\0'};
  std::memcpy(key.as_span().data(), bytes.data(), bytes.size());
  co_return key;
}

} // namespace tenzir::plugins::parquet
