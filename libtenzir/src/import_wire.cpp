//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#include "tenzir/import_wire.hpp"

#include "tenzir/nova/bitz.hpp"
#include "tenzir/try.hpp"

namespace tenzir {

auto to_import_wire(nova::Events const& events)
  -> Result<ImportWireBatch, std::string> {
  if (events.active_count() == 0) {
    return ImportWireBatch{};
  }
  TRY(auto payload, nova::bitz::encode(nova::bitz::Batch{
                      events.data, events.mask, events.meta}));
  return ImportWireBatch{std::move(payload)};
}

auto from_import_wire(ImportWireBatch const& wire)
  -> Result<nova::Events, std::string> {
  if (wire.payload.empty()) {
    return nova::Events{};
  }
  TRY(auto batch, nova::bitz::decode(wire.payload));
  auto records = std::move(batch.data).try_as<nova::Record>();
  if (not records) {
    return Err{"import batch root must be a record array"};
  }
  return nova::Events{std::move(*records), std::move(batch.mask),
                      std::move(batch.meta)};
}

} // namespace tenzir
