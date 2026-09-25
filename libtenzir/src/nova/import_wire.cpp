//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#include "tenzir/nova/import_wire.hpp"

#include "tenzir/nova/bitz.hpp"
#include "tenzir/try.hpp"

namespace tenzir::nova {

auto to_import_wire(Events const& events)
  -> Result<ImportWireBatch, std::string> {
  if (events.active_count() == 0) {
    return ImportWireBatch{};
  }
  TRY(auto payload,
      bitz::encode(bitz::Batch{events.data, events.mask, events.meta}));
  return ImportWireBatch{std::move(payload)};
}

auto from_import_wire(ImportWireBatch const& wire)
  -> Result<Events, std::string> {
  if (wire.payload.empty()) {
    return Events{};
  }
  TRY(auto batch, bitz::decode(wire.payload));
  auto records = std::move(batch.data).try_as<Record>();
  if (not records) {
    return Err{"import batch root must be a record array"};
  }
  return Events{std::move(*records), std::move(batch.mask),
                std::move(batch.meta)};
}

} // namespace tenzir::nova
