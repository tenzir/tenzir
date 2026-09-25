//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#pragma once

#include "tenzir/nova/events.hpp"
#include "tenzir/result.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/type.hpp"

#include <string>
#include <utility>
#include <vector>

namespace tenzir::nova {

/// Retains selected events while null-compatible output schemas are inferred.
/// A failed add leaves the buffer unchanged. Each output has one concrete
/// schema and keeps the import times of its selected rows.
class ImportConversionBuffer {
public:
  ImportConversionBuffer(std::string name, bool internal);

  auto add(Events events, storage::BitMap selection)
    -> Result<void, std::string>;
  auto snapshot() const -> Result<std::vector<table_slice>, std::string>;
  auto selected_events() const -> std::vector<std::pair<type, Events>>;
  auto rows() const -> size_t;

private:
  struct Selection {
    size_t batch;
    storage::BitMap mask;
  };

  struct Candidate {
    record_type schema;
    std::vector<Selection> selections;
  };

  std::string name_;
  bool internal_;
  size_t rows_ = 0;
  std::vector<Events> batches_;
  std::vector<Candidate> candidates_;
};

/// Adapts an existing Arrow slice for local subscribers without changing its
/// schema, metadata, or import timestamp.
auto import_table_slice(table_slice const& slice)
  -> Result<Events, std::string>;

} // namespace tenzir::nova
