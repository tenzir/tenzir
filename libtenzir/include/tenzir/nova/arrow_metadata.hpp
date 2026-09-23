//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/events.hpp"
#include "tenzir/type.hpp"

#include <arrow/type_fwd.h>

#include <string>
#include <string_view>

namespace tenzir::nova {

/// Schema-level metadata shared by Arrow readers and writers. Import time is
/// not schema metadata; format-specific envelopes handle it separately.
struct ArrowMetadata {
  std::string name;
  bool internal = false;

  static auto from_arrow(arrow::Schema const& schema,
                         std::string_view fallback_name = {}) -> ArrowMetadata;

  auto to_meta(storage::Index length) const -> Events::Meta;
  auto apply(type schema) const -> type;
};

} // namespace tenzir::nova
