//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/function_plugin.hpp"

#include <concepts>

namespace tenzir::nova {

/// A record alternative together with the requested rows where it is present.
struct RecordArgument {
  Array<Record> data;
  storage::BitMap present;
};

/// Resolves the record alternative of an evaluated argument. Null values
/// propagate silently. Requested rows of any other type result in a diagnostic
/// that names the offending type. Returns `None` if no requested row holds a
/// record, in which case callers produce an all-null result.
inline auto resolve_record(ValueArgument const& arg, EvalFrame frame)
  -> Option<RecordArgument> {
  auto records = arg.data.get_alternative<Record>();
  auto present = records ? frame.mask() & records->present
                         : storage::BitMap{frame.length(), false};
  auto invalid = frame.mask().and_not(present);
  if (auto nulls = arg.data.get_alternative<Null>()) {
    invalid = std::move(invalid).and_not(nulls->present);
  }
  if (invalid.any()) {
    auto warn = [&]<data_type Tag>(Array<Tag> const&) {
      if constexpr (not std::same_as<Tag, Record>
                    and not std::same_as<Tag, Null>) {
        diagnostic::warning("expected `record`, got `{}`",
                            Type<Tag>::static_name)
          .primary(arg.source)
          .emit(frame);
      }
    };
    match(arg.data, warn, [&](UnionArray const& array) {
      for (auto const& field : array.fields()) {
        if ((invalid & field.present).any()) {
          match(field.data, warn);
        }
      }
    });
  }
  if (not records or not present.any()) {
    return None{};
  }
  return RecordArgument{std::move(records->data), std::move(present)};
}

} // namespace tenzir::nova
