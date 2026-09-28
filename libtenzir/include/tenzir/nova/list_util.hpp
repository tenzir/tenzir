//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/function_plugin.hpp"

namespace tenzir::nova {

/// A list alternative together with the requested rows where it is present.
struct ListArgument {
  Array<List> data;
  storage::BitMap present;
};

/// Resolves the list alternative of an evaluated argument. Null values
/// propagate silently. By default, values of other types result in a
/// diagnostic; callers with more specific diagnostics may disable it.
inline auto resolve_list(ValueArgument const& arg, EvalFrame frame,
                         bool diagnose = true) -> Option<ListArgument> {
  auto const& requested = frame.mask();
  return match(
    arg.data,
    [&](Array<List> const& list) -> Option<ListArgument> {
      return ListArgument{list, requested};
    },
    [&](Array<Null> const&) -> Option<ListArgument> {
      return None{};
    },
    [&](UnionArray const& array) -> Option<ListArgument> {
      auto list = array.get_alternative<List>();
      auto present = list ? requested & list->present
                          : storage::BitMap{frame.length(), false};
      auto invalid
        = requested.and_not(present).and_not(array.alternative_mask<Null>());
      if (diagnose and invalid.any()) {
        diagnostic::warning("expected `list`, got a different type")
          .primary(arg.source)
          .emit(frame);
      }
      if (not list) {
        return None{};
      }
      return ListArgument{std::move(list->data), std::move(present)};
    },
    [&]<data_type Tag>(Array<Tag> const&) -> Option<ListArgument> {
      if (diagnose) {
        diagnostic::warning("expected `list`, got `{}`", Type<Tag>::static_name)
          .primary(arg.source)
          .emit(frame);
      }
      return None{};
    });
}

} // namespace tenzir::nova
