//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/series_builder.hpp"

namespace tenzir::detail {

auto make_actor_metrics_builder() -> series_builder {
  return series_builder{type{
    "tenzir.metrics.actor",
    record_type{
      {"timestamp", time_type{}},
      {"id", string_type{}},
      {"name", string_type{}},
      {"inbox_size", uint64_type{}},
    },
    {{"internal"}},
  }};
}

} // namespace tenzir::detail
