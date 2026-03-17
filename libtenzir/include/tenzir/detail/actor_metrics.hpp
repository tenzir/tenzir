//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/series_builder.hpp"

namespace tenzir::detail {

auto make_actor_metrics_builder() -> series_builder;

template <class Actor>
auto generate_actor_metrics(series_builder& builder, Actor self)
  -> table_slice {
  auto metric = builder.record();
  metric.field("timestamp", time::clock::now());
  metric.field("id", self->id());
  metric.field("name", self->name());
  metric.field("inbox_size", uint64_t{self->mailbox().size()});
  return builder.finish_assert_one_slice();
}

} // namespace tenzir::detail
