//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/metric_handler.hpp"
#include "tenzir/type.hpp"

namespace tenzir {

inline auto make_metric_handler(OpCtx& ctx, const type& metric_type)
  -> metric_handler {
  auto receiver = ctx.metrics_receiver();
  if (not receiver) {
    return {};
  }
  /// We just always use operator index 0 here, as the integral operator index
  /// does not translate to the new executor; It didnt even work correctly in
  /// the old executor.
  return metric_handler{std::move(receiver), uint64_t{0}, metric_type};
}

} // namespace tenzir
