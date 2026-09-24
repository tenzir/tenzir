//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/union_array.hpp"

namespace tenzir::nova {

auto _::EvalRun::eval(const ast::meta& x, EvalFrame frame) -> Array<Data> {
  if (not input_) {
    return fail_not_constant(x.get_location(), std::move(frame));
  }
  switch (x.kind) {
    case ast::meta_kind::name: {
      return input_->meta.name;
    }
    case ast::meta_kind::import_time: {
      // The import time is not nullable. Like the legacy evaluator, read the
      // default-constructed time, which marks a missing value, as `null`.
      auto const& import_time = input_->meta.import_time;
      auto missing = storage::BitMap::Builder{};
      for (auto row = storage::Index{0}; row < import_time.length(); ++row) {
        missing.emplace_back(*import_time.get(row) == Time{});
      }
      return Array<Data>{import_time}.null_where(missing.finish());
    }
    case ast::meta_kind::internal: {
      return input_->meta.internal;
    }
  }
}
} // namespace tenzir::nova
