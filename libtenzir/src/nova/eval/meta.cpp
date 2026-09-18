//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"

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
      return input_->meta.import_time;
    }
    case ast::meta_kind::internal: {
      return input_->meta.internal;
    }
  }
}
} // namespace tenzir::nova
