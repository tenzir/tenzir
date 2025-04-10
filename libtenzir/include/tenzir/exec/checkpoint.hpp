//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace tenzir::exec {

struct checkpoint {
  friend auto inspect(auto& f, checkpoint& x) -> bool {
    return f.object(x).fields();
  }
};

} // namespace tenzir::exec
