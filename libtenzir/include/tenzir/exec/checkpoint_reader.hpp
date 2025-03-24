//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <caf/type_list.hpp>

namespace tenzir::exec {

struct checkpoint_reader_traits {
  using signatures
    = caf::type_list<auto(uuid id, uint64_t index)->caf::result<chunk_ptr>>;
};

using checkpoint_reader_actor = caf::typed_actor<checkpoint_reader_traits>;

} // namespace tenzir::exec
