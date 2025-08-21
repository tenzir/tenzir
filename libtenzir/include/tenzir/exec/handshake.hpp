//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/exec/message.hpp"

namespace tenzir::exec {

struct handshake {
  handshake() = default;

  explicit(false) handshake(any_stream input) : input{std::move(input)} {
  }

  any_stream input;

  friend auto inspect(auto& f, handshake& x) -> bool {
    return f.apply(x.input);
  }
};

struct handshake_response {
  any_stream output;

  friend auto inspect(auto& f, handshake_response& x) -> bool {
    return f.apply(x.output);
  }
};

} // namespace tenzir::exec
