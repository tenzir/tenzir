//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <mutex>
#include <string>
#include <vector>

namespace vast::plugins::rest {

// TODO: Maybe rename to `authenticator`?
class server_state {
public:
  using token_t = std::string;

  void initialize(bool require_authentication);

  token_t generate();

  [[nodiscard]] bool authenticate(token_t) const;

  // TODO: authenticate the whole request, not just the token
  // [[nodiscard]] bool authenticate()

private:
  bool require_authentication_ = true;
  // TODO: Add internal mutex
  mutable std::mutex tokens_mutex_;
  std::vector<token_t> tokens_;
};

// TODO: Figure out a better way to share the state
// between the commands.
server_state& server_singleton();

} // namespace vast::plugins::rest
