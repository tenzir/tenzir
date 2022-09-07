//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "rest/server_state.hpp"

#include <vast/detail/base64.hpp>

#include <array>

// TODO: multiplatform
#include <sys/random.h>

namespace vast::plugins::rest {

server_state& server_singleton() {
  static server_state state = {};
  return state;
}

void server_state::initialize(bool require_authentication) {
  require_authentication_ = require_authentication;
}

auto server_state::generate() -> token_t {
  auto result = std::array<char, 16>{};
  // FIXME: check return value
  ::getrandom(result.data(), result.size(), 0);
  auto string
    = detail::base64::encode(std::string_view{result.data(), result.size()});
  std::unique_lock<std::mutex> lock(tokens_mutex_);
  tokens_.push_back(string);
  return string;
}

bool server_state::authenticate(token_t x) const {
  if (!require_authentication_)
    return true;
  std::unique_lock<std::mutex> lock(tokens_mutex_);
  return std::find_if(tokens_.begin(), tokens_.end(),
                      [&](const token_t& token) {
                        return token == x;
                      })
         != tokens_.end();
}

} // namespace vast::plugins::rest
