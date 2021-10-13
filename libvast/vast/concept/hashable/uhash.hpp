//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/hash_append.hpp"

namespace vast {

/// The universal hash function.
template <class HashAlgorithm>
class uhash {
public:
  using result_type = typename HashAlgorithm::result_type;

  template <class... Ts>
  uhash(Ts&&... xs) : h_(std::forward<Ts>(xs)...) {
  }

  template <class T>
  result_type operator()(const T& x) noexcept {
    hash_append(h_, x);
    return static_cast<result_type>(h_);
  }

private:
  HashAlgorithm h_;
};

} // namespace vast
