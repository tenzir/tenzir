//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/hash/concepts.hpp"
#include "tenzir/hash/hash_append.hpp"

namespace tenzir {

/// The universal hash function.
template <reusable_hash HashAlgorithm>
class uhash {
public:
  using result_type = typename HashAlgorithm::result_type;

  template <class... Ts>
  uhash(Ts&&... xs) : h_(std::forward<Ts>(xs)...) {
  }

  template <class T>
  result_type operator()(const T& x) noexcept {
    h_.reset();
    hash_append(h_, x);
    return h_.finish();
  }

private:
  HashAlgorithm h_;
};

} // namespace tenzir
