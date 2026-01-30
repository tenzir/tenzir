//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <folly/coro/UnboundedQueue.h>

namespace tenzir {

template <class T>
using UnboundedQueue = folly::coro::UnboundedQueue<T>;

} // namespace tenzir
