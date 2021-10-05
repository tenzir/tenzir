//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:

#pragma once

#include <cstdlib>
#include <memory>

namespace vast::detail {

template <class T>
struct delete_aligned {
  void operator()(T* x) const {
    std::free(x);
  }
};

/// Performs aligned memory allocation.
template <class T>
std::unique_ptr<T[], delete_aligned<T>>
allocate_aligned(size_t alignment, size_t size) {
  auto ptr = std::aligned_alloc(alignment, size);
  return {static_cast<T*>(ptr), delete_aligned<T>{}};
}

template <class T>
using aligned_unique_ptr = std::unique_ptr<T[], delete_aligned<T>>;

} // namespace vast::detail
