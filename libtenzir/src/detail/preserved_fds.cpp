//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/preserved_fds.hpp"

namespace tenzir::detail {

preserved_fds::preserved_fds(std::vector<int> pfds) : fds(std::move(pfds)) {
}
auto preserved_fds::get_used_handles() -> std::vector<int>& {
  return fds;
}

} // namespace tenzir::detail
