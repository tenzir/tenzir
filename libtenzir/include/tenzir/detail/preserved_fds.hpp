//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <boost/process/detail/handler.hpp>
#include <boost/process/handles.hpp>

#include <vector>

namespace tenzir::detail {

struct preserved_fds : boost::process::detail::handler,
                       boost::process::detail::uses_handles {
  std::vector<int> fds;
  preserved_fds(std::vector<int> pfds);

  auto get_used_handles() -> std::vector<int>&;
};

} // namespace tenzir::detail
