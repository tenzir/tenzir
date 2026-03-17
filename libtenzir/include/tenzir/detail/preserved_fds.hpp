//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#if __has_include(<boost/process/v1/detail/handler.hpp>)

#  include <boost/process/v1/detail/handler.hpp>
#  include <boost/process/v1/handles.hpp>

namespace bp = boost::process::v1;

#else

#  include <boost/process/detail/handler.hpp>
#  include <boost/process/handles.hpp>

namespace bp = boost::process;

#endif

#include <vector>

namespace tenzir::detail {

struct preserved_fds : bp::detail::handler, bp::detail::uses_handles {
  std::vector<int> fds;
  preserved_fds(std::vector<int> pfds);

  auto get_used_handles() -> std::vector<int>&;
};

} // namespace tenzir::detail
