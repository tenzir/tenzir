//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <array>

namespace tenzir {

class exec_node_name_guard {
public:
  using name_type = std::array<char, 15>;
  thread_local static inline name_type operator_name = {};
  exec_node_name_guard(const name_type& name);
  exec_node_name_guard(const exec_node_name_guard&) = delete;
  ~exec_node_name_guard();
};

} // namespace tenzir
