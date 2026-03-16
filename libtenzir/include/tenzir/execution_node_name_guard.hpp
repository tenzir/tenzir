//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <array>
#include <cstdint>

namespace tenzir {

class exec_node_name_guard {
public:
  enum type : std::uint8_t { none, actor, folly };
  using name_type = std::array<char, 15>;
  thread_local static inline name_type operator_name = {};
  thread_local static inline type operator_type = type::none;
  exec_node_name_guard(const name_type& name, type t);
  exec_node_name_guard(const exec_node_name_guard&) = delete;
  exec_node_name_guard& operator=(const exec_node_name_guard&) = delete;
  exec_node_name_guard(exec_node_name_guard&&) = delete;
  exec_node_name_guard& operator=(exec_node_name_guard&&) = delete;
  ~exec_node_name_guard();
};

} // namespace tenzir
