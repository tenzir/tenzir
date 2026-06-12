//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/execution_node_name_guard.hpp"

namespace tenzir {

exec_node_name_guard::exec_node_name_guard(const name_type& name, type t) {
  operator_name = name;
  operator_type = t;
}

exec_node_name_guard::~exec_node_name_guard() {
  operator_type = type::none;
}

} // namespace tenzir
