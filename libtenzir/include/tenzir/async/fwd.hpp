//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

namespace tenzir {

template <class T>
class Box;

template <class Input, class Output>
class Operator;

using AnyOperator = variant<
  Box<Operator<void, void>>, Box<Operator<void, chunk_ptr>>,
  Box<Operator<void, table_slice>>, Box<Operator<chunk_ptr, chunk_ptr>>,
  Box<Operator<chunk_ptr, table_slice>>, Box<Operator<table_slice, chunk_ptr>>,
  Box<Operator<table_slice, table_slice>>, Box<Operator<table_slice, void>>,
  Box<Operator<chunk_ptr, void>>>;

} // namespace tenzir
