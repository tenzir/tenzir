//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

namespace tenzir {

template <class T>
class Box;

/// The base class of all runtime operators.
///
/// `MultipleOutputPorts` selects between the single-port `process(Input,
/// Push<Output>&, OpCtx&)` and the multi-port `process(Input,
/// PushPorts<Output>&, OpCtx&)` interface. Only operators that route items to
/// more than one downstream branch (`if`, `match`, `fork`, `fork_merge`) opt
/// into the latter.
///
/// Multiple output ports are only available for operators that both consume
/// and produce items. Sources (`Input == void`) have no `process` overload to
/// begin with, and sinks (`Output == void`) have nothing to route.
template <class Input, class Output, bool MultipleOutputPorts = false>
class Operator;

/// A type-erased runtime operator.
///
/// This enumerates only the element type combinations that operators actually
/// use; add new alternatives on demand. Every alternative multiplies the number
/// of lambda instantiations in the generic `match` over this variant, of which
/// the executor alone has a dozen.
using AnyOperator = variant<
  Box<Operator<void, void>>, Box<Operator<void, chunk_ptr>>,
  Box<Operator<void, table_slice>>, Box<Operator<chunk_ptr, chunk_ptr>>,
  Box<Operator<chunk_ptr, table_slice>>, Box<Operator<table_slice, chunk_ptr>>,
  Box<Operator<table_slice, table_slice>>,
  Box<Operator<table_slice, table_slice, true>>,
  Box<Operator<table_slice, void>>, Box<Operator<chunk_ptr, void>>>;

} // namespace tenzir
