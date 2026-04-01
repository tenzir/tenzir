//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/expression.hpp>

namespace tenzir {

// Returns an expression for catalog lookup.
// This is currently a no-op because broadening field predicates for lookup can
// change which candidate partitions are returned in ways that are expensive to
// recover from later.
expression prune(expression e, const detail::heterogeneous_string_hashset& hs);

} // namespace tenzir
