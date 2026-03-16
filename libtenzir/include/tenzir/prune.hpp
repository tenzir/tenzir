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

// Optimizes a given expression specifically for the catalog lookup.
// Currently this does only a single optimization: It deduplicates string
// lookups for the type level string synopsis.
// This is most relevant when looking up using concepts, to rewrite queries like
//
//      (suricata.dns.dns.rrname == "u8wm3g4pw100420ydpzc"
//       || suricata.http.http.hostname == "u8wm3g4pw100420ydpzc")
//       || (suricata.fileinfo.http.hostname == "u8wm3g4pw100420ydpzc")
//
// to
//
//     ':string == "u8wm3g4pw100420ydpzc"'
expression prune(expression e, const detail::heterogeneous_string_hashset& hs);

} // namespace tenzir
