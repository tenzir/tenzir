//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace tenzir {

/// Whether `tenzir` runs with the unified command interface, which
/// `TENZIR_UNIFIED` opts into. It reads the variable as the `tenzir`
/// dispatcher does, which passes it on: an unset or empty value, or `0`,
/// `false` or `no` in any case, keeps the default interface.
auto unified_mode() -> bool;

} // namespace tenzir
