//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>
#include <unordered_set>

namespace tenzir {

/// Stores the feature set advertised by the platform when it connects.
///
/// The platform negotiates its supported features during the WebSocket
/// handshake (see the `platform` plugin). Other plugins, such as the
/// pipeline manager, need to know which features the platform understands so
/// they can adjust the data they send to it. Because these plugins do not
/// share state with the platform plugin, we keep the negotiated feature set in
/// this process-global, thread-safe store.
///
/// Replaces the currently stored feature set with @p features. Passing an
/// empty set (e.g. on disconnect) clears all features.
auto set_platform_features(std::unordered_set<std::string> features) -> void;

/// Returns whether the platform advertised the given feature on its most
/// recent connection. Returns `false` if no platform is connected.
auto platform_supports_feature(std::string_view feature) -> bool;

} // namespace tenzir
