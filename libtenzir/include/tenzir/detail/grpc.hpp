//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/result.hpp"
#include "tenzir/tls_options.hpp"

#include <grpcpp/security/server_credentials.h>

#include <memory>
#include <string>
#include <string_view>

namespace tenzir::detail {

/// Extracts the address from a gRPC IPv4 or IPv6 peer string.
auto grpc_peer_ip(std::string_view peer) -> Result<std::string, std::string>;

/// Creates gRPC server credentials from resolved Tenzir TLS options.
///
/// This function performs blocking certificate file reads and must run on the
/// blocking executor.
auto make_grpc_server_credentials(TlsConfig const& tls,
                                  location fallback_location)
  -> Result<std::shared_ptr<grpc::ServerCredentials>, diagnostic>;

} // namespace tenzir::detail
