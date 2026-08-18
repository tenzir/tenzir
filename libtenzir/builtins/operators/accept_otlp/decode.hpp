//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <span>

#include "types.hpp"

namespace tenzir::plugins::accept_otlp::detail {

auto decode(Signal signal, Encoding encoding, std::span<std::byte const> bytes,
            DecodeContext ctx) -> DecodeResult;

auto decode(GrpcRequest request, DecodeContext ctx) -> DecodeResult;

auto make_decode_context(RequestMetadata const& metadata,
                         AcceptOtlpArgs const& args)
  -> Result<DecodeContext, std::string>;

} // namespace tenzir::plugins::accept_otlp::detail
