//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/generator.hpp"

namespace tenzir::detail {

/// Decodes a Feather (Arrow IPC) stream into table slices.
///
/// The decoder consumes the byte stream incrementally, so callers can feed it
/// a `generator<chunk_ptr>` whose chunk boundaries do not align with the
/// underlying Arrow IPC message boundaries. It yields empty slices for
/// backpressure whenever it needs more input, and stops after the stream's
/// end-of-stream marker.
///
/// The input must carry Tenzir's schema metadata; batches without it abort
/// decoding with an error diagnostic.
auto parse_feather(generator<chunk_ptr> input, diagnostic_handler& dh)
  -> generator<table_slice>;

} // namespace tenzir::detail
