//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/secret.hpp"
#include "tenzir/nova/union_array.hpp"

#include <string>
#include <string_view>

namespace tenzir::nova {

/// Converts a single nova row into an owning Nova `Data` value.
auto materialize_data(const RowView<Data>& row) -> Data;

/// Converts a single nova row into an owning legacy `data` value at an explicit
/// legacy boundary. Strings are copied, records keep their field order, and
/// lists are converted element-wise.
auto materialize_legacy(const RowView<Data>& row) -> data;

/// Converts an owning Nova `Data` value into a legacy `data` value, like the
/// overload above.
auto materialize_legacy(const Data& value) -> data;

/// Returns the PLAIN TEXT of a secret at an API boundary that consumes it as
/// UTF-8 text. Emits an error naming `name` if the bytes are not valid UTF-8.
auto reveal_string(SecretView secret, std::string_view name, location source,
                   diagnostic_handler& dh) -> failure_or<std::string>;

/// Like `materialize_legacy`, but reveals each nested secret as its PLAIN TEXT
/// UTF-8 string instead of `"***"`. Only for API boundaries that consume
/// resolved credentials.
auto materialize_revealed(const Data& value, std::string_view name,
                          location source, diagnostic_handler& dh)
  -> failure_or<data>;

} // namespace tenzir::nova
