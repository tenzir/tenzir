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
#include "tenzir/location.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <span>
#include <string>
#include <vector>

namespace tenzir {

/// Opaque parsed URL template for filesystem sink operators.
///
/// Encapsulates the template syntax (currently `**` and `{uuid}`, but
/// designed to be swapped out without changing callers).  The base operator
/// and derived classes interact with URLs only through this type.
class FsUrlTemplate {
public:
  /// Parse a resolved URL string into a template.
  ///
  /// Extracts partition field names, validates structure, and sanitizes
  /// placeholders so the URL can be passed to Arrow URI / Options parsers.
  /// Emits diagnostics and returns failure on invalid templates.
  static auto
  parse(std::string url, Option<ast::expression> const& partition_by,
        location url_loc, diagnostic_handler& dh) -> failure_or<FsUrlTemplate>;

  /// The URL with template placeholders replaced by URI-safe tokens.
  /// Safe to pass to `arrow::util::Uri::Parse` and `*Options::FromUri`.
  auto sanitized_url() const -> std::string const&;

  /// Partition field paths inferred from the template (or from `partition_by`
  /// in the current `**` syntax).  Empty when no partitioning is configured.
  auto partition_fields() const -> std::span<ast::field_path const>;

  /// Whether the *path portion* contains a `{uuid}` placeholder that
  /// `fill_path` can expand. Only meaningful after `set_path` has run; a
  /// `{uuid}` that appears only in the URL's authority or query does not
  /// count, because `fill_path` operates on the path alone.
  auto has_uuid() const -> bool;

  /// Store the path extracted from `make_filesystem`'s result, undoing the
  /// sanitization. Finalises `has_uuid()` against the actual path and emits
  /// any placeholder-related diagnostics. Call exactly once, after
  /// `make_filesystem` returns.
  void set_path(std::string path_with_tokens, location url_loc,
                diagnostic_handler& dh);

  /// Expand the stored path template with concrete partition values and a
  /// fresh UUIDv7.  `key` is the composite partition key (a `list` when
  /// multiple fields are used, or `data{}` when unpartitioned).
  auto fill_path(data const& key) const -> std::string;

private:
  std::string sanitized_url_;
  std::string path_template_;
  std::vector<ast::field_path> partition_fields_;
  /// `{uuid}` appeared somewhere in the original URL. Records the user's
  /// *intent*; `has_uuid_` below is the authoritative flag for rotation.
  bool url_has_uuid_ = false;
  bool has_uuid_ = false;

  // Mapping from safe tokens back to real placeholders.
  // Implementation detail — depends on the template syntax.
  struct Replacement {
    std::string token;
    std::string original;
  };
  std::vector<Replacement> replacements_;
};

} // namespace tenzir
