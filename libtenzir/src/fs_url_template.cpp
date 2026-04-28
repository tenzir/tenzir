//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fs_url_template.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/glob.hpp"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/version.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#if BOOST_VERSION < 108600 || defined(TENZIR_FORCE_BOOST_UUID_COMPAT)
#  include <tenzir/detail/boost_uuid_generators.hpp>
#endif

#include <ranges>

namespace tenzir {
namespace {

auto replace_all(std::string& str, std::string_view from, std::string_view to)
  -> void {
  auto pos = size_t{0};
  while ((pos = str.find(from, pos)) != std::string::npos) {
    str.replace(pos, from.size(), to);
    pos += to.size();
  }
}

// Convert an ast::field_path to a dotted string for use as a hive partition
// directory name component (e.g. `region` or `geo.country`).
auto selector_to_name(ast::field_path const& sel) -> std::string {
  auto path = sel.path() | std::views::transform(&ast::field_path::segment::id)
              | std::views::transform(&ast::identifier::name);
  return fmt::to_string(fmt::join(path, "."));
}

auto hive_partition_value(data const& value) -> std::string {
  return match(
    value,
    [](std::string const& x) -> std::string {
      return x;
    },
    [&](auto const&) -> std::string {
      return fmt::to_string(value);
    });
}

} // namespace

auto FsUrlTemplate::parse(std::string url,
                          Option<ast::expression> const& partition_by,
                          location url_loc, diagnostic_handler& dh)
  -> failure_or<FsUrlTemplate> {
  auto result = FsUrlTemplate{};

  // Extract partition fields from the partition_by expression.
  //
  // Current syntax: `**` in the URL marks where hive paths go, and
  // partition_by lists the fields.  A future syntax may infer fields
  // directly from `{field}` placeholders in the URL instead.
  if (partition_by) {
    if (auto const* l = try_as<ast::list>(*partition_by)) {
      for (auto const& item : l->items) {
        match(
          item, [](ast::spread const&) { /* rejected in describe_to */ },
          [&](ast::expression const& expr) {
            auto fp = ast::field_path::try_from(expr);
            // describe_to rejects non-field-path list items.
            TENZIR_ASSERT(fp);
            result.partition_fields_.push_back(std::move(*fp));
          });
      }
    } else {
      // describe_to rejects non-list, non-field-path partition_by values.
      auto fp = ast::field_path::try_from(*partition_by);
      TENZIR_ASSERT(fp);
      result.partition_fields_.push_back(std::move(*fp));
    }
  }

  // Parse the URL as a glob so we can locate `**` segments from the parse
  // result rather than searching for the raw `**` substring, which would
  // mis-identify the wrong occurrence if the URL happens to contain more
  // than one `**` (only one is supported as a partition placeholder).
  auto parts = parse_glob(url);
  auto const glob_count = std::ranges::count_if(parts, [](auto const& p) {
    return is<glob_star_star>(p);
  });
  auto const has_glob = glob_count > 0;
  result.url_has_uuid_ = url.contains("{uuid}");

  if (not result.partition_fields_.empty() and not has_glob) {
    diagnostic::error("URL must contain `**` when using `partition_by`")
      .primary(url_loc)
      .emit(dh);
    return failure::promise();
  }
  if (result.partition_fields_.empty() and has_glob) {
    diagnostic::error("URL contains `**` but `partition_by` is not set")
      .primary(url_loc)
      .emit(dh);
    return failure::promise();
  }
  if (glob_count > 1) {
    diagnostic::error("URL must contain at most one `**` placeholder")
      .primary(url_loc)
      .emit(dh);
    return failure::promise();
  }
  // `{uuid}`-placement diagnostics are deferred to `set_path`, where the
  // actual path portion is known. A `{uuid}` in the authority or query
  // never reaches `fill_path`, so we can only tell if it's operationally
  // present once the filesystem has handed us back the path.

  // Sanitize: replace placeholders with URI-safe tokens so that derived
  // make_filesystem() implementations can parse the URL with
  // arrow::util::Uri / *Options::FromUri without hitting invalid chars.
  //
  // Tokens embed a per-parse random UUID so that a user path containing
  // the literal token prefix is never mis-identified during
  // un-sanitization (`set_path`).
  result.sanitized_url_ = std::move(url);
  auto const suffix
    = boost::uuids::to_string(boost::uuids::random_generator{}());
  auto const glob_token = fmt::format("__TNZ_GLOB_{}__", suffix);
  auto const uuid_token = fmt::format("__TNZ_UUID_{}__", suffix);

  if (has_glob) {
    // Compute the byte offset of the glob `**` from the parsed parts rather
    // than via `find("**")`. The two agree for the current parse_glob
    // (which treats every `**` as a glob_star_star), but going through the
    // parse result keeps the sanitization aligned with what the glob parser
    // actually recognises and makes future parser changes safe.
    auto pos = size_t{0};
    for (auto const& part : parts) {
      if (is<glob_star_star>(part)) {
        break;
      }
      pos += match(
        part,
        [](std::string const& s) -> size_t {
          return s.size();
        },
        [](glob_star) -> size_t {
          return 1;
        },
        [](glob_star_star const& ss) -> size_t {
          return ss.slash ? 3 : 2;
        });
    }
    TENZIR_ASSERT(pos + 2 <= result.sanitized_url_.size());
    result.replacements_.push_back({glob_token, "**"});
    result.sanitized_url_.replace(pos, 2, glob_token);
  }
  if (result.url_has_uuid_) {
    result.replacements_.push_back({uuid_token, "{uuid}"});
    replace_all(result.sanitized_url_, "{uuid}", uuid_token);
  }

  return result;
}

// TODO: What if the sanitized URL is too long?
auto FsUrlTemplate::sanitized_url() const -> std::string const& {
  return sanitized_url_;
}

auto FsUrlTemplate::partition_fields() const
  -> std::span<ast::field_path const> {
  return partition_fields_;
}

auto FsUrlTemplate::has_uuid() const -> bool {
  return has_uuid_;
}

void FsUrlTemplate::set_path(std::string path, location url_loc,
                             diagnostic_handler& dh) {
  for (auto const& r : replacements_) {
    replace_all(path, r.token, r.original);
  }
  path_template_ = std::move(path);
  // Authoritative check: only a `{uuid}` in the path portion can be
  // expanded by `fill_path`. One that ended up in the authority/query is
  // invisible here and would silently cause rotations to overwrite.
  has_uuid_ = path_template_.contains("{uuid}");
  if (url_has_uuid_ and not has_uuid_) {
    diagnostic::warning("`{{uuid}}` placeholder does not appear in the "
                        "object path and will not be expanded")
      .primary(url_loc)
      .note("`{{uuid}}` must be in the URL's path portion for rotation "
            "to produce unique files")
      .emit(dh);
  }
  if (not partition_fields_.empty() and not has_uuid_) {
    diagnostic::warning("URL has no `{{uuid}}` placeholder in its path; "
                        "files for the same partition key will overwrite "
                        "each other on rotation")
      .primary(url_loc)
      .emit(dh);
  }
}

auto FsUrlTemplate::fill_path(data const& key) const -> std::string {
  auto result = path_template_;

  // Replace `**` with the hive-partitioned directory path.
  // Example: partition_by=[region, country], key=list{"us","ny"}
  // yields "region=us/country=ny".
  if (auto pos = result.find("**"); pos != std::string::npos) {
    auto hive = std::string{};
    for (auto const& [field, val] :
         std::views::zip(partition_fields_, as<list>(key))) {
      if (not hive.empty()) {
        hive += '/';
      }
      hive += fmt::format("{}={}", selector_to_name(field),
                          hive_partition_value(val));
    }
    result.replace(pos, 2, hive);
  }

  // Replace each `{uuid}` with a *separate* fresh UUIDv7. Reusing one
  // UUID for every occurrence would surprise a user who placed multiple
  // `{uuid}` markers to mean "two distinct IDs".
  // Each thread gets its own generator so no synchronisation is needed.
  thread_local auto uuid_gen = boost::uuids::time_generator_v7{};
  for (auto pos = result.find("{uuid}"); pos != std::string::npos;
       pos = result.find("{uuid}", pos)) {
    auto uuid = boost::uuids::to_string(uuid_gen());
    result.replace(pos, 6, uuid);
    pos += uuid.size();
  }

  return result;
}

} // namespace tenzir
