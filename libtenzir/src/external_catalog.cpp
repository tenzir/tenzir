//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/external_catalog.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/double_synopsis.hpp"
#include "tenzir/duration_synopsis.hpp"
#include "tenzir/error.hpp"
#include "tenzir/int64_synopsis.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/uint64_synopsis.hpp"

#include <string_view>

namespace tenzir {

namespace {

auto get_uint(const record& r, std::string_view field)
  -> std::optional<uint64_t> {
  if (const auto* v = get_if<uint64_t>(&r, field)) {
    return *v;
  }
  if (const auto* v = get_if<int64_t>(&r, field); v and *v >= 0) {
    return static_cast<uint64_t>(*v);
  }
  return std::nullopt;
}

auto get_time(const record& r, std::string_view field) -> std::optional<time> {
  if (const auto* v = get_if<time>(&r, field)) {
    return *v;
  }
  if (const auto* v = get_if<std::string>(&r, field)) {
    if (auto parsed = to<time>(*v)) {
      return *parsed;
    }
  }
  return std::nullopt;
}

/// Coerces a JSON-parsed bound into the `data` alternative matching `t`.
auto coerce_bound(const type& t, const data& raw) -> caf::expected<data> {
  return match(
    t, detail::overload{
         [&](const int64_type&) -> caf::expected<data> {
           if (const auto* v = try_as<int64_t>(&raw)) {
             return data{*v};
           }
           if (const auto* v = try_as<uint64_t>(&raw)) {
             return data{static_cast<int64_t>(*v)};
           }
           return caf::make_error(ec::parse_error, "expected an integer bound");
         },
         [&](const uint64_type&) -> caf::expected<data> {
           if (const auto* v = try_as<uint64_t>(&raw)) {
             return data{*v};
           }
           if (const auto* v = try_as<int64_t>(&raw); v and *v >= 0) {
             return data{static_cast<uint64_t>(*v)};
           }
           return caf::make_error(ec::parse_error,
                                  "expected a non-negative integer bound");
         },
         [&](const double_type&) -> caf::expected<data> {
           if (const auto* v = try_as<double>(&raw)) {
             return data{*v};
           }
           if (const auto* v = try_as<int64_t>(&raw)) {
             return data{static_cast<double>(*v)};
           }
           if (const auto* v = try_as<uint64_t>(&raw)) {
             return data{static_cast<double>(*v)};
           }
           return caf::make_error(ec::parse_error, "expected a numeric bound");
         },
         [&](const time_type&) -> caf::expected<data> {
           if (const auto* v = try_as<time>(&raw)) {
             return data{*v};
           }
           if (const auto* v = try_as<std::string>(&raw)) {
             if (auto parsed = to<time>(*v)) {
               return data{*parsed};
             }
           }
           return caf::make_error(ec::parse_error, "expected a time bound");
         },
         [&](const duration_type&) -> caf::expected<data> {
           if (const auto* v = try_as<duration>(&raw)) {
             return data{*v};
           }
           if (const auto* v = try_as<std::string>(&raw)) {
             if (auto parsed = to<duration>(*v)) {
               return data{*parsed};
             }
           }
           return caf::make_error(ec::parse_error, "expected a duration bound");
         },
         [&](const auto&) -> caf::expected<data> {
           return caf::make_error(ec::parse_error,
                                  "unsupported synopsis type; expected one of "
                                  "int64, uint64, double, duration, time");
         },
       });
}

auto parse_synopsis(const record& r) -> caf::expected<external_synopsis> {
  const auto* kind = get_if<std::string>(&r, "type");
  if (not kind) {
    return caf::make_error(ec::parse_error,
                           "synopsis entry is missing a string `type`");
  }
  auto t = type{};
  if (*kind == "int64") {
    t = type{int64_type{}};
  } else if (*kind == "uint64") {
    t = type{uint64_type{}};
  } else if (*kind == "double") {
    t = type{double_type{}};
  } else if (*kind == "duration") {
    t = type{duration_type{}};
  } else if (*kind == "time") {
    t = type{time_type{}};
  } else {
    return caf::make_error(ec::parse_error,
                           fmt::format("unsupported synopsis type `{}`; "
                                       "expected one of int64, "
                                       "uint64, double, duration, time",
                                       *kind));
  }
  const auto* min = r.find("min") != r.end() ? &r.at("min") : nullptr;
  const auto* max = r.find("max") != r.end() ? &r.at("max") : nullptr;
  if (not min or not max) {
    return caf::make_error(ec::parse_error,
                           "synopsis entry is missing `min` or `max`");
  }
  auto min_data = coerce_bound(t, *min);
  if (not min_data) {
    return std::move(min_data.error());
  }
  auto max_data = coerce_bound(t, *max);
  if (not max_data) {
    return std::move(max_data.error());
  }
  return external_synopsis{std::move(t), std::move(*min_data),
                           std::move(*max_data)};
}

auto parse_entry(const record& r) -> caf::expected<external_partition> {
  auto result = external_partition{};
  const auto* id = get_if<std::string>(&r, "id");
  if (not id) {
    return caf::make_error(ec::parse_error,
                           "external catalog entry is missing a string `id`");
  }
  if (not parsers::uuid(*id, result.id)) {
    return caf::make_error(
      ec::parse_error,
      fmt::format("external catalog entry has an invalid `id`: {}", *id));
  }
  const auto* schema = get_if<std::string>(&r, "schema");
  if (not schema) {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} is missing a "
                                       "base64 `schema`",
                                       result.id));
  }
  auto decoded = detail::base64::try_decode<std::string>(*schema);
  if (not decoded) {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} has an "
                                       "invalid base64 `schema`",
                                       result.id));
  }
  result.schema = type{chunk::copy(decoded->data(), decoded->size())};
  if (not result.schema) {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} has an empty "
                                       "`schema`",
                                       result.id));
  }
  if (auto events = get_uint(r, "events")) {
    result.events = *events;
  } else {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} is missing "
                                       "an integer `events`",
                                       result.id));
  }
  if (auto t = get_time(r, "min_import_time")) {
    result.min_import_time = *t;
  } else {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} is missing "
                                       "`min_import_time`",
                                       result.id));
  }
  if (auto t = get_time(r, "max_import_time")) {
    result.max_import_time = *t;
  } else {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} is missing "
                                       "`max_import_time`",
                                       result.id));
  }
  if (result.min_import_time > result.max_import_time) {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} has "
                                       "min_import_time > "
                                       "max_import_time",
                                       result.id));
  }
  if (auto version = get_uint(r, "version")) {
    result.version = *version;
  } else {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog entry {} is missing "
                                       "an integer `version`",
                                       result.id));
  }
  if (const auto* synopses = get_if<list>(&r, "synopses")) {
    result.synopses.reserve(synopses->size());
    for (const auto& entry : *synopses) {
      const auto* rec = try_as<record>(&entry);
      if (not rec) {
        return caf::make_error(ec::parse_error,
                               fmt::format("external catalog entry {} has a "
                                           "non-object synopsis",
                                           result.id));
      }
      auto parsed = parse_synopsis(*rec);
      if (not parsed) {
        return caf::make_error(ec::parse_error,
                               fmt::format("external catalog entry {}: {}",
                                           result.id, parsed.error()));
      }
      result.synopses.push_back(std::move(*parsed));
    }
  }
  return result;
}

} // namespace

auto make_min_max_synopsis(const type& t, const data& min, const data& max)
  -> synopsis_ptr {
  return match(
    t, detail::overload{
         [&](const int64_type&) -> synopsis_ptr {
           return std::make_unique<int64_synopsis>(as<int64_t>(min),
                                                   as<int64_t>(max));
         },
         [&](const uint64_type&) -> synopsis_ptr {
           return std::make_unique<uint64_synopsis>(as<uint64_t>(min),
                                                    as<uint64_t>(max));
         },
         [&](const double_type&) -> synopsis_ptr {
           return std::make_unique<double_synopsis>(as<double>(min),
                                                    as<double>(max));
         },
         [&](const duration_type&) -> synopsis_ptr {
           return std::make_unique<duration_synopsis>(as<duration>(min),
                                                      as<duration>(max));
         },
         [&](const time_type&) -> synopsis_ptr {
           return std::make_unique<time_synopsis>(as<time>(min), as<time>(max));
         },
         [&](const auto&) -> synopsis_ptr {
           return nullptr;
         },
       });
}

auto load_external_catalog(const std::filesystem::path& path)
  -> caf::expected<std::vector<external_partition>> {
  auto bytes = io::read(path);
  if (not bytes) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to read external catalog {}: {}",
                                       path, bytes.error()));
  }
  auto json = from_json(std::string_view{
    reinterpret_cast<const char*>(bytes->data()), bytes->size()});
  if (not json) {
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to parse external catalog {} as "
                                       "JSON: {}",
                                       path, json.error()));
  }
  // The manifest is either a record with a `partitions` list or a bare list.
  const list* entries = nullptr;
  if (const auto* rec = try_as<record>(&*json)) {
    entries = get_if<list>(rec, "partitions");
    if (not entries) {
      return caf::make_error(
        ec::parse_error, fmt::format("external catalog {} object is missing "
                                     "a `partitions` array",
                                     path));
    }
  } else if (const auto* lst = try_as<list>(&*json)) {
    entries = lst;
  } else {
    return caf::make_error(ec::parse_error,
                           fmt::format("external catalog {} must be a JSON "
                                       "object or array",
                                       path));
  }
  auto result = std::vector<external_partition>{};
  result.reserve(entries->size());
  for (const auto& entry : *entries) {
    const auto* rec = try_as<record>(&entry);
    if (not rec) {
      return caf::make_error(ec::parse_error,
                             fmt::format("external catalog {} contains a "
                                         "non-object partition entry",
                                         path));
    }
    auto parsed = parse_entry(*rec);
    if (not parsed) {
      return std::move(parsed.error());
    }
    result.push_back(std::move(*parsed));
  }
  return result;
}

} // namespace tenzir
