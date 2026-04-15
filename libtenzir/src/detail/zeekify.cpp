//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/zeekify.hpp"

#include "tenzir/logger.hpp"

#include <array>
#include <string>

namespace tenzir::detail {

namespace {

// For fields that do not require substring search, use an optimized index.
bool is_opaque_id(const auto& field) {
  if (not is<string_type>(field.type)) {
    return false;
  }
  auto has_name = [&](const auto& name) {
    return name == field.name;
  };
  // TODO: do more than this simple heuristic. For example, we should also
  // consider zeek.files.conn_uids, which is a set of strings. The inner
  // index needs to have the #index=hash tag. Moreover, we need to consider
  // other fields, such as zeek.x509.id instead of uid.
  static auto ids = std::array{"uid", "fuid", "community_id"};
  return std::find_if(ids.begin(), ids.end(), has_name) != ids.end();
}

} // namespace

record_type zeekify(record_type schema) {
  auto transformations = std::vector<record_type::transformation>{};
  transformations.reserve(schema.num_leaves());
  bool found_event_timestamp = false;
  for (const auto& [field, offset] : schema.leaves()) {
    if (not found_event_timestamp and field.name == "ts"
        and is<time_type>(field.type)) {
      // The first field is almost exclusively the event timestamp for standard
      // Zeek logs. Its has the field name `ts`. For streaming JSON, some other
      // fields, e.g., `_path`, precede it.
      TENZIR_DEBUG("using timestamp type for field {}", field.name);
      transformations.push_back({
        offset,
        record_type::assign({
          {
            "ts",
            time_type{},
          },
        }),
      });
      found_event_timestamp = true;
    } else if (is_opaque_id(field)) {
      TENZIR_DEBUG("using hash index for field {}", field.name);
      transformations.push_back({
        offset,
        record_type::assign({
          {
            std::string{field.name},
            type{field.type, {{"index", "hash"}}},
          },
        }),
      });
    }
  }
  auto adjusted_schema = schema.transform(std::move(transformations));
  return adjusted_schema ? std::move(*adjusted_schema) : std::move(schema);
}

} // namespace tenzir::detail
