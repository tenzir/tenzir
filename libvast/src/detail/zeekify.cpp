//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/zeekify.hpp"

#include "vast/detail/array.hpp"
#include "vast/logger.hpp"

#include <string>

namespace vast::detail {

using namespace std::string_literals;

record_type zeekify(record_type layout) {
  // The first field is almost exclusively the event timestamp for standard
  // Zeek logs. Its has the field name `ts`. For streaming JSON, some other
  // fields, e.g., `_path`, precede it.
  for (auto& field : layout.fields)
    if (field.name == "ts")
      if (auto ts = caf::get_if<time_type>(&field.type)) {
        VAST_DEBUG("using timestamp type for field {}", field.name);
        // field.type = alias_type{field.type}.name("timestamp");
        field.type.name("timestamp");
        break;
      }
  // For fields that do not require substring search, use an optimized index.
  auto is_opaque_id = [](const auto& field) {
    if (!caf::holds_alternative<string_type>(field.type))
      return false;
    auto has_name = [&](const auto& name) {
      return name == field.name;
    };
    // TODO: do more than this simple heuristic. For example, we should also
    // consider zeek.files.conn_uids, which is a set of strings. The inner
    // index needs to have the #index=hash tag. Moreover, we need to consider
    // other fields, such as zeek.x509.id instead of uid.
    static auto ids = make_array("uid", "fuid", "community_id");
    return std::find_if(ids.begin(), ids.end(), has_name) != ids.end();
  };
  for (auto& field : layout.fields)
    if (is_opaque_id(field)) {
      VAST_DEBUG("using hash index for field {}", field.name);
      field.type.update_attributes({{"index", "hash"}});
    }
  return layout;
}

} // namespace vast::detail
