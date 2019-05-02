/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <unordered_map>

#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/expected.hpp"
#include "vast/format/multi_layout_reader.hpp"
#include "vast/json.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/view.hpp"

namespace vast::format::json {

struct suricata {
  caf::optional<vast::record_type> operator()(const vast::json::object& j) {
    auto path = j.find("event_type");
    if (path == j.end())
      return caf::none;
    auto p = caf::get_if<std::string>(&path->second);
    if (!p)
      return caf::none;
    auto it = types.find(*p);
    if (it == types.end())
      return caf::none;
    auto type = it->second;
    if (j.size() > type.fields.size()) {
      extend(type, j);
    }
    return type;
  }

  // TODO ch5152: Needs implementation
  void extend(vast::record_type& t, const vast::json::object& j) {
  }

  suricata() {
    auto alert = record_type{{"timestamp",
                              timestamp_type{}.attributes({{"time"}})},
                             {"flow_id", count_type{}},
                             {"pcap_cnt", count_type{}},
                             {"src_ip", address_type{}},
                             {"src_port", port_type{}},
                             {"dest_ip", address_type{}},
                             {"dest_port", port_type{}},
                             {"proto", string_type{}},
                             {"alert.action", string_type{}},
                             {"alert.gid", count_type{}},
                             {"alert.signature_id", count_type{}},
                             {"alert.rev", count_type{}},
                             {"alert.signature", string_type{}},
                             {"alert.category", string_type{}},
                             {"alert.severity", count_type{}},
                             {"flow.pkts_toserver", count_type{}},
                             {"flow.pkts_toclient", count_type{}},
                             {"flow.bytes_toserver", count_type{}},
                             {"flow.bytes_toclient", count_type{}},
                             {"flow.start", timestamp_type{}},
                             {"payload", string_type{}},
                             {"payload_printable", string_type{}},
                             {"stream", count_type{}},
                             {"packet", string_type{}},
                             {"packet_info.linktype", count_type{}}};
    types["alert"] = alert;
  }

  caf::error schema(vast::schema) {
    // The vast types are automatically generated and cannot be changed.
    return make_error(vast::ec::unspecified, "schema cannot be changed");
  }

  vast::schema schema() const {
    vast::schema result;
    for (auto& [key, value] : types)
      result.add(value);
    return result;
  }

  static const char* name() {
    return "suricata-json-reader";
  }

  std::unordered_map<std::string, vast::record_type> types;
};

} // namespace vast::format::json
