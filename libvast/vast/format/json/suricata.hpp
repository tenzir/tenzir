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
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/view.hpp"

namespace vast::format::json {

struct suricata {
  caf::optional<vast::record_type> operator()(const vast::json::object& j) {
    auto i = j.find("event_type");
    if (i == j.end())
      return caf::none;
    auto event_type = caf::get_if<vast::json::string>(&i->second);
    if (!event_type) {
      VAST_WARNING(this, "got an event_type field with a non-string value");
      return caf::none;
    }
    auto it = types.find(*event_type);
    if (it == types.end()) {
      VAST_WARNING(this, "does not have a layout for event_type", *event_type);
      return caf::none;
    }
    auto type = it->second;
    return type;
  }

  suricata() {
    auto common = record_type{{"timestamp",
                               timestamp_type{}.attributes({{"time"}})},
                              {"flow_id", count_type{}},
                              {"pcap_cnt", count_type{}},
                              {"src_ip", address_type{}},
                              {"src_port", port_type{}},
                              {"dest_ip", address_type{}},
                              {"dest_port", port_type{}},
                              {"proto", string_type{}},
                              {"community_id", string_type{}}};
    auto app_proto = record_type{{"app_proto", string_type{}}};
    // https://suricata.readthedocs.io/en/suricata-4.1.3/output/eve/eve-json-format.html#event-type-alert
    auto alert_part = record_type{
      {"alert",
       concat(app_proto,
              record_type{// TODO: Switch to enumeration_type{{"allowed",
                          //       "drop", reject"}}?
                          {"action", string_type{}},
                          {"gid", count_type{}},
                          {"signature_id", count_type{}},
                          {"rev", count_type{}},
                          {"signature", string_type{}},
                          // Not documented?
                          {"category", string_type{}},
                          {"severity", count_type{}},
                          {"source.ip", address_type{}},
                          {"source.port", port_type{}},
                          {"target.ip", address_type{}},
                          {"target.port", port_type{}}})}};
    auto dhcp = record_type{
      {"dhcp", record_type{{"type", string_type{}},
                           {"id", count_type{}},
                           {"client_mac", string_type{}},
                           {"assigned_ip", address_type{}},
                           {"client_ip", address_type{}},
                           {"dhcp_type", string_type{}},
                           {"assigned_ip", address_type{}},
                           {"client_id", string_type{}},
                           {"hostname", string_type{}},
                           {"params", vector_type{string_type{}}}}}};
    auto dns = record_type{{"dns", record_type{{"type", string_type{}},
                                               {"id", count_type{}},
                                               {"flags", count_type{}},
                                               {"rrname", string_type{}},
                                               {"rrtype", string_type{}},
                                               {"rcode", string_type{}},
                                               {"rdata", string_type{}},
                                               {"ttl", count_type{}},
                                               {"tx_id", count_type{}}}}};
    // https://suricata.readthedocs.io/en/suricata-4.1.3/output/eve/eve-json-format.html#event-type-http
    // Corresponds to http extended logging.
    auto http = record_type{
      {"http", record_type{{"hostname", string_type{}},
                           {"url", string_type{}},
                           {"http_port", count_type{}},
                           {"http_user_agent", string_type{}},
                           {"http_content_type", string_type{}},
                           {"http_method", string_type{}},
                           {"http_refer", string_type{}},
                           {"protocol", string_type{}},
                           {"status", count_type{}},
                           {"redirect", string_type{}},
                           {"length", count_type{}}}}};
    auto fileinfo = record_type{
      {"fileinfo", record_type{{"filename", string_type{}},
                               {"magic", string_type{}},
                               {"gaps", boolean_type{}},
                               {"state", string_type{}},
                               {"md5", string_type{}},
                               {"sha1", string_type{}},
                               {"sha256", string_type{}},
                               {"stored", boolean_type{}},
                               {"file_id", count_type{}},
                               {"size", count_type{}},
                               {"tx_id", count_type{}}}}};
    auto flow = record_type{
      {"flow",
       record_type{{"pkts_toserver", count_type{}},
                   {"pkts_toclient", count_type{}},
                   {"bytes_toserver", count_type{}},
                   {"bytes_toclient", count_type{}},
                   // TODO: fix timestamp parser to accept "+02:00" suffixes
                   {"start", timestamp_type{}},
                   {"end", timestamp_type{}},
                   {"age", count_type{}},
                   {"state", string_type{}},
                   {"reason", string_type{}},
                   {"alerted", boolean_type{}}}}};
    auto netflow = record_type{
      {"netflow", record_type{{"pkts", count_type{}},
                              {"bytes", count_type{}},
                              {"start", timestamp_type{}},
                              {"end", timestamp_type{}},
                              {"age", count_type{}}}}};
    auto tls = record_type{
      {"tls", record_type{
                {"subject", string_type{}},
                {"issuerdn", string_type{}},
                {"serial", string_type{}},
                {"fingerprint", string_type{}},
                {"ja3", record_type{{"hash", string_type{}},
                                    {"string", string_type{}}}},
                {"ja3s", record_type{{"hash", string_type{}},
                                     {"string", string_type{}}}},
                {"notbefore", timestamp_type{}},
                {"notafter", timestamp_type{}},
              }}};
    auto alert = concat(common, alert_part, flow,
                        record_type{{"payload", string_type{}},
                                    {"payload_printable", string_type{}},
                                    {"stream", count_type{}},
                                    {"packet", string_type{}},
                                    {"packet_info.linktype", count_type{}}});
    types = {{"alert", flatten(alert).name("suricata.alert")},
             {"dhcp", flatten(concat(common, dhcp)).name("suricata.dhcp")},
             {"dns", flatten(concat(common, dns)).name("suricata.dns")},
             {"fileinfo", flatten(concat(common, fileinfo, http, app_proto))
                            .name("suricata.fileinfo")},
             {"http", flatten(concat(common, http,
                                     record_type{{"tx_id", count_type{}}}))
                        .name("suricata.http")},
             {"flow",
              flatten(concat(common, flow, app_proto)).name("suricata.flow")},
             {"netflow", flatten(concat(common, netflow, app_proto))
                           .name("suricata.netflow")},
             {"tls", flatten(concat(common, tls)).name("suricata.tls")}};
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
    return "suricata-reader";
  }

  std::unordered_map<std::string, record_type> types;
};

} // namespace vast::format::json
