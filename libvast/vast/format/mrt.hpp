#ifndef VAST_FORMAT_MRT_HPP
#define VAST_FORMAT_MRT_HPP

#include <iostream>

#include "vast/address.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/vast/address.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/subnet.hpp"
#include "vast/data.hpp"
#include "vast/detail/range.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"

namespace {
  static constexpr size_t mrt_header_length = 12;
} // namespace <anonymous>

namespace vast {
namespace format {
namespace mrt {

/// A parser for bgp4mp messages and table_dump_v2 entries from MRT
/// (Multi-Threaded Routing Toolkit Routing Information Export Format - RFC 6396
/// - https://tools.ietf.org/html/rfc6396) files.
struct mrt_parser {
  using mrt_data_iterator = std::vector<char>::iterator;

  type table_dump_v2_peer_type_;
  type table_dump_v2_rib_entry_type_;
  type bgp4mp_announce_type_;
  type bgp4mp_withdraw_type_;
  type bgp4mp_state_change_type_;
  type bgp4mp_open_type_;
  type bgp4mp_notification_type_;
  type bgp4mp_keepalive_type_;

  struct mrt_header {
    vast::timestamp timestamp;
    count type = 0;
    count subtype = 0;
    count length = 0;
  };

  struct bgp4mp_info {
    bool as4;
    bool afi_ipv4;
    count peer_as_nr = 0;
    address peer_ip_addr;
    count length = 0;
  };

  mrt_parser();
  bool parse(std::istream& input, std::vector<event> &event_queue);

private:
  // ### MRT ###
  // Parses the MRT header
  bool parse_mrt_header(mrt_data_iterator& f, mrt_data_iterator& l,
                        mrt_header& header);
  // ### MRT/TABLE_DUMP_V2 ###
  // Parses the MRT type TABLE_DUMP_V2
  bool parse_mrt_message_table_dump_v2(mrt_data_iterator& f,
                                       mrt_data_iterator& l,
                                       mrt_header& header,
                                       std::vector<event>& event_queue);
  // Parses the TABLE_DUMP_V2 subtype PEER_INDEX_TABLE
  bool parse_mrt_message_table_dump_v2_peer(mrt_data_iterator& f,
                                            mrt_data_iterator& l,
                                            mrt_header& header,
                                            std::vector<event>& event_queue);
  // Parses the TABLE_DUMP_V2 subtype RIB_IPV4_* and RIB_IPV6_*
  bool parse_mrt_message_table_dump_v2_rib(mrt_data_iterator& f,
                                           mrt_data_iterator& l,
                                           mrt_header& header,
                                           bool afi_ipv4,
                                           std::vector<event>& event_queue);
  // ### MRT/BGP4MP ###
  // Parses the MRT type BGP4MP_ET
  bool parse_mrt_message_bgp4mp_et(mrt_data_iterator& f, mrt_data_iterator& l,
                                   mrt_header& header,
                                   std::vector<event> &event_queue);
  // Parses the MRT type BGP4MP
  bool parse_mrt_message_bgp4mp(mrt_data_iterator& f, mrt_data_iterator& l,
                                mrt_header& header,
                                std::vector<event> &event_queue);
  // Parses the BGP4MP subtype BGP4MP_STATE_CHANGE and BGP4MP_STATE_CHANGE_AS4
  bool parse_mrt_message_bgp4mp_state_change(mrt_data_iterator& f,
                                             mrt_data_iterator& l, bool as4,
                                             mrt_header& header,
                                             std::vector<event> &event_queue);
  // Parses the BGP4MP subtype BGP4MP_MESSAGE and BGP4MP_MESSAGE_AS4
  bool parse_mrt_message_bgp4mp_message(mrt_data_iterator& f,
                                        mrt_data_iterator& l, bool as4,
                                        mrt_header& header,
                                        std::vector<event> &event_queue);
  // ### BGP4MP ###
  // Parses a BGP4MP message OPEN
  bool parse_bgp4mp_message_open(mrt_data_iterator& f, mrt_data_iterator& l,
                                 mrt_header& header, bgp4mp_info& info,
                                 std::vector<event>& event_queue);
  // Parses a BGP4MP message UPDATE
  bool parse_bgp4mp_message_update(mrt_data_iterator& f, mrt_data_iterator& l,
                                   mrt_header& header, bgp4mp_info& info,
                                   std::vector<event>& event_queue);
  // Parses a BGP4MP message NOTIFICATION
  bool parse_bgp4mp_message_notification(mrt_data_iterator& f,
                                         mrt_data_iterator& l,
                                         mrt_header& header,
                                         std::vector<event>& event_queue);
  // Parses a BGP4MP message KEEPALIVE
  bool parse_bgp4mp_message_keepalive(mrt_header& header,
                                      std::vector<event>& event_queue);
  // Parses the BGP4MP path attribute ORIGIN
  bool parse_bgp4mp_path_attribute_origin(mrt_data_iterator& f,
                                          mrt_data_iterator& l,
                                          std::string& origin);
  // Parses the BGP4MP path attribute AS_PATH
  bool parse_bgp4mp_path_attribute_as_path(mrt_data_iterator& f,
                                           mrt_data_iterator& l, bool as4,
                                           std::vector<vast::data>& as_path,
                                           count& origin_as);
  // Parses the BGP4MP path attribute AGGREGATOR
  bool parse_bgp4mp_path_attribute_aggregator(mrt_data_iterator& f,
                                              mrt_data_iterator& l, bool as4,
                                              count& aggregator_as,
                                              address& aggregator_ip);
  // Parses the BGP4MP path attribute COMMUNITIES
  bool parse_bgp4mp_path_attribute_communities(
    mrt_data_iterator& f, mrt_data_iterator& l, count attr_length,
    std::vector<vast::data>& communities);
  // Parses the BGP4MP path attribute EXTENDED_COMMUNITIES
  bool parse_bgp4mp_path_attribute_extended_communities(
    mrt_data_iterator& f, mrt_data_iterator& l, count attr_length,
    std::vector<vast::data>& communities);
  // Parse prefix in the BGP4MP prefix encoding
  bool parse_bgp4mp_prefix(mrt_data_iterator& f, mrt_data_iterator& l,
                           bool afi_ipv4, count length,
                           std::vector<subnet>& prefix);
};

/// A MRT reader.
class reader {
public:
  reader() = default;

  /// Constructs a MRT reader.
  explicit reader(std::unique_ptr<std::istream> input);

  expected<event> read();

  expected<void> schema(vast::schema const& sch);

  expected<vast::schema> schema() const;

  const char* name() const;

private:
  mrt_parser parser_;
  std::unique_ptr<std::istream> input_;
  std::vector<event> event_queue_;
};

} // namespace mrt
} // namespace format
} // namespace vast

#endif
