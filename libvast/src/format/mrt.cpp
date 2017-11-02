#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include "vast/format/mrt.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"

namespace vast {
namespace format {
namespace mrt {

namespace table_dump_v2 {

const auto peer_entries_type = record_type{{
  {"type", count_type{}},
  {"bgp_id", count_type{}},
  {"ip_address", address_type{}},
  {"as", count_type{}},
}};

const auto peer_index_table_type = record_type{{
  {"collector_bgp_id", count_type{}},
  {"view_name", string_type{}},
  {"ip_address", address_type{}},
  {"peer_entries", vector_type{peer_entries_type}}
}};

} // namespace table_dump_v2


namespace bgp4mp {

// TODO: Define this type.
const auto message_as4_type = type{};

} // namespace bgp4mp

namespace {

struct factory {
  value operator()(table_dump_v2::peer_index_table& /* x */) const {
    // TODO: Implement this function.
    return {nil, table_dump_v2::peer_index_table_type};
  }

  value operator()(bgp4mp::message_as4& /* x */) const {
    // TODO: Implement this function.
    return {nil, bgp4mp::message_as4_type};
  }
};

} // namespace anonymous

reader::reader(std::unique_ptr<std::istream> input) : input_{std::move(input)} {
}

expected<event> reader::read() {
  VAST_ASSERT(input_);
  // We have to read the input block-wise in a manner that respects the
  // protocol framing.
  static constexpr size_t common_header_length = 12;
  if (buffer_.size() < common_header_length)
    buffer_.resize(common_header_length);
  input_->read(buffer_.data(), common_header_length);
  if (input_->eof())
    return make_error(ec::end_of_input, "reached end of input");
  if (input_->fail())
    return make_error(ec::format_error, "failed to read MRT common header");
  auto ptr = reinterpret_cast<const uint32_t*>(buffer_.data() + 8);
  auto message_length = vast::detail::to_host_order(*ptr);
  // TODO: Where does the RFC specify the maximum length?
  static constexpr size_t max_message_length = 1 << 20;
  if (message_length > max_message_length)
    return make_error(ec::format_error, "MRT message exceeds maximum length",
                      message_length, max_message_length);
  buffer_.resize(common_header_length + message_length);
  if (!input_->read(buffer_.data() + common_header_length, message_length))
    return make_error(ec::format_error, "failed to read MRT message");
  mrt::record r;
  if (!parser_(buffer_, r))
    return make_error(ec::format_error, "failed to parse MRT message");
  // Convert
  auto e = event{visit(factory{}, r.message)};
  // Take the timestamp from the Common Header as event time.
  std::chrono::duration<uint32_t> since_epoch{r.header.timestamp};
  auto ts = timestamp{std::chrono::duration_cast<timespan>(since_epoch)};
  e.timestamp(ts);
  return e;
}

expected<void> reader::schema(vast::schema const&) {
  // TODO
  return make_error(ec::unspecified, "not yet implemented");
}

expected<vast::schema> reader::schema() const {
  // TODO
  return make_error(ec::unspecified, "not yet implemented");
}

const char* reader::name() const {
  return "mrt-reader";
}

} // namespace mrt

//namespace mrt {
//
//mrt_parser::mrt_parser() {
//  // RIB type
//  auto rib_entry_fields = std::vector<record_field>{
//    {"timestamp", timestamp_type{}},
//    {"peer_index", count_type{}},
//    {"prefix", subnet_type{}},
//    {"as_path", vector_type{count_type{}}},
//    {"origin_as", count_type{}},
//    {"origin", string_type{}.attributes({{"skip"}})},
//    {"nexthop", address_type{}},
//    {"local_pref", count_type{}},
//    {"med", count_type{}},
//    {"community", vector_type{count_type{}}},
//    {"atomic_aggregate", boolean_type{}},
//    {"aggregator_as", count_type{}},
//    {"aggregator_ip", address_type{}},
//  };
//  table_dump_v2_rib_entry_type_ = record_type{std::move(rib_entry_fields)};
//  table_dump_v2_rib_entry_type_.name("mrt::table_dump_v2::rib_entry");
//  // Announce type.
//  auto announce_fields = std::vector<record_field>{
//    {"timestamp", timestamp_type{}},
//    {"source_ip", address_type{}},
//    {"source_as", count_type{}},
//    {"prefix", subnet_type{}},
//    {"as_path", vector_type{count_type{}}},
//    {"origin_as", count_type{}},
//    {"origin", string_type{}.attributes({{"skip"}})},
//    {"nexthop", address_type{}},
//    {"local_pref", count_type{}},
//    {"med", count_type{}},
//    {"community", vector_type{count_type{}}},
//    {"atomic_aggregate", boolean_type{}},
//    {"aggregator_as", count_type{}},
//    {"aggregator_ip", address_type{}},
//  };
//  bgp4mp_announce_type_ = record_type{std::move(announce_fields)};
//  bgp4mp_announce_type_.name("mrt::bgp4mp::announcement");
//  // Withdraw type
//  auto withdraw_fields = std::vector<record_field>{
//    {"timestamp", timestamp_type{}},
//    {"source_ip", address_type{}},
//    {"source_as", count_type{}},
//    {"prefix", subnet_type{}},
//  };
//  bgp4mp_withdraw_type_ = record_type{std::move(withdraw_fields)};
//  bgp4mp_withdraw_type_.name("mrt::bgp4mp::withdrawn");
//  // State-change type.
//  auto state_change_fields = std::vector<record_field>{
//    {"timestamp", timestamp_type{}},
//    {"source_ip", address_type{}},
//    {"source_as", count_type{}},
//    {"old_state", count_type{}},
//    {"new_state", count_type{}},
//  };
//  bgp4mp_state_change_type_ = record_type{std::move(state_change_fields)};
//  bgp4mp_state_change_type_.name("mrt::bgp4mp::state_change");
//  // Open type.
//  auto open_fields = std::vector<record_field>{
//    {"timestamp", timestamp_type{}},
//    {"version", count_type{}},
//    {"my_autonomous_system", count_type{}},
//    {"hold_time", count_type{}},
//    {"bgp_identifier", count_type{}},
//  };
//  bgp4mp_open_type_ = record_type{std::move(open_fields)};
//  bgp4mp_open_type_.name("mrt::bgp4mp::open");
//  // Notification type.
//  auto notification_fields = std::vector<record_field>{
//    {"timestamp", timestamp_type{}},
//    {"error_code", count_type{}},
//    {"error_subcode", count_type{}},
//  };
//  bgp4mp_notification_type_ = record_type{std::move(notification_fields)};
//  bgp4mp_notification_type_.name("mrt::bgp4mp::notification");
//  // Keepalive type.
//  auto keepalive_fields = std::vector<record_field>{
//      {"timestamp", timestamp_type{}},
//  };
//  bgp4mp_keepalive_type_ = record_type{std::move(keepalive_fields)};
//  bgp4mp_keepalive_type_.name("mrt::bgp4mp::keepalive");
//}
//
//// ### MRT ###
//bool mrt_parser::parse(std::istream& input, std::vector<event>& event_queue) {
//  mrt_header header;
//  std::vector<char> raw(mrt_header_length);
//  input.read(raw.data(), mrt_header_length);
//  if (!input) {
//    if(input.eof())
//      return true;
//    VAST_ERROR("mrt-parser", "could read just", input.gcount(), "of",
//               mrt_header_length, "bytes from stream");
//    return false;
//  }
//  auto f = raw.begin();
//  auto l = raw.end();
//  if (!parse_mrt_header(f, l, header))
//    return false;
//  raw.resize(header.length);
//  input.read(raw.data(), header.length);
//  if (!input) {
//    VAST_ERROR("mrt-parser", "could read just", input.gcount(), "of",
//               mrt_header_length, "bytes from stream");
//    return false;
//  }
//  f = raw.begin();
//  l = raw.end();
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.  MRT Types
//    11   OSPFv2
//    12   TABLE_DUMP
//    13   TABLE_DUMP_V2
//    16   BGP4MP
//    17   BGP4MP_ET
//    32   ISIS
//    33   ISIS_ET
//    48   OSPFv3
//    49   OSPFv3_ET
//  */
//  switch (header.type) {
//    case 13:
//      return parse_mrt_message_table_dump_v2(f, l, header, event_queue);
//    case 16:
//      return parse_mrt_message_bgp4mp(f, l, header, event_queue);
//    case 17:
//      return parse_mrt_message_bgp4mp_et(f, l, header, event_queue);
//    default:
//      VAST_WARNING("mrt-parser", "ignores unsupported MRT type", header.type);
//      return false;
//  }
//}
//
//bool mrt_parser::parse_mrt_header(mrt_data_iterator& f, mrt_data_iterator& l,
//                                  mrt_header& header) {
//  using namespace std::chrono;
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  auto stime32 = b32be->*[](uint32_t x) { return vast::timestamp{seconds(x)}; };
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  2.  MRT Common Header
//     0                   1                   2                   3
//     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                           Timestamp                           |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |             Type              |            Subtype            |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                             Length                            |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                      Message... (variable)
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  auto mrt_header_parser = stime32 >> count16 >> count16 >> count32;
//  if (!mrt_header_parser(f, l, header.timestamp, header.type, header.subtype,
//                         header.length))
//    return false;
//  VAST_TRACE("mrt-parser parses header:", "timestamp", header.timestamp, "type",
//             header.type, "subtype", header.subtype, "length",
//             header.length);
//  return true;
//}
//
//// ### MRT/TABLE_DUMP_V2 ###
//bool mrt_parser::parse_mrt_message_table_dump_v2(
//  mrt_data_iterator& f, mrt_data_iterator& l, mrt_header& header,
//  std::vector<event>& event_queue) {
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.3.  TABLE_DUMP_V2 Type
//  Subtypes:
//    1    PEER_INDEX_TABLE
//    2    RIB_IPV4_UNICAST
//    3    RIB_IPV4_MULTICAST
//    4    RIB_IPV6_UNICAST
//    5    RIB_IPV6_MULTICAST
//    6    RIB_GENERIC
//  */
//  switch (header.subtype) {
//    case 1:
//      return parse_mrt_message_table_dump_v2_peer(f, l, header, event_queue);
//    case 2:
//    case 3:
//      return parse_mrt_message_table_dump_v2_rib(f, l, header, true,
//                                                 event_queue);
//    case 4:
//    case 5:
//      return parse_mrt_message_table_dump_v2_rib(f, l, header, false,
//                                                 event_queue);
//    default:
//      VAST_WARNING("mrt-parser",
//                   "ignores unsupported MRT TABLE_DUMP_V2 subtype",
//                   header.subtype);
//      return false;
//  }
//}
//
//bool mrt_parser::parse_mrt_message_table_dump_v2_peer(
//  mrt_data_iterator& f, mrt_data_iterator& l, mrt_header& header,
//  std::vector<event>& event_queue) {
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  auto ipv4 = b32be->*[](uint32_t x) {
//    return address{&x, address::ipv4, address::host};
//  };
//  auto ipv6 = bytes<16>->*[](std::array<uint8_t, 16> x) {
//    return address{x.data(), address::ipv6, address::network};
//  };
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.3.1.  PEER_INDEX_TABLE Subtype
//     0                   1                   2                   3
//     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                      Collector BGP ID                         |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |       View Name Length        |     View Name (variable)      |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |          Peer Count           |    Peer Entries (variable)
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  count collector_bgp_id = 0;
//  count view_name_length = 0;
//  count peer_count = 0;
//  auto peer_index_table_parser = count32 >> count16;
//  if (!peer_index_table_parser(f, l, collector_bgp_id, view_name_length))
//    return false;
//  VAST_TRACE("mrt-parser parses table-dump-v2-peer:", "collector_bgp_id",
//             collector_bgp_id, "view_name_length", view_name_length);
//  if (!count16(f, l, peer_count))
//    return false;
//  VAST_TRACE("mrt-parser parses table-dump-v2-peer:", "peer_count", peer_count);
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.3.1.  PEER_INDEX_TABLE Subtype
//  Peer Entry
//    [...] The PEER_INDEX_TABLE record contains Peer Count number of Peer
//    Entries.
//     0                   1                   2                   3
//     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |   Peer Type   |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                         Peer BGP ID                           |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                   Peer IP Address (variable)                  |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                        Peer AS (variable)                     |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  for (count i = 0u; i < peer_count; i++) {
//    uint8_t peer_type;
//    count peer_bgp_id = 0;
//    address peer_ip_address;
//    count peer_as = 0;
//    auto peer_entry_parser = byte >> count32;
//    if (!peer_entry_parser(f, l, peer_type, peer_bgp_id))
//      return false;
//    /*
//    RFC 6396 https://tools.ietf.org/html/rfc6396
//    4.3.1.  PEER_INDEX_TABLE Subtype
//    Peer Type Field
//       0 1 2 3 4 5 6 7
//      +-+-+-+-+-+-+-+-+
//      | | | | | | |A|I|
//      +-+-+-+-+-+-+-+-+
//      Bit 6: Peer AS number size:  0 = 16 bits, 1 = 32 bits
//      Bit 7: Peer IP Address family:  0 = IPv4,  1 = IPv6
//    */
//    bool is_as4 = ((peer_type & 2) >> 1) == 1;
//    bool is_ipv6 = ((peer_type & 1)) == 1;
//    if (is_ipv6) {
//      if (!ipv6(f, l, peer_ip_address))
//        return false;
//    } else {
//      if (!ipv4(f, l, peer_ip_address))
//        return false;
//    }
//    if (is_as4) {
//      if (!count32(f, l, peer_as))
//        return false;
//    } else {
//      if (!count16(f, l, peer_as))
//        return false;
//    }
//    VAST_TRACE("mrt-parser parses table-dump-v2-peer:", "peer_bgp_id",
//               peer_bgp_id, "peer_ip_address", peer_ip_address, "peer_as",
//               peer_as);
//    /*
//    RFC 6396 https://tools.ietf.org/html/rfc6396
//    4.3.1.  PEER_INDEX_TABLE Subtype
//      [...] The position of the peer in the PEER_INDEX_TABLE is used as an index
//      in the subsequent TABLE_DUMP_V2 MRT records. The index number begins with
//      0.
//    */
//    event e{{
//      vector{header.timestamp,
//             i,
//             peer_bgp_id,
//             peer_ip_address,
//             peer_as},
//      table_dump_v2_peer_type_
//    }};
//    e.timestamp(header.timestamp);
//    event_queue.push_back(e);
//  }
//  return true;
//}
//
//bool mrt_parser::parse_mrt_message_table_dump_v2_rib(
//  mrt_data_iterator& f, mrt_data_iterator& l, mrt_header& header, bool afi_ipv4,
//  std::vector<event>& event_queue) {
//  using namespace std::chrono;
//  auto count8 = byte->*[](uint8_t x) { return count(x); };
//  auto count16 = b16be->*[](uint16_t x) { return count(x); };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  auto stime32 = b32be->*[](uint32_t x) { return vast::timestamp{seconds(x)}; };
//  auto ipv4 = b32be->*[](uint32_t x) {
//    return address{&x, address::ipv4, address::host};
//  };
//  auto ipv6 = bytes<16>->*[](std::array<uint8_t, 16> x) {
//    return address{x.data(), address::ipv6, address::network};
//  };
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.3.2.  AFI/SAFI-Specific RIB Subtypes
//    [...] The Prefix Length and Prefix fields are encoded in the same manner as
//    the BGP NLRI encoding for IPv4 and IPv6 prefixes. [...]
//     0                   1                   2                   3
//     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                         Sequence Number                       |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    | Prefix Length |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                        Prefix (variable)                      |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |         Entry Count           |  RIB Entries (variable)
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  count sequence_nr = 0;
//  std::vector<subnet> prefix;
//  count entry_count = 0;
//  if (!count32(f, l, sequence_nr))
//    return false;
//  VAST_TRACE("mrt-parser parses table-dump-v2-rib:", "sequence_nr",
//             sequence_nr);
//  if (!parse_bgp4mp_prefix(f, l, afi_ipv4, 1, prefix))
//     return false;
//  VAST_TRACE("mrt-parser parses table-dump-v2-rib:", "prefix", prefix[0]);
//  if (!count16(f, l, entry_count))
//    return false;
//  VAST_TRACE("mrt-parser parses table-dump-v2-rib:", "entry_count",
//             entry_count);
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.3.4.  RIB Entries
//    The RIB Entries are repeated Entry Count times. [...] All AS numbers in
//    the AS_PATH attribute MUST be encoded as 4-byte AS numbers.
//     0                   1                   2                   3
//     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |         Peer Index            |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                         Originated Time                       |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |      Attribute Length         |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                    BGP Attributes... (variable)
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  for (auto i = 0u; i < entry_count; i++) {
//    count peer_index = 0;
//    vast::timestamp originated_time;
//    count attributes_length = 0;
//    auto rib_entry_parser = count16 >> stime32 >> count16;
//    if (!rib_entry_parser(f, l, peer_index, originated_time, attributes_length))
//      return false;
//    VAST_TRACE("mrt-parser parses table-dump-v2-rib:", "peer_index", peer_index,
//              "originated_time", originated_time, "attributes_length",
//              attributes_length);
//    std::string origin;
//    std::vector<vast::data> as_path;
//    count origin_as = 0;
//    address next_hop;
//    count multi_exit_disc = 0;
//    count local_pref = 0;
//    bool atomic_aggregate = false;
//    count aggregator_as = 0;
//    address aggregator_ip;
//    std::vector<vast::data> communities;
//    int64_t slength = attributes_length;
//    while (slength > 0) {
//      uint8_t attr_flags;
//      uint8_t attr_type_code;
//      count attr_length;
//      auto bgp4mp_attribute_type_parser = byte >> byte;
//      if (!bgp4mp_attribute_type_parser(f, l, attr_flags, attr_type_code))
//        return false;
//      /*
//      RFC 4271 https://tools.ietf.org/html/rfc4271
//      4.3.  UPDATE Message Format
//      Path Attributes
//        The fourth high-order bit (bit 3) of the Attribute Flags octet is the
//        Extended Length bit. It defines whether the Attribute Length is one octet
//        (if set to 0) or two octets (if set to 1).
//      */
//      bool is_extended_length = ((attr_flags & 16) >> 4) == 1;
//      if (is_extended_length) {
//        if (!count16(f, l, attr_length))
//          return false;
//      } else {
//        if (!count8(f, l, attr_length))
//          return false;
//      }
//      VAST_TRACE("mrt-parser parses path-attribute:", "attr_type_code",
//                 static_cast<uint16_t>(attr_type_code), "attr_length",
//                 attr_length);
//      /*
//      Path Attribute Type Codes:
//      RFC 4271 https://tools.ietf.org/html/rfc4271
//         1 - ORIGIN
//         2 - AS_PATH
//         3 - NEXT_HOP
//         4 - MULTI_EXIT_DISC
//         5 - LOCAL_PREF
//         6 - ATOMIC_AGGREGATE
//         7 - AGGREGATOR
//      RFC 1997 https://tools.ietf.org/html/rfc1997
//         8 - COMMUNITIES
//      RFC 4760 https://tools.ietf.org/html/rfc4760
//        14 - MP_REACH_NLRI
//      RFC 4360 https://tools.ietf.org/html/rfc4360
//        16 - Extended Communities
//      RFC 6793 https://tools.ietf.org/html/rfc6793
//        17 - AS4_PATH
//        18 - AS4_AGGREGATOR
//      */
//      auto t = f;
//      switch (attr_type_code) {
//        case 1:
//          if (!parse_bgp4mp_path_attribute_origin(t, l, origin))
//            return false;
//          break;
//        case 2:
//          if(!parse_bgp4mp_path_attribute_as_path(t, l, true, as_path,
//                                                  origin_as))
//            return false;
//          break;
//        case 3:
//          if (!ipv4(t, l, next_hop))
//            return false;
//          VAST_TRACE("mrt-parser parses path-attribute:", "next_hop", next_hop);
//          break;
//        case 4:
//          if (!count32(t, l, multi_exit_disc))
//            return false;
//          VAST_TRACE("mrt-parser parses path-attribute:", "multi_exit_disc",
//                     multi_exit_disc);
//          break;
//        case 5:
//          if (!count32(t, l, local_pref))
//            return false;
//          VAST_TRACE("mrt-parser parses path-attribute:", "local_pref",
//                     local_pref);
//          break;
//        case 6:
//          atomic_aggregate = true;
//          VAST_TRACE("mrt-parser parses path-attribute:", "atomic_aggregate",
//                     atomic_aggregate);
//          break;
//        case 7:
//          if (!parse_bgp4mp_path_attribute_aggregator(t, l, true,
//                                                      aggregator_as,
//                                                      aggregator_ip))
//            return false;
//          break;
//        case 8:
//          if (!parse_bgp4mp_path_attribute_communities(t, l, attr_length,
//                                                       communities))
//            return false;
//          break;
//        case 14:
//          {
//            /*
//            RFC 6396 https://tools.ietf.org/html/rfc6396
//            4.3.4.  RIB Entries
//              There is one exception to the encoding of BGP attributes for the
//              BGP MP_REACH_NLRI attribute (BGP Type Code 14) [RFC4760]. Since
//              the AFI, SAFI, and NLRI information is already encoded in the RIB
//              Entry Header or RIB_GENERIC Entry Header, only the Next Hop
//              Address Length and Next Hop Address fields are included. [...]
//            */
//            count next_hop_network_address_length = 0;
//            if (!count8(t, l, next_hop_network_address_length))
//              return false;
//            VAST_TRACE("mrt-parser parses path-attribute:",
//                       "next_hop_network_address_length",
//                       next_hop_network_address_length);
//            if (afi_ipv4) {
//              if (!ipv4(t, l, next_hop))
//                return false;
//            } else {
//              if (!ipv6(t, l, next_hop))
//                return false;
//            }
//            VAST_TRACE("mrt-parser parses path-attribute:", "next_hop",
//                       next_hop);
//          }
//          break;
//        case 16:
//          if (! parse_bgp4mp_path_attribute_extended_communities(t, l,
//                                                                 attr_length,
//                                                                 communities))
//            return false;
//          break;
//        case 17:
//          // Not relevant when encapsulated in mrt.
//          break;
//        case 18:
//          // Not relevant when encapsulated in mrt.
//          break;
//        default:
//          VAST_WARNING("mrt-parser",
//                       "ignores unsupported BGP4MP path attribute type",
//                       static_cast<uint16_t>(attr_type_code));
//      }
//      f += attr_length;
//      if (is_extended_length)
//        slength -= attr_length + 4;
//      else
//        slength -= attr_length + 3;
//    }
//    event e{{
//      vector{header.timestamp,
//             peer_index,
//             prefix[0],
//             as_path,
//             origin_as,
//             origin,
//             next_hop,
//             local_pref,
//             multi_exit_disc,
//             communities,
//             atomic_aggregate,
//             aggregator_as,
//             aggregator_ip},
//      table_dump_v2_rib_entry_type_
//    }};
//    e.timestamp(header.timestamp);
//    event_queue.push_back(e);
//  }
//  return true;
//}
//
//// ### MRT/BGP4MP ###
//bool mrt_parser::parse_mrt_message_bgp4mp_et(mrt_data_iterator& f,
//                                             mrt_data_iterator& l,
//                                             mrt_header& header,
//                                             std::vector<event>& event_queue) {
//  using namespace std::chrono;
//  auto ustime32 = b32be->*[](uint32_t x) {
//    return vast::timespan{microseconds(x)};
//  };
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  3.  Extended Timestamp MRT Header
//    [...] This field, Microsecond Timestamp, contains an unsigned 32BIT offset
//    value in microseconds, which is added to the Timestamp field value. [...]
//    The Microsecond Timestamp immediately follows the Length field in the MRT
//    Common Header and precedes all other fields in the message. The Microsecond
//    Timestamp is included in the computation of the Length field value. [...]
//  */
//  vast::timespan timestamp_et;
//  if (!ustime32(f, l, timestamp_et))
//    return false;
//  header.timestamp += timestamp_et;
//  VAST_TRACE("mrt-parser parses bgp4mp-message-et:", "timestamp",
//             header.timestamp);
//  return parse_mrt_message_bgp4mp(f, l, header, event_queue);
//}
//
//bool mrt_parser::parse_mrt_message_bgp4mp(mrt_data_iterator& f,
//                                          mrt_data_iterator& l,
//                                          mrt_header& header,
//                                          std::vector<event>& event_queue) {
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.4.  BGP4MP Type
//  Subtypes:
//    0    BGP4MP_STATE_CHANGE
//    1    BGP4MP_MESSAGE
//    4    BGP4MP_MESSAGE_AS4
//    5    BGP4MP_STATE_CHANGE_AS4
//    6    BGP4MP_MESSAGE_LOCAL
//    7    BGP4MP_MESSAGE_AS4_LOCAL
//  */
//  switch (header.subtype) {
//    case 0:
//      return parse_mrt_message_bgp4mp_state_change(f, l, false, header,
//                                                   event_queue);
//    case 1:
//      return parse_mrt_message_bgp4mp_message(f, l, false, header,
//                                              event_queue);
//    case 4:
//      return parse_mrt_message_bgp4mp_message(f, l, true, header, event_queue);
//    case 5:
//      return parse_mrt_message_bgp4mp_state_change(f, l, true, header,
//                                                   event_queue);
//    default:
//      VAST_WARNING("mrt-parser", "ignores unsupported MRT BGP4MP subtype",
//                   header.subtype);
//      return false;
//  }
//}
//
//bool mrt_parser::parse_mrt_message_bgp4mp_state_change(
//  mrt_data_iterator& f, mrt_data_iterator& l, bool as4, mrt_header& header,
//  std::vector<event> &event_queue) {
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  auto ipv4 = b32be->*[](uint32_t x) {
//    return address{&x, address::ipv4, address::host};
//  };
//  auto ipv6 = bytes<16>->*[](std::array<uint8_t, 16> x) {
//    return address{x.data(), address::ipv6, address::network};
//  };
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.4.1.  BGP4MP_STATE_CHANGE Subtype
//     0                   1                   2                   3
//     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |         Peer AS Number        |        Local AS Number        |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |        Interface Index        |        Address Family         |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                      Peer IP Address (variable)               |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                      Local IP Address (variable)              |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |            Old State          |          New State            |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  count peer_as_nr = 0;
//  count local_as_nr = 0;
//  count interface_index = 0;
//  count addr_family = 0;
//  address peer_ip_addr;
//  address local_ip_addr;
//  count old_state = 0;
//  count new_state = 0;
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.4.4.  BGP4MP_STATE_CHANGE_AS4 Subtype
//    This subtype updates the BGP4MP_STATE_CHANGE Subtype to support
//    4-byte AS numbers.
//  */
//  if (as4) {
//    auto bgp4mp_state_change_parser = count32 >> count32 >> count16 >> count16;
//    if(!bgp4mp_state_change_parser(f, l, peer_as_nr, local_as_nr,
//                                   interface_index, addr_family))
//      return false;
//  } else {
//    auto bgp4mp_state_change_parser = count16 >> count16 >> count16 >> count16;
//    if(!bgp4mp_state_change_parser(f, l, peer_as_nr, local_as_nr,
//                                   interface_index, addr_family))
//      return false;
//  }
//  VAST_TRACE("mrt-parser parses bgp4mp-state-change:", "peer_as_nr", peer_as_nr,
//             "local_as_nr", local_as_nr, "interface_index", interface_index,
//             "addr_family", addr_family);
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.4.1.  BGP4MP_STATE_CHANGE Subtype
//  Address Family Types:
//    1    AFI_IPv4
//    2    AFI_IPv6
//  */
//  if (addr_family == 1) {
//    auto bgp4mp_state_change_parser = ipv4 >> ipv4 >> count16 >> count16;
//    if (!bgp4mp_state_change_parser(f, l, peer_ip_addr, local_ip_addr,
//                                    old_state, new_state))
//      return false;
//  } else if (addr_family == 2) {
//    auto bgp4mp_state_change_parser = ipv6 >> ipv6 >> count16 >> count16;
//    if (!bgp4mp_state_change_parser(f, l, peer_ip_addr, local_ip_addr,
//                                    old_state, new_state))
//      return false;
//  } else {
//    return false;
//  }
//  VAST_TRACE("mrt-parser parses bgp4mp-state-change:", "peer_ip_addr",
//             peer_ip_addr, "local_ip_addr", local_ip_addr, "old_state",
//             old_state, "new_state", new_state);
//  event e{{
//    vector{header.timestamp,
//           peer_ip_addr,
//           peer_as_nr,
//           old_state,
//           new_state},
//    bgp4mp_state_change_type_
//  }};
//  e.timestamp(header.timestamp);
//  event_queue.push_back(e);
//  return true;
//}
//
//bool mrt_parser::parse_mrt_message_bgp4mp_message(
//  mrt_data_iterator& f, mrt_data_iterator& l, bool as4, mrt_header& header,
//  std::vector<event> &event_queue) {
//  auto count8 = byte->*[](uint8_t x) { return count{x}; };
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  auto ipv4 = b32be->*[](uint32_t x) {
//    return address{&x, address::ipv4, address::host};
//  };
//  auto ipv6 = bytes<16>->*[](std::array<uint8_t, 16> x) {
//    return address{x.data(), address::ipv6, address::network};
//  };
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.4.2.  BGP4MP_MESSAGE Subtype
//     0                   1                   2                   3
//     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |         Peer AS Number        |        Local AS Number        |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |        Interface Index        |        Address Family         |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                      Peer IP Address (variable)               |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                      Local IP Address (variable)              |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                    BGP Message... (variable)
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  count peer_as_nr = 0;
//  count local_as_nr = 0;
//  count interface_index = 0;
//  count addr_family = 0;
//  address peer_ip_addr;
//  address local_ip_addr;
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.4.3.  BGP4MP_MESSAGE_AS4 Subtype
//    This subtype updates the BGP4MP_MESSAGE Subtype to support 4-byte AS
//    numbers.
//  */
//  if (as4) {
//    auto bgp4mp_message_parser = count32 >> count32 >> count16 >> count16;
//    if(!bgp4mp_message_parser(f, l, peer_as_nr, local_as_nr, interface_index,
//                              addr_family))
//      return false;
//  } else {
//    auto bgp4mp_message_parser = count16 >> count16 >> count16 >> count16;
//    if(!bgp4mp_message_parser(f, l, peer_as_nr, local_as_nr, interface_index,
//                              addr_family))
//      return false;
//  }
//  VAST_TRACE("mrt-parser parses bgp4mp-message:", "peer_as_nr", peer_as_nr,
//             "local_as_nr", local_as_nr, "interface_index", interface_index,
//             "addr_family", addr_family);
//  /*
//  RFC 6396 https://tools.ietf.org/html/rfc6396
//  4.4.2.  BGP4MP_MESSAGE Subtype
//  Address Family Types:
//    1    AFI_IPv4
//    2    AFI_IPv6
//  */
//  if (addr_family == 1) {
//    auto bgp4mp_message_parser = ipv4 >> ipv4;
//    if (!bgp4mp_message_parser(f, l, peer_ip_addr, local_ip_addr))
//      return false;
//  } else if (addr_family == 2) {
//    auto bgp4mp_message_parser = ipv6 >> ipv6;
//    if (!bgp4mp_message_parser(f, l, peer_ip_addr, local_ip_addr))
//      return false;
//  } else {
//    return false;
//  }
//  VAST_TRACE("mrt-parser parses bgp4mp-message:", "peer_ip_addr", peer_ip_addr,
//             "local_ip_addr", local_ip_addr);
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.1.  Message Header Format
//    0                   1                   2                   3
//    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                                                               |
//    +                                                               +
//    |                                                               |
//    +                                                               +
//    |                           Marker                              |
//    +                                                               +
//    |                                                               |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |          Length               |      Type     |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  f += 16; // Marker
//  count length = 0;
//  count type = 0;
//  auto bgp4mp_message_parser = count16 >> count8;
//  if (!bgp4mp_message_parser(f, l, length, type))
//    return false;
//  VAST_TRACE("mrt-parser parses bgp4mp-message:", "length", length, "type",
//             type);
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.1.  Message Header Format
//  Types:
//    1 - OPEN
//    2 - UPDATE
//    3 - NOTIFICATION
//    4 - KEEPALIVE
//  */
//  bgp4mp_info info;
//  info.as4 = as4;
//  info.afi_ipv4 = (addr_family == 1);
//  info.peer_as_nr = peer_as_nr;
//  info.peer_ip_addr = peer_ip_addr;
//  info.length = length;
//  switch (type) {
//    case 1:
//      return parse_bgp4mp_message_open(f, l, header, info, event_queue);
//    case 2:
//      return parse_bgp4mp_message_update(f, l, header, info, event_queue);
//    case 3:
//      return parse_bgp4mp_message_notification(f, l, header, event_queue);
//    case 4:
//      return parse_bgp4mp_message_keepalive(header, event_queue);
//    default:
//      VAST_WARNING("mrt-parser", "ignores unsupported BGP4MP message type",
//                   type);
//      return false;
//  }
//}
//
//// ### BGP4MP ###
//bool mrt_parser::parse_bgp4mp_message_open(mrt_data_iterator& f,
//                                           mrt_data_iterator& l,
//                                           mrt_header& header,
//                                           bgp4mp_info& info,
//                                           std::vector<event> &event_queue) {
//  auto count8 = byte->*[](uint8_t x) { return count{x}; };
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.2.  OPEN Message Format
//    0                   1                   2                   3
//    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+
//    |    Version    |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |     My Autonomous System      |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |           Hold Time           |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                         BGP Identifier                        |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    | Opt Parm Len  |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |                                                               |
//    |             Optional Parameters (variable)                    |
//    |                                                               |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  count version = 0;
//  count my_autonomous_system = 0;
//  count hold_time = 0;
//  count bgp_identifier = 0;
//  count opt_parm_len = 0;
//  if (info.as4) {
//    auto bgp4mp_messasge_open_parser = count8 >> count32 >> count16 >>
//                                       count32 >> count8;
//    if (!bgp4mp_messasge_open_parser(f, l, version, my_autonomous_system,
//                                     hold_time, bgp_identifier, opt_parm_len))
//      return false;
//  } else {
//    auto bgp4mp_messasge_open_parser = count8 >> count16 >> count16 >>
//                                       count32 >> count8;
//    if (!bgp4mp_messasge_open_parser(f, l, version, my_autonomous_system,
//                                     hold_time, bgp_identifier, opt_parm_len))
//      return false;
//  }
//  VAST_TRACE("mrt-parser parses bgp4mp-message-open:", "version", version,
//             "my_autonomous_system", my_autonomous_system, "hold_time",
//             hold_time, "bgp_identifier", bgp_identifier);
//  event e{{
//    vector{header.timestamp,
//           version,
//           my_autonomous_system,
//           hold_time,
//           bgp_identifier},
//    bgp4mp_open_type_
//  }};
//  e.timestamp(header.timestamp);
//  event_queue.push_back(e);
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_message_update(mrt_data_iterator& f,
//                                             mrt_data_iterator& l,
//                                             mrt_header& header,
//                                             bgp4mp_info& info,
//                                             std::vector<event> &event_queue) {
//  auto count8 = byte->*[](uint8_t x) { return count{x}; };
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  auto ipv4 = b32be->*[](uint32_t x) {
//    return address{&x, address::ipv4, address::host};
//  };
//  auto ipv6 = bytes<16>->*[](std::array<uint8_t, 16> x) {
//    return address{x.data(), address::ipv6, address::network};
//  };
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.3.  UPDATE Message Format
//    +-----------------------------------------------------+
//    |   Withdrawn Routes Length (2 octets)                |
//    +-----------------------------------------------------+
//    |   Withdrawn Routes (variable)                       |
//    +-----------------------------------------------------+
//    |   Total Path Attribute Length (2 octets)            |
//    +-----------------------------------------------------+
//    |   Path Attributes (variable)                        |
//    +-----------------------------------------------------+
//    |   Network Layer Reachability Information (variable) |
//    +-----------------------------------------------------+
//  */
//  count withdrawn_routes_length;
//  count total_path_attribute_length;
//  std::vector<subnet> prefix;
//  if (!count16(f, l, withdrawn_routes_length))
//    return false;
//  VAST_TRACE("mrt-parser parses bgp4mp-message-update:",
//             "withdrawn_routes_length", withdrawn_routes_length);
//  if (!parse_bgp4mp_prefix(f, l, info.afi_ipv4, withdrawn_routes_length,
//                           prefix))
//    return false;
//  for (auto i = 0u; i < prefix.size(); i++) {
//    VAST_TRACE("mrt-parser parses bgp4mp-message-update-withdrawn:", "prefix",
//               prefix[i]);
//    event e{{
//      vector{header.timestamp,
//             info.peer_ip_addr,
//             info.peer_as_nr,
//             prefix[i]},
//      bgp4mp_withdraw_type_
//    }};
//    e.timestamp(header.timestamp);
//    event_queue.push_back(e);
//  }
//  prefix.clear();
//  if (!count16(f, l, total_path_attribute_length))
//    return false;
//  VAST_TRACE("mrt-parser parses bgp4mp-message-update:",
//            "total_path_attribute_length", total_path_attribute_length);
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.3.  UPDATE Message Format
//  Path Attributes
//    [...]
//    Each path attribute is a triple <attribute type, attribute length, attribute
//    value> of variable length.
//    attribute type
//      0                   1
//      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
//      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//      |  Attr. Flags  |Attr. Type Code|
//      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  std::string origin;
//  std::vector<vast::data> as_path;
//  count origin_as = 0;
//  address next_hop;
//  count multi_exit_disc = 0;
//  count local_pref = 0;
//  bool atomic_aggregate = false;
//  count aggregator_as = 0;
//  address aggregator_ip;
//  std::vector<vast::data> communities;
//  int64_t slength = total_path_attribute_length;
//  while (slength > 0) {
//    uint8_t attr_flags;
//    uint8_t attr_type_code;
//    count attr_length;
//    auto bgp4mp_attribute_type_parser = byte >> byte;
//    if (!bgp4mp_attribute_type_parser(f, l, attr_flags, attr_type_code))
//      return false;
//    /*
//    RFC 4271 https://tools.ietf.org/html/rfc4271
//    4.3.  UPDATE Message Format
//    Path Attributes
//      The fourth high-order bit (bit 3) of the Attribute Flags octet is the
//      Extended Length bit. It defines whether the Attribute Length is one octet
//      (if set to 0) or two octets (if set to 1).
//    */
//    bool is_extended_length = ((attr_flags & 16) >> 4) == 1;
//    if (is_extended_length) {
//      if (!count16(f, l, attr_length))
//        return false;
//    } else {
//      if (!count8(f, l, attr_length))
//        return false;
//    }
//    VAST_TRACE("mrt-parser parses path-attribute:", "attr_type_code",
//               static_cast<uint16_t>(attr_type_code), "attr_length",
//               attr_length);
//    /*
//    Path Attribute Type Codes:
//    RFC 4271 https://tools.ietf.org/html/rfc4271
//       1 - ORIGIN
//       2 - AS_PATH
//       3 - NEXT_HOP
//       4 - MULTI_EXIT_DISC
//       5 - LOCAL_PREF
//       6 - ATOMIC_AGGREGATE
//       7 - AGGREGATOR
//    RFC 1997 https://tools.ietf.org/html/rfc1997
//       8 - COMMUNITIES
//    RFC 4760 https://tools.ietf.org/html/rfc4760
//      14 - MP_REACH_NLRI
//      15 - MP_UNREACH_NLRI
//    RFC 4360 https://tools.ietf.org/html/rfc4360
//      16 - Extended Communities
//    RFC 6793 https://tools.ietf.org/html/rfc6793
//      17 - AS4_PATH
//      18 - AS4_AGGREGATOR
//    */
//    auto t = f;
//    switch (attr_type_code) {
//      case 1:
//        if (!parse_bgp4mp_path_attribute_origin(t, l, origin))
//          return false;
//        break;
//      case 2:
//        if(!parse_bgp4mp_path_attribute_as_path(t, l, info.as4, as_path,
//                                                origin_as))
//          return false;
//        break;
//      case 3:
//        if (!ipv4(t, l, next_hop))
//          return false;
//        VAST_TRACE("mrt-parser parses path-attribute:", "next_hop", next_hop);
//        break;
//      case 4:
//        if (!count32(t, l, multi_exit_disc))
//          return false;
//        VAST_TRACE("mrt-parser parses path-attribute:", "multi_exit_disc",
//                   multi_exit_disc);
//        break;
//      case 5:
//        if (!count32(t, l, local_pref))
//          return false;
//        VAST_TRACE("mrt-parser parses path-attribute:", "local_pref", local_pref);
//        break;
//      case 6:
//        atomic_aggregate = true;
//        VAST_TRACE("mrt-parser parses path-attribute:", "atomic_aggregate",
//                   atomic_aggregate);
//        break;
//      case 7:
//        if (!parse_bgp4mp_path_attribute_aggregator(t, l, info.as4,
//                                                    aggregator_as,
//                                                    aggregator_ip))
//          return false;
//        break;
//      case 8:
//        if (!parse_bgp4mp_path_attribute_communities(t, l, attr_length,
//                                                     communities))
//          return false;
//        break;
//      case 14:
//        {
//          /*
//          RFC 4760 https://tools.ietf.org/html/rfc4760
//          3.  Multiprotocol Reachable NLRI - MP_REACH_NLRI (Type Code 14)
//            +---------------------------------------------------------+
//            | Address Family Identifier (2 octets)                    |
//            +---------------------------------------------------------+
//            | Subsequent Address Family Identifier (1 octet)          |
//            +---------------------------------------------------------+
//            | Length of Next Hop Network Address (1 octet)            |
//            +---------------------------------------------------------+
//            | Network Address of Next Hop (variable)                  |
//            +---------------------------------------------------------+
//            | Reserved (1 octet)                                      |
//            +---------------------------------------------------------+
//            | Network Layer Reachability Information (variable)       |
//            +---------------------------------------------------------+
//          */
//          count address_family_identifier = 0;
//          count subsequent_address_family_identifier = 0;
//          count next_hop_network_address_length = 0;
//          address mp_next_hop;
//          count mp_nlri_length = 0;
//          auto mp_reach_nlri_parser = count16 >> count8 >> count8;
//          if (!mp_reach_nlri_parser(t, l, address_family_identifier,
//                                    subsequent_address_family_identifier,
//                                    next_hop_network_address_length))
//            return false;
//          mp_nlri_length = attr_length - (5 + next_hop_network_address_length);
//          VAST_TRACE("mrt-parser parses path-attribute:",
//                     "address_family_identifier", address_family_identifier,
//                     "subsequent_address_family_identifier",
//                     subsequent_address_family_identifier,
//                     "next_hop_network_address_length",
//                     next_hop_network_address_length, "mp_nlri_length",
//                     mp_nlri_length);
//          if (address_family_identifier == 1) {
//            if (!ipv4(t, l, mp_next_hop))
//              return false;
//            t += (next_hop_network_address_length - 4);
//          } else if (address_family_identifier == 2) {
//            if (!ipv6(t, l, mp_next_hop))
//              return false;
//            t += (next_hop_network_address_length - 16);
//          } else {
//            VAST_WARNING(
//              "mrt-parser",
//              "ignores unsupported MP_REACH_NLRI address family identifier",
//              address_family_identifier);
//            return false;
//          }
//          t++; // Reserved
//          VAST_TRACE("mrt-parser parses path-attribute:", "mp_next_hop",
//                     mp_next_hop);
//          if (!parse_bgp4mp_prefix(t, l, (address_family_identifier == 1),
//                                   mp_nlri_length, prefix))
//            return false;
//          for (auto i = 0u; i < prefix.size(); i++) {
//            VAST_TRACE("mrt-parser parses bgp4mp-message-update-announce:",
//                       "prefix", prefix[i]);
//            event e{{
//              vector{header.timestamp,
//                     info.peer_ip_addr,
//                     info.peer_as_nr,
//                     prefix[i],
//                     as_path,
//                     origin_as,
//                     origin,
//                     mp_next_hop,
//                     local_pref,
//                     multi_exit_disc,
//                     communities,
//                     atomic_aggregate,
//                     aggregator_as,
//                     aggregator_ip},
//              bgp4mp_announce_type_
//            }};
//            e.timestamp(header.timestamp);
//            event_queue.push_back(e);
//          }
//          prefix.clear();
//        }
//        break;
//      case 15:
//        {
//          /*
//          RFC 4760 https://tools.ietf.org/html/rfc4760
//          4.  Multiprotocol Unreachable NLRI - MP_UNREACH_NLRI (Type Code 15)
//            +---------------------------------------------------------+
//            | Address Family Identifier (2 octets)                    |
//            +---------------------------------------------------------+
//            | Subsequent Address Family Identifier (1 octet)          |
//            +---------------------------------------------------------+
//            | Withdrawn Routes (variable)                             |
//            +---------------------------------------------------------+
//          */
//          count address_family_identifier = 0;
//          count subsequent_address_family_identifier = 0;
//          count mp_nlri_length = 0;
//          auto mp_unreach_nlri_parser = count16 >> count8;
//          if (!mp_unreach_nlri_parser(t, l, address_family_identifier,
//                                      subsequent_address_family_identifier))
//            return false;
//          mp_nlri_length = attr_length - 3;
//          VAST_TRACE("mrt-parser parses path-attribute:",
//                     "address_family_identifier", address_family_identifier,
//                     "subsequent_address_family_identifier",
//                     subsequent_address_family_identifier, "mp_nlri_length",
//                     mp_nlri_length);
//          if (!parse_bgp4mp_prefix(t, l, (address_family_identifier == 1),
//                                   mp_nlri_length, prefix))
//            return false;
//          for (auto i = 0u; i < prefix.size(); i++) {
//            VAST_TRACE("mrt-parser parses bgp4mp-message-update-withdrawn:",
//                       "prefix", prefix[i]);
//            event e{{
//              vector{header.timestamp,
//                     info.peer_ip_addr,
//                     info.peer_as_nr,
//                     prefix[i]},
//              bgp4mp_withdraw_type_
//            }};
//            e.timestamp(header.timestamp);
//            event_queue.push_back(e);
//          }
//          prefix.clear();
//        }
//        break;
//      case 16:
//        if (! parse_bgp4mp_path_attribute_extended_communities(t, l,
//                                                               attr_length,
//                                                               communities))
//          return false;
//        break;
//      case 17:
//        // Not relevant when encapsulated in mrt.
//        break;
//      case 18:
//        // Not relevant when encapsulated in mrt.
//        break;
//      default:
//        VAST_WARNING("mrt-parser",
//                     "ignores unsupported BGP4MP path attribute type",
//                     static_cast<uint16_t>(attr_type_code));
//    }
//    f += attr_length;
//    if (is_extended_length)
//      slength -= attr_length + 4;
//    else
//      slength -= attr_length + 3;
//  }
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.3.  UPDATE Message Format
//  Network Layer Reachability Information
//    [...] The length, in octets, of the Network Layer Reachability Information
//    is not encoded explicitly, but can be calculated as:
//      UPDATE message Length - 23 - Total Path Attributes Length
//      - Withdrawn Routes Length
//  */
//  count network_layer_reachability_information_length =
//    info.length - 23 - total_path_attribute_length - withdrawn_routes_length;
//  VAST_TRACE("mrt-parser parses bgp4mp-message-update:",
//             "network_layer_reachability_information_length",
//             network_layer_reachability_information_length);
//  if (!parse_bgp4mp_prefix(f, l, info.afi_ipv4,
//                           network_layer_reachability_information_length,
//                           prefix))
//    return false;
//  for (auto i = 0u; i < prefix.size(); i++) {
//    VAST_TRACE("mrt-parser parses bgp4mp-message-update-announce:", "prefix",
//               prefix[i]);
//    event e{{
//      vector{header.timestamp,
//             info.peer_ip_addr,
//             info.peer_as_nr,
//             prefix[i],
//             as_path,
//             origin_as,
//             origin,
//             next_hop,
//             local_pref,
//             multi_exit_disc,
//             communities,
//             atomic_aggregate,
//             aggregator_as,
//             aggregator_ip},
//      bgp4mp_announce_type_
//    }};
//    e.timestamp(header.timestamp);
//    event_queue.push_back(e);
//  }
//  prefix.clear();
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_message_notification(
//  mrt_data_iterator& f, mrt_data_iterator& l, mrt_header& header,
//  std::vector<event>& event_queue) {
//  auto count8 = byte->*[](uint8_t x) { return count{x}; };
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.5.  NOTIFICATION Message Format
//    0                   1                   2                   3
//    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    | Error code    | Error subcode |   Data (variable)             |
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  */
//  count error_code = 0;
//  count error_subcode = 0;
//  auto bgp4mp_messasge_notification_parser = count8 >> count8;
//  if (!bgp4mp_messasge_notification_parser(f, l, error_code, error_subcode))
//    return false;
//  VAST_TRACE("mrt-parser parses bgp4mp-message-notification:", "error_code",
//             error_code, "error_subcode", error_subcode);
//  event e{{
//    vector{header.timestamp,
//           error_code,
//           error_subcode},
//    bgp4mp_notification_type_
//  }};
//  e.timestamp(header.timestamp);
//  event_queue.push_back(e);
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_message_keepalive(
//  mrt_header& header, std::vector<event>& event_queue) {
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.4.  KEEPALIVE Message Format
//    [...] A KEEPALIVE message consists of only the message header [...]
//  */
//  VAST_TRACE("mrt-parser parses bgp4mp-message-keepalive");
//  event e{{vector{header.timestamp}, bgp4mp_keepalive_type_}};
//  e.timestamp(header.timestamp);
//  event_queue.push_back(e);
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_path_attribute_origin(mrt_data_iterator& f,
//                                                    mrt_data_iterator& l,
//                                                    std::string& origin) {
//  auto count8 = byte->*[](uint8_t x) { return count{x}; };
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.3.  UPDATE Message Format
//  Path Attributes
//    a) ORIGIN (Type Code 1)
//  */
//  count value = 0;
//  if (!count8(f, l, value))
//    return false;
//  switch (value) {
//    case 0:
//      origin = "IGP";
//      break;
//    case 1:
//      origin = "EGP";
//      break;
//    case 2:
//      origin = "INCOMPLETE";
//      break;
//  }
//  VAST_TRACE("mrt-parser parses path-attribute:", "origin", origin);
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_path_attribute_as_path(
//  mrt_data_iterator& f, mrt_data_iterator& l, bool as4,
//  std::vector<vast::data>& as_path, count& origin_as) {
//  auto count8 = byte->*[](uint8_t x) { return count{x}; };
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.3.  UPDATE Message Format
//  Path Attributes
//    b) AS_PATH (Type Code 2)
//  */
//  count path_segment_type = 0;
//  count path_segment_length = 0;
//  count path_segment_value = 0;
//  auto bgp4mp_as_path_parser = count8 >> count8;
//  if (!bgp4mp_as_path_parser(f, l, path_segment_type,
//                             path_segment_length))
//    return false;
//  for (auto i = 0u; i < path_segment_length; i++) {
//    /*
//    RFC 6396 https://tools.ietf.org/html/rfc6396
//    4.4.3.  BGP4MP_MESSAGE_AS4 Subtype
//      [...] The AS_PATH in these messages MUST only
//      consist of 4-byte AS numbers. [...]
//    */
//    if (as4) {
//      if (!count32(f, l, path_segment_value))
//        return false;
//    } else {
//      if (!count16(f, l, path_segment_value))
//        return false;
//    }
//    as_path.push_back(path_segment_value);
//  }
//  origin_as = path_segment_value;
//  VAST_TRACE("mrt-parser parses path-attribute:", "as_path", to_string(as_path),
//             "origin_as", origin_as);
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_path_attribute_aggregator(
//  mrt_data_iterator& f, mrt_data_iterator& l, bool as4, count& aggregator_as,
//  address& aggregator_ip) {
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  auto ipv4 = b32be->*[](uint32_t x) {
//    return address{&x, address::ipv4, address::host};
//  };
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.3.  UPDATE Message Format
//  Path Attributes
//    g) AGGREGATOR (Type Code 7)
//  */
//  if (as4) {
//    if (!count32(f, l, aggregator_as))
//      return false;
//  } else {
//    if (!count16(f, l, aggregator_as))
//      return false;
//  }
//  if (!ipv4(f, l, aggregator_ip))
//    return false;
//  VAST_TRACE("mrt-parser parses path-attribute:", "aggregator_as",
//             aggregator_as, "aggregator_ip", aggregator_ip);
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_path_attribute_communities(
//  mrt_data_iterator& f, mrt_data_iterator& l, count attr_length,
//  std::vector<vast::data>& communities) {
//  auto count32 = b32be->*[](uint32_t x) { return count{x}; };
//  /*
//  RFC 1997 https://tools.ietf.org/html/rfc1997
//  COMMUNITIES attribute (Type Code 8)
//    [...] The attribute consists of a set of four octet values, each of
//    which specify a community. [...]
//  */
//  count community = 0;
//  for (auto i = 0u; i < (attr_length / 4u); i++) {
//    if (!count32(f, l, community))
//      return false;
//    communities.push_back(community);
//  }
//  VAST_TRACE("mrt-parser parses path-attribute:", "communities",
//             to_string(communities));
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_path_attribute_extended_communities(
//  mrt_data_iterator& f, mrt_data_iterator& l, count attr_length,
//  std::vector<vast::data>& communities) {
//  auto count16 = b16be->*[](uint16_t x) { return count{x}; };
//  auto count48 = bytes<6>->*[](std::array<uint8_t, 6> x) {
//    uint64_t y = 0;
//    for (auto i = 0u; i < 6u; i++) {
//      y |= x[i] & 0xFF;
//      y <<= 8;
//    }
//    return count{y};
//  };
//  /*
//  RFC 4360 https://tools.ietf.org/html/rfc4360
//  2.  BGP Extended Communities Attribute (Type Code 16)
//    Each Extended Community is encoded as an 8-octet quantity, as
//    follows:
//     - Type Field  : 1 or 2 octets
//     - Value Field : Remaining octets
//      0                   1                   2                   3
//      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     |  Type high    |  Type low(*)  |                               |
//     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+          Value                |
//     |                                                               |
//     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     (*) Present for Extended types only, used for the Value field
//         otherwise.
//  */
//  count type_field = 0;
//  count community = 0;
//  for (auto i = 0u; i < (attr_length / 8u); i++) {
//    auto extended_communites_parser = count16 >> count48;
//    if (!extended_communites_parser(f, l, type_field, community))
//      return false;
//    communities.push_back(community);
//  }
//  VAST_TRACE("mrt-parser parses path-attribute:", "communities",
//             to_string(communities));
//  return true;
//}
//
//bool mrt_parser::parse_bgp4mp_prefix(mrt_data_iterator& f, mrt_data_iterator& l,
//                                     bool afi_ipv4, count length,
//                                     std::vector<subnet>& prefix) {
//  /*
//  RFC 4271 https://tools.ietf.org/html/rfc4271
//  4.3.  UPDATE Message Format
//  Prefix
//    +---------------------------+
//    |   Length (1 octet)        |
//    +---------------------------+
//    |   Prefix (variable)       |
//    +---------------------------+
//  */
//  int64_t slength = length;
//  while (slength > 0) {
//    uint8_t prefix_length;
//    if (!byte(f, l, prefix_length))
//      return false;
//    count prefix_bytes = prefix_length / 8;
//    if (prefix_length % 8 != 0)
//      prefix_bytes++;
//    std::array<uint8_t, 16> ip{};
//    for (auto i = 0u; i < prefix_bytes; i++) {
//      if (!byte(f, l, ip[i]))
//        return false;
//    }
//    prefix.emplace_back(subnet{address{ip.data(),
//                                       afi_ipv4 ? address::ipv4 : address::ipv6,
//                                       address::network},
//                               prefix_length});
//    slength -= prefix_bytes + 1;
//  }
//  return true;
//}
//
//
//reader::reader(std::unique_ptr<std::istream> input) : input_{std::move(input)} {
//  VAST_ASSERT(input_);
//}
//
//expected<event> reader::read() {
//  if (!event_queue_.empty()) {
//    event current_event = event_queue_.back();
//    event_queue_.pop_back();
//    return std::move(current_event);
//  }
//  if (input_->eof())
//    return make_error(ec::end_of_input, "input exhausted");
//  if (!parser_.parse(*input_, event_queue_))
//    return make_error(ec::parse_error, "failed to parse MRT record");
//  if (!event_queue_.empty()) {
//    event current_event = event_queue_.back();
//    event_queue_.pop_back();
//    return std::move(current_event);
//  }
//  return no_error;
//}
//
//expected<void> reader::schema(vast::schema const& sch) {
//  auto types = {
//    &parser_.table_dump_v2_peer_type_,
//    &parser_.table_dump_v2_rib_entry_type_,
//    &parser_.bgp4mp_announce_type_,
//    &parser_.bgp4mp_withdraw_type_,
//    &parser_.bgp4mp_state_change_type_,
//    &parser_.bgp4mp_open_type_,
//    &parser_.bgp4mp_notification_type_,
//    &parser_.bgp4mp_keepalive_type_,
//  };
//  for (auto t : types)
//    if (auto u = sch.find(t->name())) {
//      if (! congruent(*t, *u))
//        return make_error(ec::format_error, "incongruent type:", t->name());
//      else
//        *t = *u;
//    }
//  return {};
//}
//
//expected<schema> reader::schema() const {
//  vast::schema sch;
//  sch.add(parser_.table_dump_v2_peer_type_);
//  sch.add(parser_.table_dump_v2_rib_entry_type_);
//  sch.add(parser_.bgp4mp_announce_type_);
//  sch.add(parser_.bgp4mp_withdraw_type_);
//  sch.add(parser_.bgp4mp_state_change_type_);
//  sch.add(parser_.bgp4mp_open_type_);
//  sch.add(parser_.bgp4mp_notification_type_);
//  sch.add(parser_.bgp4mp_keepalive_type_);
//  return sch;
//}
//
//char const* reader::name() const {
//  return "mrt-reader";
//}
//
//} // namespace mrt

} // namespace format
} // namespace vast
