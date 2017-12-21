#ifndef VAST_FORMAT_MRT_HPP
#define VAST_FORMAT_MRT_HPP

#include "vast/address.hpp"
#include "vast/event.hpp"
#include "vast/none.hpp"
#include "vast/schema.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/address.hpp"

namespace vast {
namespace format {
namespace mrt {

// -- RFC 4271 ----------------------------------------------------------------

/// This namespace includes BGP types as defined in [RFC
/// 4271](https://tools.ietf.org/html/rfc4271).
namespace bgp {

/// BGP messages are sent over TCP connections.  A message is processed only
/// after it is entirely received.  The maximum message size is 4096 octets.
/// All implementations are required to support this maximum message size.  The
/// smallest message that may be sent consists of a BGP header without a data
/// portion (19 octets).
///
/// All multi-octet fields are in network byte order.
///
/// Each message has a fixed-size header.  There may or may not be a data
/// portion following the header, depending on the message type.  The layout of
/// these fields is shown below:
///
///      0                   1                   2                   3
///      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///      |                                                               |
///      +                                                               +
///      |                                                               |
///      +                                                               +
///      |                           Marker                              |
///      +                                                               +
///      |                                                               |
///      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///      |          Length               |      Type     |
///      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
/// Marker:
///
///    This 16-octet field is included for compatibility; it MUST be
///    set to all ones.
///
/// Length:
///
///    This 2-octet unsigned integer indicates the total length of the
///    message, including the header in octets.  Thus, it allows one
///    to locate the (Marker field of the) next message in the TCP
///    stream.  The value of the Length field MUST always be at least
///    19 and no greater than 4096, and MAY be further constrained,
///    depending on the message type.  "padding" of extra data after
///    the message is not allowed.  Therefore, the Length field MUST
///    have the smallest value required, given the rest of the
///    message.
///
/// Type:
///
///    This 1-octet unsigned integer indicates the type code of the
///    message.  This document defines the following type codes:
///
///                         1 - OPEN
///                         2 - UPDATE
///                         3 - NOTIFICATION
///                         4 - KEEPALIVE
///
///    [RFC2918](...) defines one more type code.
struct message_header {
  std::array<uint8_t, 16> marker;
  uint16_t length;
  uint8_t type;
};

struct message_header_parser : parser<message_header_parser> {
  using attribute = message_header;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, message_header& x) const {
    using namespace parsers;
    auto skip = [&] {
      static auto header_length = 16 + 2 + 1;
      if (x.length < header_length || x.length > 4096)
        throw std::runtime_error{"cannot parse RFC-violoating records"};
      f += std::min(static_cast<size_t>(l - f),
                    static_cast<size_t>(x.length - header_length));
    };
    auto p = (bytes<16> >> b16be >> byte) ->* skip;
    return p(f, l, x.marker, x.length, x.type);
  }
};

} // namespace bgp

// -- RFC 6396 (MRT) ----------------------------------------------------------

/// [MRT Common Header](https://tools.ietf.org/html/rfc6396#section-2).
///
/// All MRT format records have a Common Header that consists of a Timestamp,
/// Type, Subtype, and Length field.  The header is followed by a Message
/// field.  The MRT Common Header is illustrated below.
///
///        0                   1                   2                   3
///        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                           Timestamp                           |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |             Type              |            Subtype            |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                             Length                            |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                      Message... (variable)
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
struct common_header {
  uint32_t timestamp;   ///< UNIX time
  uint16_t type = 0;    ///< Message type
  uint16_t subtype = 0; ///< Message subtype
  uint32_t length = 0;  ///< Number of bytes in message (excluding header)
};

/// [MRT Types](https://tools.ietf.org/html/rfc6396#section-4).
///
/// The following MRT Types are currently defined for the MRT format. The MRT
/// Types that contain the "_ET" suffix in their names identify those types
/// that use an Extended Timestamp MRT Header.  The Subtype and Message fields
/// in these types remain as defined for the MRT Types of the same name without
/// the "_ET" suffix.
enum types {
  OSPFv2 = 11,
  TABLE_DUMP = 12,
  TABLE_DUMP_V2 = 13,
  BGP4MP = 16,
  BGP4MP_ET = 17,
  ISIS = 32,
  ISIS_ET = 33,
  OSPFv3 = 48,
  OSPFv3_ET = 49,
};

// TODO
struct ospfv2 {
};

// TODO
struct table_dump {
};

/// The TABLE_DUMP_V2 Type updates the TABLE_DUMP Type to include 4-byte
/// Autonomous System Number (ASN) support and full support for BGP
/// multiprotocol extensions.  It also improves upon the space efficiency of
/// the TABLE_DUMP Type by employing an index table for peers and permitting a
/// single MRT record per Network Layer Reachability Information (NLRI) entry.
/// The following subtypes are used with the TABLE_DUMP_V2 Type.
///
///     1    PEER_INDEX_TABLE
///     2    RIB_IPV4_UNICAST
///     3    RIB_IPV4_MULTICAST
///     4    RIB_IPV6_UNICAST
///     5    RIB_IPV6_MULTICAST
///     6    RIB_GENERIC
namespace table_dump_v2 {

enum subtypes {
  PEER_INDEX_TABLE = 1,
  RIB_IPV4_UNICAST = 2,
  RIB_IPV4_MULTICAST = 3,
  RIB_IPV6_UNICAST = 4,
  RIB_IPV6_MULTICAST = 5,
  RIB_GENERIC = 6,
};

// TODO
struct rib_ipv4_unicast {
};

// TODO
struct rib_ipv4_multicast {
};

// TODO
struct rib_ipv6_unicast {
};

// TODO
struct rib_ipv6_multicast {
};

struct peer_entries; // forward declaration

/// An initial PEER_INDEX_TABLE MRT record provides the BGP ID of the
/// collector, an OPTIONAL view name, and a list of indexed peers.
///
/// The header of the PEER_INDEX_TABLE Subtype is shown below.  The View Name
/// is OPTIONAL and, if not present, the View Name Length MUST be set to 0.
/// The View Name encoding MUST follow the UTF-8 transformation format
/// [RFC3629].
///
///      0                   1                   2                   3
///      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                      Collector BGP ID                         |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |       View Name Length        |     View Name (variable)      |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |          Peer Count           |    Peer Entries (variable)
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
struct peer_index_table {
  uint32_t collector_bgp_id;
  uint16_t view_name_length;
  std::string view_name;
  uint16_t peer_count;
  std::vector<peer_entries> peer_entries;
};

/// Peer Entries message.
///
///      0                   1                   2                   3
///      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |   Peer Type   |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                         Peer BGP ID                           |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                   Peer IP Address (variable)                  |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                        Peer AS (variable)                     |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
/// The Peer Type, Peer BGP ID, Peer IP Address, and Peer AS fields are
/// repeated as indicated by the Peer Count field.  The position of the peer
/// in the PEER_INDEX_TABLE is used as an index in the subsequent
/// TABLE_DUMP_V2 MRT records. The index number begins with 0.
///
/// The Peer Type field is a bit field that encodes the type of the AS and IP
/// address as identified by the A and I bits, respectively, below.
///
///     0 1 2 3 4 5 6 7
///    +-+-+-+-+-+-+-+-+
///    | | | | | | |A|I|
///    +-+-+-+-+-+-+-+-+
///
///    Bit 6: Peer AS number size:  0 = 16 bits, 1 = 32 bits
///    Bit 7: Peer IP Address family:  0 = IPv4,  1 = IPv6
///
struct peer_entries {
  uint8_t peer_type;
  uint32_t peer_bgp_id;
  address peer_ip_address;
  uint32_t peer_as;
};

// TODO
struct rib_generic {
};

} // namespace table_dump_v2

/// The BGP4MP Type. This type was initially defined in the Zebra software
/// package for the BGP protocol with multiprotocol extension support as
/// defined by RFC 4760 [RFC4760].  The BGP4MP Type has six Subtypes, which are
/// defined as follows:
///
///       0    BGP4MP_STATE_CHANGE
///       1    BGP4MP_MESSAGE
///       4    BGP4MP_MESSAGE_AS4
///       5    BGP4MP_STATE_CHANGE_AS4
///       6    BGP4MP_MESSAGE_LOCAL
///       7    BGP4MP_MESSAGE_AS4_LOCAL
namespace bgp4mp {

enum subtypes {
  STATE_CHANGE = 0,
  MESSAGE = 1,
  MESSAGE_AS4 = 4,
  STATE_CHANGE_AS4 = 5,
  MESSAGE_LOCAL = 6,
  MESSAGE_AS4_LOCAL = 7,
};

struct state_change {
  // TODO
};

/// This subtype is used to encode BGP messages.  It can be used to encode
/// any Type of BGP message.  The entire BGP message is encapsulated in the
/// BGP Message field, including the 16-octet marker, the 2-octet length, and
/// the 1-octet type fields.  The BGP4MP_MESSAGE Subtype does not support
/// 4-byte AS numbers.  The AS_PATH contained in these messages MUST only
/// consist of 2-byte AS numbers.  The BGP4MP_MESSAGE_AS4 Subtype updates the
/// BGP4MP_MESSAGE Subtype in order to support 4-byte AS numbers.  The
/// BGP4MP_MESSAGE fields are shown below:
///
///      0                   1                   2                   3
///      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |         Peer AS Number        |        Local AS Number        |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |        Interface Index        |        Address Family         |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                      Peer IP Address (variable)               |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                      Local IP Address (variable)              |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                    BGP Message... (variable)
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
struct message {
  uint16_t peer_as_number;
  uint16_t local_as_number;
  uint16_t interface_index;
  uint16_t address_family;
  address peer_ip_address;
  address local_ip_address;
  bgp::message_header message;
};

/// This subtype updates the BGP4MP_MESSAGE Subtype to support 4-byte AS
/// numbers.  The BGP4MP_MESSAGE_AS4 Subtype is otherwise identical to the
/// BGP4MP_MESSAGE Subtype. The AS_PATH in these messages MUST only consist
/// of 4-byte AS numbers. The BGP4MP_MESSAGE_AS4 fields are shown below:
///
///        0                   1                   2                   3
///        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                         Peer AS Number                        |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                         Local AS Number                       |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |        Interface Index        |        Address Family         |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                      Peer IP Address (variable)               |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                      Local IP Address (variable)              |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                    BGP Message... (variable)
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
struct message_as4 {
  uint32_t peer_as_number;
  uint32_t local_as_number;
  uint16_t interface_index;
  uint16_t address_family;
  address peer_ip_address;
  address local_ip_address;
  bgp::message_header message;
};

/// This subtype updates the BGP4MP_STATE_CHANGE Subtype to support 4-byte AS
/// numbers.  As with the BGP4MP_STATE_CHANGE Subtype, the BGP FSM states are
/// encoded in the Old State and New State fields to indicate the previous
/// and current state.  Aside from the extension of the Peer and Local AS
/// Number fields to 4 bytes, this subtype is otherwise identical to the
/// BGP4MP_STATE_CHANGE Subtype.  The BGP4MP_STATE_CHANGE_AS4 fields are
/// shown below:
///
///      0                   1                   2                   3
///      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                         Peer AS Number                        |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                         Local AS Number                       |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |        Interface Index        |        Address Family         |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                      Peer IP Address (variable)               |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |                      Local IP Address (variable)              |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///     |            Old State          |          New State            |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
struct state_change_as4 {
  uint32_t peer_as_number;
  uint32_t local_as_number;
  uint16_t interface_index;
  uint16_t address_family;
  address peer_ip_address;
  address local_ip_address;
  uint16_t old_state;
  uint16_t new_state;
};

struct message_local {
  // TODO
};

struct message_as4_local {
  // TODO
};

} // namespace bgp4mp

namespace isis {
// TODO
} // namespace isis

namespace isis_et {
// TODO
} // namespace isis_et

namespace ospfv3 {
// TODO
} // namespace ospfv3

namespace ospfv3_et {
// TODO
} // namespace ospfv3_et

// -- Parsers -----------------------------------------------------------------

struct common_header_parser : parser<common_header_parser> {
  using attribute = common_header;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, common_header& x) const {
    using namespace parsers;
    auto p = b32be >> b16be >> b16be >> b32be;
    return p(f, l, x.timestamp, x.type, x.subtype, x.length);
  }
};

namespace detail {

template <class F>
auto make_ip_v4_v6_parser(F f) {
  using namespace parsers;
  auto v4 = [](uint32_t a) { return address::v4(&a); };
  auto v6 = [](std::array<uint8_t, 16> a) { return address::v6(a.data()); };
  return (b32be ->* v4).when(f) | (bytes<16> ->* v6);
}

} // namespace detail

namespace table_dump_v2 {

struct peer_entries_parser : parser<peer_entries_parser> {
  using attribute = peer_entries;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, peer_entries& x)
  const {
    using namespace parsers;
    auto ip_addr = detail::make_ip_v4_v6_parser(
      [&] { return (x.peer_type & 1) == 0; }
    );
    auto to_u32 = [](uint16_t u) { return uint32_t{u}; };
    auto peer_as =
        (b16be ->* to_u32).when([&] { return (x.peer_type & 2) == 0; })
      | b32be
      ;
    auto p = byte >> b32be >> ip_addr >> peer_as;
    return p(f, l, x.peer_type, x.peer_bgp_id, x.peer_ip_address, x.peer_as);
  }
};

struct peer_index_table_parser : parser<peer_index_table_parser> {
  using attribute = peer_index_table;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, peer_index_table& x)
  const {
    using namespace parsers;
    auto view_name = nbytes<char>(x.view_name_length);
    auto peer_entries = rep(peer_entries_parser{}, x.peer_count);
    auto p = b32be >> b16be >> view_name >> b16be >> peer_entries;
    return p(f, l, x.collector_bgp_id, x.view_name_length, x.view_name,
             x.peer_count, x.peer_entries);
  }
};

} // namespace table_dump_v2

namespace bgp4mp {

struct message_as4_parser : parser<message_as4_parser> {
  using attribute = message_as4;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, message_as4& x) const {
    using namespace parsers;
    auto ip_addr = detail::make_ip_v4_v6_parser(
      [&] { return x.address_family == 1; }
    );
    auto msg = bgp::message_header_parser{};
    auto p = b32be >> b32be >> b16be >> b16be >> ip_addr >> ip_addr >> msg;
    return p(f, l, x.peer_as_number, x.local_as_number, x.interface_index,
             x.address_family, x.peer_ip_address, x.local_ip_address,
             x.message);
  };
};

} // namespace bgp4mp

/// The top-level MRT record. All MRT format records have a Common Header
/// followed by a Message field.
struct record {
  common_header header;
  variant<
    none,
    table_dump_v2::peer_index_table,
    bgp4mp::message_as4
  > message;
};

struct record_parser : parser<record_parser> {
  using attribute = record;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, record& x) const {
    auto peer_index_table = table_dump_v2::peer_index_table_parser{}.when([&] {
      return x.header.type == TABLE_DUMP_V2
        && x.header.subtype == table_dump_v2::PEER_INDEX_TABLE;
    });
    auto message_as4 = bgp4mp::message_as4_parser{}.when([&] {
      return x.header.type == BGP4MP
        && x.header.subtype == bgp4mp::MESSAGE_AS4;
    });
    auto skip = parsers::eps ->* [&] {
      f += std::min(static_cast<size_t>(l - f),
                    static_cast<size_t>(x.header.length));
    };
    auto msg = peer_index_table
             | message_as4
             | skip;
    auto p = common_header_parser{} >> msg;
    return p(f, l, x.header, x.message);
  }
};

/// An MRT reader.
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
  std::unique_ptr<std::istream> input_;
  std::vector<char> buffer_;
  record_parser parser_;
};

} // namespace mrt

//namespace mrt {
//
//// ----------------------------------------------------------------------------
//
///// A parser for bgp4mp messages and table_dump_v2 entries from MRT
///// (Multi-Threaded Routing Toolkit Routing Information Export Format - RFC 6396
///// - https://tools.ietf.org/html/rfc6396) files.
//struct mrt_parser {
//  using mrt_data_iterator = std::vector<char>::iterator;
//
//  type table_dump_v2_peer_type_;
//  type table_dump_v2_rib_entry_type_;
//  type bgp4mp_announce_type_;
//  type bgp4mp_withdraw_type_;
//  type bgp4mp_state_change_type_;
//  type bgp4mp_open_type_;
//  type bgp4mp_notification_type_;
//  type bgp4mp_keepalive_type_;
//
//  struct mrt_header {
//    vast::timestamp timestamp;
//    count type = 0;
//    count subtype = 0;
//    count length = 0;
//  };
//
//  struct bgp4mp_info {
//    bool as4;
//    bool afi_ipv4;
//    count peer_as_nr = 0;
//    address peer_ip_addr;
//    count length = 0;
//  };
//
//  mrt_parser();
//  bool parse(std::istream& input, std::vector<event> &event_queue);
//
//private:
//  // ### MRT ###
//  // Parses the MRT header
//  bool parse_mrt_header(mrt_data_iterator& f, mrt_data_iterator& l,
//                        mrt_header& header);
//  // ### MRT/TABLE_DUMP_V2 ###
//  // Parses the MRT type TABLE_DUMP_V2
//  bool parse_mrt_message_table_dump_v2(mrt_data_iterator& f,
//                                       mrt_data_iterator& l,
//                                       mrt_header& header,
//                                       std::vector<event>& event_queue);
//  // Parses the TABLE_DUMP_V2 subtype PEER_INDEX_TABLE
//  bool parse_mrt_message_table_dump_v2_peer(mrt_data_iterator& f,
//                                            mrt_data_iterator& l,
//                                            mrt_header& header,
//                                            std::vector<event>& event_queue);
//  // Parses the TABLE_DUMP_V2 subtype RIB_IPV4_* and RIB_IPV6_*
//  bool parse_mrt_message_table_dump_v2_rib(mrt_data_iterator& f,
//                                           mrt_data_iterator& l,
//                                           mrt_header& header,
//                                           bool afi_ipv4,
//                                           std::vector<event>& event_queue);
//  // ### MRT/BGP4MP ###
//  // Parses the MRT type BGP4MP_ET
//  bool parse_mrt_message_bgp4mp_et(mrt_data_iterator& f, mrt_data_iterator& l,
//                                   mrt_header& header,
//                                   std::vector<event> &event_queue);
//  // Parses the MRT type BGP4MP
//  bool parse_mrt_message_bgp4mp(mrt_data_iterator& f, mrt_data_iterator& l,
//                                mrt_header& header,
//                                std::vector<event> &event_queue);
//  // Parses the BGP4MP subtype BGP4MP_STATE_CHANGE and BGP4MP_STATE_CHANGE_AS4
//  bool parse_mrt_message_bgp4mp_state_change(mrt_data_iterator& f,
//                                             mrt_data_iterator& l, bool as4,
//                                             mrt_header& header,
//                                             std::vector<event> &event_queue);
//  // Parses the BGP4MP subtype BGP4MP_MESSAGE and BGP4MP_MESSAGE_AS4
//  bool parse_mrt_message_bgp4mp_message(mrt_data_iterator& f,
//                                        mrt_data_iterator& l, bool as4,
//                                        mrt_header& header,
//                                        std::vector<event> &event_queue);
//  // ### BGP4MP ###
//  // Parses a BGP4MP message OPEN
//  bool parse_bgp4mp_message_open(mrt_data_iterator& f, mrt_data_iterator& l,
//                                 mrt_header& header, bgp4mp_info& info,
//                                 std::vector<event>& event_queue);
//  // Parses a BGP4MP message UPDATE
//  bool parse_bgp4mp_message_update(mrt_data_iterator& f, mrt_data_iterator& l,
//                                   mrt_header& header, bgp4mp_info& info,
//                                   std::vector<event>& event_queue);
//  // Parses a BGP4MP message NOTIFICATION
//  bool parse_bgp4mp_message_notification(mrt_data_iterator& f,
//                                         mrt_data_iterator& l,
//                                         mrt_header& header,
//                                         std::vector<event>& event_queue);
//  // Parses a BGP4MP message KEEPALIVE
//  bool parse_bgp4mp_message_keepalive(mrt_header& header,
//                                      std::vector<event>& event_queue);
//  // Parses the BGP4MP path attribute ORIGIN
//  bool parse_bgp4mp_path_attribute_origin(mrt_data_iterator& f,
//                                          mrt_data_iterator& l,
//                                          std::string& origin);
//  // Parses the BGP4MP path attribute AS_PATH
//  bool parse_bgp4mp_path_attribute_as_path(mrt_data_iterator& f,
//                                           mrt_data_iterator& l, bool as4,
//                                           std::vector<vast::data>& as_path,
//                                           count& origin_as);
//  // Parses the BGP4MP path attribute AGGREGATOR
//  bool parse_bgp4mp_path_attribute_aggregator(mrt_data_iterator& f,
//                                              mrt_data_iterator& l, bool as4,
//                                              count& aggregator_as,
//                                              address& aggregator_ip);
//  // Parses the BGP4MP path attribute COMMUNITIES
//  bool parse_bgp4mp_path_attribute_communities(
//    mrt_data_iterator& f, mrt_data_iterator& l, count attr_length,
//    std::vector<vast::data>& communities);
//  // Parses the BGP4MP path attribute EXTENDED_COMMUNITIES
//  bool parse_bgp4mp_path_attribute_extended_communities(
//    mrt_data_iterator& f, mrt_data_iterator& l, count attr_length,
//    std::vector<vast::data>& communities);
//  // Parse prefix in the BGP4MP prefix encoding
//  bool parse_bgp4mp_prefix(mrt_data_iterator& f, mrt_data_iterator& l,
//                           bool afi_ipv4, count length,
//                           std::vector<subnet>& prefix);
//};
//
///// A MRT reader.
//class reader {
//public:
//  reader() = default;
//
//  /// Constructs a MRT reader.
//  explicit reader(std::unique_ptr<std::istream> input);
//
//  expected<event> read();
//
//  expected<void> schema(vast::schema const& sch);
//
//  expected<vast::schema> schema() const;
//
//  const char* name() const;
//
//private:
//  mrt_parser parser_;
//  std::unique_ptr<std::istream> input_;
//  std::vector<event> event_queue_;
//};

} // namespace format
} // namespace vast

#endif
