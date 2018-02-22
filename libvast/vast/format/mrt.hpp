#ifndef VAST_FORMAT_MRT_HPP
#define VAST_FORMAT_MRT_HPP

#include <iostream>
#include <queue>

#include "vast/address.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/logger.hpp"
#include "vast/none.hpp"
#include "vast/schema.hpp"
#include "vast/subnet.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/data.hpp"

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

enum types {
  OPEN = 1,
  UPDATE = 2,
  NOTIFICATION = 3,
  KEEPALICE = 4,
};

/// In addition to the fixed-size BGP header, the OPEN message contains
/// the following fields:
///
///       0                   1                   2                   3
///       0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///       +-+-+-+-+-+-+-+-+
///       |    Version    |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |     My Autonomous System      |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |           Hold Time           |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                         BGP Identifier                        |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       | Opt Parm Len  |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                                                               |
///       |             Optional Parameters (variable)                    |
///       |                                                               |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
struct open {
  uint8_t version;
  uint16_t my_autonomous_system;
  uint16_t hold_time;
  uint32_t bgp_identifier;
  uint8_t opt_parm_len;
  // optional_parameters
};

struct attributes {
  std::string origin;
  std::vector<uint32_t> as_path;
  address next_hop;
  uint32_t multi_exit_disc;
  uint32_t local_pref;
  bool atomic_aggregate = false;
  uint32_t aggregator_as;
  address aggregator_ip;
  std::vector<uint64_t> communities;
  std::vector<subnet> mp_reach_nlri;
  std::vector<subnet> mp_unreach_nlri;
};

///   An UPDATE message is used to advertise feasible routes that share
///   common path attributes to a peer, or to withdraw multiple unfeasible
///   routes from service (see 3.1).  An UPDATE message MAY simultaneously
///   advertise a feasible route and withdraw multiple unfeasible routes
///   from service.  The UPDATE message always includes the fixed-size BGP
///   header, and also includes the other fields, as shown below (note,
///   some of the shown fields may not be present in every UPDATE message):
///
///      +-----------------------------------------------------+
///      |   Withdrawn Routes Length (2 octets)                |
///      +-----------------------------------------------------+
///      |   Withdrawn Routes (variable)                       |
///      +-----------------------------------------------------+
///      |   Total Path Attribute Length (2 octets)            |
///      +-----------------------------------------------------+
///      |   Path Attributes (variable)                        |
///      +-----------------------------------------------------+
///      |   Network Layer Reachability Information (variable) |
///      +-----------------------------------------------------+
///
struct update {
  uint16_t withdrawn_routes_length;
  std::vector<subnet> withdrawn_routes;
  uint16_t total_path_attribute_length;
  attributes path_attributes;
  std::vector<subnet> network_layer_reachability_information;
};

/// In addition to the fixed-size BGP header, the NOTIFICATION message
/// contains the following fields:
///
///      0                   1                   2                   3
///      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///      | Error code    | Error subcode |   Data (variable)             |
///      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
struct notification {
  uint8_t error_code;
  uint8_t error_subcode;
  // data
};

/// A KEEPALIVE message consists of only the message header and has a
/// length of 19 octets.
struct keepalive;

struct message {
  message_header header;
  variant<
    none,
    open,
    update,
    notification
  > message;
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

struct rib_entry;

/// RIB Entry Header.
///
///        0                   1                   2                   3
///        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                         Sequence Number                       |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       | Prefix Length |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                        Prefix (variable)                      |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |         Entry Count           |  RIB Entries (variable)
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
struct rib_entry_header {
  uint32_t sequence_number;
  uint8_t prefix_length;
  std::vector<subnet> prefix;
  uint16_t entry_count;
  std::vector<rib_entry> rib_entries;
};

/// RIB Entry. The RIB Entries are repeated Entry Count times.  These entries
/// share a common format as shown below.  They include a Peer Index from the
/// PEER_INDEX_TABLE MRT record, an originated time for the RIB Entry, and the
/// BGP path attribute length and attributes.  All AS numbers in the AS_PATH
/// attribute MUST be encoded as 4-byte AS numbers.
///
///         0                   1                   2                   3
///         0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///        |         Peer Index            |
///        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///        |                         Originated Time                       |
///        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///        |      Attribute Length         |
///        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///        |                    BGP Attributes... (variable)
///        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
struct rib_entry {
  uint16_t peer_index;
  uint32_t originated_time;
  uint16_t attribute_length;
  bgp::attributes bgp_attributes;
};

/// RIB_GENERIC Entry Header.
///
///        0                   1                   2                   3
///        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |                         Sequence Number                       |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |    Address Family Identifier  |Subsequent AFI |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |     Network Layer Reachability Information (variable)         |
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///       |         Entry Count           |  RIB Entries (variable)
///       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
struct rib_generic_header {
  uint32_t sequence_number;
  uint16_t afi_id;
  uint8_t subsequent_afi;
  // TODO
};

/// AFI/SAFI-Specific RIB Subtypes.
struct rib_afi_safi {
  rib_entry_header header;
  std::vector<rib_entry> entries;
};

/// RIB_GENERIC Subtype.
struct rib_generic {
  rib_generic_header header;
  std::vector<rib_entry> entries;
};

struct peer_entry; // forward declaration

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
  std::vector<peer_entry> peer_entries;
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
struct peer_entry {
  uint8_t peer_type;
  uint32_t peer_bgp_id;
  address peer_ip_address;
  uint32_t peer_as;
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

/// This message is used to encode state changes in the BGP finite state
/// machine (FSM).  The BGP FSM states are encoded in the Old State and
/// New State fields to indicate the previous and current state.  In some
/// cases, the Peer AS Number may be undefined.  In such cases, the value
/// of this field MAY be set to zero.  The format is illustrated below:
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
///     |            Old State          |          New State            |
///     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///
struct state_change {
  uint16_t peer_as_number;
  uint16_t local_as_number;
  uint16_t interface_index;
  uint16_t address_family;
  address peer_ip_address;
  address local_ip_address;
  uint16_t old_state;
  uint16_t new_state;
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
  bgp::message message;
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
  bgp::message message;
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
  // auto v6 = [](std::array<uint8_t, 16> a) { return address::v6(a.data()); };
  auto v6 = [](std::array<uint8_t, 16> a) {
    return address{a.data(), address::ipv6, address::network};
  };
  return (b32be ->* v4).when(f) | (bytes<16> ->* v6);
}

} // namespace detail

namespace bgp {

namespace detail {

template <class Iterator>
bool parse_prefix(Iterator& f, const Iterator& l, std::vector<subnet>& x,
                  uint16_t length, bool afi_ipv4) {
  using namespace parsers;
  int64_t slength = length;
  while (slength > 0) {
    uint8_t prefix_length;
    if (!byte(f, l, prefix_length))
      return false;
    count prefix_bytes = prefix_length / 8;
    if (prefix_length % 8 != 0)
      prefix_bytes++;
    std::array<uint8_t, 16> ip{};
    for (auto i = 0u; i < prefix_bytes; i++) {
      if (!byte(f, l, ip[i]))
        return false;
    }
    x.push_back(subnet{address{ip.data(),
                               afi_ipv4 ? address::ipv4 : address::ipv6,
                               address::network},
                       prefix_length});
    slength -= prefix_bytes + 1;
  }
  return true;
}

template <class Iterator>
bool parse_attribute_origin(Iterator& f, const Iterator& l, std::string& x) {
  using namespace parsers;
  uint8_t value = 0;
  if (!byte(f, l, value))
    return false;
  switch (value) {
    case 0:
      x = "IGP";
      break;
    case 1:
      x = "EGP";
      break;
    case 2:
      x = "INCOMPLETE";
      break;
 }
 return true;
}

template <class Iterator>
bool parse_attribute_as_path(Iterator& f, const Iterator& l,
                             std::vector<uint32_t>& x, bool as4) {
  using namespace parsers;
  auto b16be_to_32 = b16be->*[](uint16_t x) { return uint32_t{x}; };
  uint8_t path_segment_type = 0;
  uint8_t path_segment_length = 0;
  uint32_t path_segment_value = 0;
  auto path_segment_parser = byte >> byte;
  if (!path_segment_parser(f, l, path_segment_type, path_segment_length))
    return false;
  for (auto i = 0u; i < path_segment_length; i++) {
    if (as4) {
      if (!b32be(f, l, path_segment_value))
        return false;
    } else {
      if (!b16be_to_32(f, l, path_segment_value))
        return false;
    }
    x.push_back(path_segment_value);
  }
  return true;
}

template <class Iterator>
bool parse_attribute_aggregator(Iterator& f, const Iterator& l,
                                uint32_t& aggregator_as, address& aggregator_ip,
                                bool as4) {
  using namespace parsers;
  auto b16be_to_32 = b16be->*[](uint16_t x) { return uint32_t{x}; };
  if (as4) {
    if (!b32be(f, l, aggregator_as))
      return false;
  } else {
    if (!b16be_to_32(f, l, aggregator_as))
      return false;
  }
  auto ipv4 = mrt::detail::make_ip_v4_v6_parser( [&] { return true; });
  if (!ipv4(f, l, aggregator_ip))
    return false;
  return true;
}

template <class Iterator>
bool parse_attribute_communities(Iterator& f, const Iterator& l,
                                 std::vector<uint64_t>& communities,
                                 uint16_t length) {
  using namespace parsers;
  auto b32be_to_64 = b32be->*[](uint32_t x) { return uint64_t{x}; };
  uint64_t community = 0;
  for (auto i = 0u; i < (length / 4u); i++) {
    if (!b32be_to_64(f, l, community))
      return false;
    communities.push_back(community);
  }
  return true;
}

template <class Iterator>
bool parse_attribute_extended_communities(Iterator& f, const Iterator& l,
                                          std::vector<uint64_t>& communities,
                                          uint16_t length) {
  using namespace parsers;
  uint64_t community = 0;
  for (auto i = 0u; i < (length / 8u); i++) {
    if (!b64be(f, l, community))
      return false;
    communities.push_back(community);
  }
  return true;
}

template <class Iterator>
bool parse_attributes(Iterator& f, const Iterator& l, attributes& x,
                      uint16_t length, bool as4) {
  using namespace parsers;
  auto byte_to_16 = byte->*[](uint8_t x) { return uint16_t{x}; };
  int64_t slength = length;
  while (slength > 0) {
    uint8_t attr_flags;
    uint8_t attr_type_code;
    uint16_t attr_length;
    auto attribute_type_parser = byte >> byte;
    if (!attribute_type_parser(f, l, attr_flags, attr_type_code))
      return false;
    bool is_extended_length = ((attr_flags & 16) >> 4) == 1;
    if (is_extended_length) {
      if (!b16be(f, l, attr_length))
        return false;
    } else {
      if (!byte_to_16(f, l, attr_length))
        return false;
    }
    auto t = f;
    switch (attr_type_code) {
      case 1:
        if (!parse_attribute_origin(t, l, x.origin))
          return false;
        break;
      case 2:
        if(!parse_attribute_as_path(t, l, x.as_path, as4))
          return false;
        break;
      case 3:
        {
          auto ipv4 = mrt::detail::make_ip_v4_v6_parser( [&] { return true; });
          if (!ipv4(t, l, x.next_hop))
            return false;
        }
        break;
      case 4:
        if (!b32be(t, l, x.multi_exit_disc))
          return false;
        break;
      case 5:
        if (!b32be(t, l, x.local_pref))
          return false;
        break;
      case 6:
        x.atomic_aggregate = true;
        break;
      case 7:
        if (!parse_attribute_aggregator(t, l, x.aggregator_as, x.aggregator_ip,
                                       as4))
          return false;
        break;
      case 8:
        if (!parse_attribute_communities(t, l, x.communities, attr_length))
          return false;
        break;
      case 14:
        {
          uint16_t address_family_identifier = 0;
          uint8_t subsequent_address_family_identifier = 0;
          uint8_t next_hop_network_address_length = 0;
          auto mp_reach_nlri_parser = b16be >> byte >> byte;
             if (!mp_reach_nlri_parser(t, l, address_family_identifier,
                                       subsequent_address_family_identifier,
                                       next_hop_network_address_length))
               return false;
          auto mp_nlri_length = attr_length -
                                (5 + next_hop_network_address_length);
          auto mp_next_hop = mrt::detail::make_ip_v4_v6_parser(
            [&] { return address_family_identifier == 1; }
          );
          if (!mp_next_hop(t, l, x.next_hop))
            return false;
          if (address_family_identifier == 1)
            t += (next_hop_network_address_length - 4 + 1);
          else
            t += (next_hop_network_address_length - 16 + 1);
          if (!parse_prefix(t, l, x.mp_reach_nlri, mp_nlri_length,
                            (address_family_identifier == 1)))
            return false;
        }
        break;
      case 15:
        {
          uint16_t address_family_identifier = 0;
          uint8_t subsequent_address_family_identifier = 0;
          auto mp_unreach_nlri_parser = b16be >> byte;
          if (!mp_unreach_nlri_parser(t, l, address_family_identifier,
                                    subsequent_address_family_identifier))
            return false;
          auto mp_nlri_length = attr_length - 3;
          if (!parse_prefix(t, l, x.mp_unreach_nlri, mp_nlri_length,
                            (address_family_identifier == 1)))
            return false;
        }
        break;
      case 16:
        if (!parse_attribute_extended_communities(t, l, x.communities,
                                                  attr_length))
             return false;
           break;
    }
    f += attr_length;
    if (is_extended_length)
      slength -= attr_length + 4;
    else
      slength -= attr_length + 3;
  }
  return true;
}

} // namespace detail

struct message_header_parser : parser<message_header_parser> {
  using attribute = message_header;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, message_header& x) const {
    using namespace parsers;
    auto p = bytes<16> >> b16be >> byte;
    return p(f, l, x.marker, x.length, x.type);
  }
};

struct open_parser : parser<open_parser> {
  using attribute = open;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, open& x) const {
    using namespace parsers;
    auto skip = [&] {
      f += std::min(static_cast<size_t>(l - f),
                    static_cast<size_t>(x.opt_parm_len));
    };
    auto p = (byte >> b16be >> b16be >> b32be >> byte) ->* skip;
    return p(f, l, x.version, x.my_autonomous_system, x.hold_time,
             x.bgp_identifier, x.opt_parm_len);
  }
};

struct update_parser : parser<update_parser> {
  using attribute = update;

  update_parser(const uint16_t& message_length, const bool& as4)
    : message_length_(message_length), as4_(as4) {
    // nop
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, update& x) const {
    using namespace parsers;
    if (!b16be(f, l, x.withdrawn_routes_length))
      return false;
    if (!detail::parse_prefix(f, l, x.withdrawn_routes,
                              x.withdrawn_routes_length, true))
      return false;
    if (!b16be(f, l, x.total_path_attribute_length))
      return false;
    if (!detail::parse_attributes(f, l, x.path_attributes,
                                  x.total_path_attribute_length, as4_))
      return false;
    if (x.total_path_attribute_length > 0) {
      auto network_layer_reachability_information_length = message_length_ -
        23 - x.total_path_attribute_length - x.withdrawn_routes_length;
      if (!detail::parse_prefix(f, l, x.network_layer_reachability_information,
                                network_layer_reachability_information_length,
                                true))
        return false;
    }
    return true;
  }

  const uint16_t& message_length_;
  const bool& as4_;
};

struct notification_parser : parser<notification_parser> {
  using attribute = notification;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, notification& x) const {
    using namespace parsers;
    auto skip = [&] {
      f += (l - f);
    };
    auto p = (byte >> byte) ->* skip;
    return p(f, l, x.error_code, x.error_subcode);
  }
};

struct message_parser : parser<message_parser> {
  using attribute = message;

  message_parser(const bool& as4) : as4_(as4) {
    // nop
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, message& x) const {
    using namespace parsers;
    auto open = open_parser{}.when([&] {
      return x.header.type == OPEN;
    });
    auto update = update_parser{x.header.length, as4_}.when([&] {
      return x.header.type == UPDATE;
    });
    auto notification = notification_parser{}.when([&] {
      return x.header.type == NOTIFICATION;
    });
    auto skip = parsers::eps ->* [&] {
      static auto header_length = 16 + 2 + 1;
      if (x.header.length < header_length || x.header.length > 4096)
        throw std::runtime_error{"cannot parse RFC-violoating records"};
      f += std::min(static_cast<size_t>(l - f),
                    static_cast<size_t>(x.header.length - header_length));
    };
    auto msg = open
             | update
             | notification
             | skip;
    auto p = message_header_parser{} >> msg;
    return p(f, l, x.header, x.message);
  }

  const bool& as4_;
};

} // namespace bgp

namespace table_dump_v2 {

namespace detail {

template <class Iterator>
bool parse_attributes(Iterator& f, const Iterator& l, bgp::attributes& x,
                      uint16_t length, bool afi_ipv4, bool as4) {
  using namespace parsers;
  auto byte_to_16 = byte->*[](uint8_t x) { return uint16_t{x}; };
  int64_t slength = length;
  while (slength > 0) {
    uint8_t attr_flags;
    uint8_t attr_type_code;
    uint16_t attr_length;
    auto attribute_type_parser = byte >> byte;
    if (!attribute_type_parser(f, l, attr_flags, attr_type_code))
      return false;
    bool is_extended_length = ((attr_flags & 16) >> 4) == 1;
    if (is_extended_length) {
      if (!b16be(f, l, attr_length))
        return false;
    } else {
      if (!byte_to_16(f, l, attr_length))
        return false;
    }
    auto t = f;
    switch (attr_type_code) {
      case 1:
        if (!bgp::detail::parse_attribute_origin(t, l, x.origin))
          return false;
        break;
      case 2:
        if(!bgp::detail::parse_attribute_as_path(t, l, x.as_path, as4))
          return false;
        break;
      case 3:
        {
          auto ipv4 = mrt::detail::make_ip_v4_v6_parser( [&] { return true; });
          if (!ipv4(t, l, x.next_hop))
            return false;
        }
        break;
      case 4:
        if (!b32be(t, l, x.multi_exit_disc))
          return false;
        break;
      case 5:
        if (!b32be(t, l, x.local_pref))
          return false;
        break;
      case 6:
        x.atomic_aggregate = true;
        break;
      case 7:
        if (!bgp::detail::parse_attribute_aggregator(t, l, x.aggregator_as,
                                                     x.aggregator_ip, as4))
          return false;
        break;
      case 8:
        if (!bgp::detail::parse_attribute_communities(t, l, x.communities,
                                                      attr_length))
          return false;
        break;
      case 14:
        {
          uint8_t next_hop_network_address_length = 0;
          if (!byte(t, l, next_hop_network_address_length))
            return false;
          auto mp_next_hop = mrt::detail::make_ip_v4_v6_parser(
            [&] { return afi_ipv4; }
          );
          if (!mp_next_hop(t, l, x.next_hop))
            return false;;
        }
        break;
      case 16:
        if (!bgp::detail::parse_attribute_extended_communities(t, l,
                                                               x.communities,
                                                               attr_length))
             return false;
           break;
    }
    f += attr_length;
    if (is_extended_length)
      slength -= attr_length + 4;
    else
      slength -= attr_length + 3;
  }
  return true;
}

} // namespace detail

struct peer_entry_parser : parser<peer_entry_parser> {
  using attribute = peer_entry;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, peer_entry& x) const {
    using namespace parsers;
    auto ip_addr = mrt::detail::make_ip_v4_v6_parser(
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
  bool parse(Iterator& f, const Iterator& l, peer_index_table& x) const {
    using namespace parsers;
    auto view_name = nbytes<char>(x.view_name_length);
    auto peer_entries = rep(peer_entry_parser{}, x.peer_count);
    auto p = b32be >> b16be >> view_name >> b16be >> peer_entries;
    return p(f, l, x.collector_bgp_id, x.view_name_length, x.view_name,
             x.peer_count, x.peer_entries);
  }
};

struct rib_entry_header_parser : parser<rib_entry_header_parser> {
  using attribute = rib_entry_header;

  rib_entry_header_parser(const bool& afi_ipv4) : afi_ipv4_(afi_ipv4) {
    // nop
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, rib_entry_header& x) const {
    using namespace parsers;
    if (!b32be(f, l, x.sequence_number))
      return false;
    if (!bgp::detail::parse_prefix(f, l, x.prefix, 1, afi_ipv4_))
      return false;
    if (!b16be(f, l, x.entry_count))
      return false;
    return true;
  }

  const bool& afi_ipv4_;
};

struct rib_entry_parser : parser<rib_entry_parser> {
  using attribute = rib_entry;

  rib_entry_parser(const bool& afi_ipv4) : afi_ipv4_(afi_ipv4) {
    // nop
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, rib_entry& x) const {
    using namespace parsers;
    auto p = b16be >> b32be >> b16be;
    if (!p(f, l, x.peer_index, x.originated_time, x.attribute_length))
      return false;
    if (!detail::parse_attributes(f, l, x.bgp_attributes, x.attribute_length,
                                  afi_ipv4_, true))
      return false;
    return true;
  }

  const bool& afi_ipv4_;
};

struct rib_afi_safi_parser : parser<rib_afi_safi_parser> {
  using attribute = rib_afi_safi;

  rib_afi_safi_parser(const bool& afi_ipv4) : afi_ipv4_(afi_ipv4) {
    // nop
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, rib_afi_safi& x) const {
    using namespace parsers;
    auto rib_entries = rep(rib_entry_parser{afi_ipv4_}, x.header.entry_count);
    auto p = rib_entry_header_parser{afi_ipv4_} >> rib_entries;
    return p(f, l, x.header, x.entries);
  }

  const bool& afi_ipv4_;
};

} // namespace table_dump_v2

namespace bgp4mp {

struct state_change_parser : parser<state_change_parser> {
  using attribute = state_change;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, state_change& x) const {
    using namespace parsers;
    auto ip_addr = detail::make_ip_v4_v6_parser(
      [&] { return x.address_family == 1; }
    );
    auto p = b16be >> b16be >> b16be >> b16be >> ip_addr >> ip_addr >> b16be >>
             b16be;
    return p(f, l, x.peer_as_number, x.local_as_number, x.interface_index,
             x.address_family, x.peer_ip_address, x.local_ip_address,
             x.old_state, x.new_state);
  };
};

struct message_parser : parser<message_parser> {
  using attribute = message;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, message& x) const {
    using namespace parsers;
    auto ip_addr = detail::make_ip_v4_v6_parser(
      [&] { return x.address_family == 1; }
    );
    auto msg = bgp::message_parser{false};
    auto p = b16be >> b16be >> b16be >> b16be >> ip_addr >> ip_addr >> msg;
    return p(f, l, x.peer_as_number, x.local_as_number, x.interface_index,
             x.address_family, x.peer_ip_address, x.local_ip_address,
             x.message);
  };
};

struct message_as4_parser : parser<message_as4_parser> {
  using attribute = message_as4;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, message_as4& x) const {
    using namespace parsers;
    auto ip_addr = detail::make_ip_v4_v6_parser(
      [&] { return x.address_family == 1; }
    );
    auto msg = bgp::message_parser{true};
    auto p = b32be >> b32be >> b16be >> b16be >> ip_addr >> ip_addr >> msg;
    return p(f, l, x.peer_as_number, x.local_as_number, x.interface_index,
             x.address_family, x.peer_ip_address, x.local_ip_address,
             x.message);
  };
};

struct state_change_as4_parser : parser<state_change_as4_parser> {
  using attribute = state_change_as4;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, state_change_as4& x) const {
    using namespace parsers;
    auto ip_addr = detail::make_ip_v4_v6_parser(
      [&] { return x.address_family == 1; }
    );
    auto p = b32be >> b32be >> b16be >> b16be >> ip_addr >> ip_addr >> b16be >>
             b16be;
    return p(f, l, x.peer_as_number, x.local_as_number, x.interface_index,
             x.address_family, x.peer_ip_address, x.local_ip_address,
             x.old_state, x.new_state);
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
    table_dump_v2::rib_afi_safi,
    bgp4mp::state_change,
    bgp4mp::message,
    bgp4mp::message_as4,
    bgp4mp::state_change_as4
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
    auto rib_ipv4 = table_dump_v2::rib_afi_safi_parser{true}.when([&] {
      return x.header.type == TABLE_DUMP_V2
        && ( x.header.subtype == table_dump_v2::RIB_IPV4_UNICAST
          || x.header.subtype == table_dump_v2::RIB_IPV4_MULTICAST);
    });
    auto rib_ipv6 = table_dump_v2::rib_afi_safi_parser{false}.when([&] {
      return x.header.type == TABLE_DUMP_V2
        && ( x.header.subtype == table_dump_v2::RIB_IPV6_UNICAST
          || x.header.subtype == table_dump_v2::RIB_IPV6_MULTICAST);
    });
    auto state_change = bgp4mp::state_change_parser{}.when([&] {
      return x.header.type == BGP4MP
        && x.header.subtype == bgp4mp::STATE_CHANGE;
    });
    auto message = bgp4mp::message_parser{}.when([&] {
      return x.header.type == BGP4MP
        && x.header.subtype == bgp4mp::MESSAGE;
    });
    auto message_as4 = bgp4mp::message_as4_parser{}.when([&] {
      return x.header.type == BGP4MP
        && x.header.subtype == bgp4mp::MESSAGE_AS4;
    });
    auto state_change_as4 = bgp4mp::state_change_as4_parser{}.when([&] {
      return x.header.type == BGP4MP
        && x.header.subtype == bgp4mp::STATE_CHANGE_AS4;
    });
    auto skip = parsers::eps ->* [&] {
      f += std::min(static_cast<size_t>(l - f),
                    static_cast<size_t>(x.header.length));
    };
    auto msg = peer_index_table
             | rib_ipv4
             | rib_ipv6
             | state_change
             | message
             | message_as4
             | state_change_as4
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
  std::queue<event> events_;
  record_parser parser_;
};

} // namespace mrt
} // namespace format
} // namespace vast

#endif
