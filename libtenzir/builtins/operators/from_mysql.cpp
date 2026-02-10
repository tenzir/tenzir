//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/any.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/tls.hpp>
#include <tenzir/concept/parseable/numeric/real.hpp>
#include <tenzir/detail/byteswap.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <folly/executors/GlobalExecutor.h>
#include <folly/coro/Sleep.h>
#include <folly/io/coro/Transport.h>
#include <openssl/sha.h>

#include <charconv>
#include <cstring>
#include <limits>
#include <utility>

namespace tenzir::plugins::mysql {

namespace {

// -- MySQL Wire Protocol Overview --------------------------------------------
//
//   Client                                Server
//     |                                     |
//     |          TCP Connect                |
//     |------------------------------------>|
//     |                                     |
//     |        Handshake (v10)              |
//     |<------------------------------------|
//     |  protocol version, server version,  |
//     |  auth plugin data (scramble),       |
//     |  capabilities, auth plugin name     |
//     |                                     |
//     |  +--- If TLS requested ---------------+
//     |  |    SSL Request (caps + padding)  |
//     |  |  ------>                         |
//     |  |    TLS Handshake                 |
//     |  |  <=====>                         |
//     |  +----------------------------------+
//     |                                     |
//     |        Auth Response                |
//     |------------------------------------>|
//     |  capabilities, user, auth data,     |
//     |  database, auth plugin name         |
//     |                                     |
//     |  +--- OK ----------------------------+ auth succeeded
//     |  |                                  |
//     |  +--- ERR ---------------------------+ auth failed
//     |  |                                  |
//     |  +--- AuthMoreData (0x01) ----------+ caching_sha2_password
//     |  |    0x03: fast auth (cached)      |
//     |  |      +--- OK --------------------|   done
//     |  |    0x04: full auth required      |
//     |  |      |   Cleartext Password      |
//     |  |      |-------------------------->|
//     |  |      +--- OK / ERR --------------|   done
//     |  |                                  |
//     |  +--- AuthSwitch (0xFE) ------------+ different plugin
//     |  |    new plugin + scramble           |
//     |  |      Auth Data (re-hashed)         |
//     |  |      |-------------------------->|
//     |  |      +--- OK / ERR / MoreData ---|   done
//     |                                     |
//     |        COM_QUERY (0x03)             |
//     |------------------------------------>|
//     |  SQL query string                   |
//     |                                     |
//     |        Column Count                 |
//     |<------------------------------------|
//     |  length-encoded integer             |
//     |                                     |
//     |        Column Definition * N        |
//     |<------------------------------------|
//     |  catalog, schema, table, name,      |
//     |  charset, length, type, flags       |
//     |                                     |
//     |        EOF                          |
//     |<------------------------------------|
//     |                                     |
//     |        Row Data * M                 |
//     |<------------------------------------|
//     |  length-encoded strings per column  |
//     |  (0xFB = NULL)                      |
//     |                                     |
//     |        EOF                          |
//     |<------------------------------------|
//     |                                     |
//
// Packet framing: every packet has a 4-byte header (3 bytes payload length +
// 1 byte sequence id) followed by the payload.
//
// References:
//   https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basics.html
//   https://mariadb.com/kb/en/clientserver-protocol/

// -- Protocol Types ----------------------------------------------------------

/// MySQL column types.
enum class mysql_type : uint8_t {
  decimal = 0,
  tiny = 1,
  short_ = 2,
  long_ = 3,
  float_ = 4,
  double_ = 5,
  null = 6,
  timestamp = 7,
  longlong = 8,
  int24 = 9,
  date = 10,
  time = 11,
  datetime = 12,
  year = 13,
  newdate = 14,
  varchar = 15,
  bit = 16,
  timestamp2 = 17,
  datetime2 = 18,
  time2 = 19,
  json = 245,
  newdecimal = 246,
  enum_ = 247,
  set = 248,
  tiny_blob = 249,
  medium_blob = 250,
  long_blob = 251,
  blob = 252,
  var_string = 253,
  string = 254,
  geometry = 255,
};

/// Column flags.
enum class column_flag : uint16_t {
  not_null = 1 << 0,
  primary_key = 1 << 1,
  unique_key = 1 << 2,
  multiple_key = 1 << 3,
  blob = 1 << 4,
  unsigned_ = 1 << 5,
  zerofill = 1 << 6,
  binary = 1 << 7,
  enum_ = 1 << 8,
  auto_increment = 1 << 9,
  timestamp = 1 << 10,
  set = 1 << 11,
  no_default_value = 1 << 12,
  on_update_now = 1 << 13,
  num = 1 << 15,
};

/// Information about a column in a result set.
struct column_info {
  std::string catalog;
  std::string schema;
  std::string table;
  std::string org_table;
  std::string name;
  std::string org_name;
  uint16_t charset = 0;
  uint32_t length = 0;
  mysql_type type = mysql_type::null;
  uint16_t flags = 0;
  uint8_t decimals = 0;
};

/// MySQL-specific error type.
struct mysql_error {
  uint16_t code = 0;
  std::string message;
  static auto protocol(std::string msg) -> mysql_error {
    return {0, std::move(msg)};
  }
};

/// Convenience alias for Result with mysql_error.
template <class T>
using MysqlResult = Result<T, mysql_error>;

auto is_unsigned(column_info const& col) -> bool {
  return (col.flags & std::to_underlying(column_flag::unsigned_)) != 0;
}

auto is_binary(column_info const& col) -> bool {
  return (col.flags & std::to_underlying(column_flag::binary)) != 0;
}

/// MySQL capability flags.
enum class capability : uint32_t {
  long_password = 1 << 0,
  found_rows = 1 << 1,
  long_flag = 1 << 2,
  connect_with_db = 1 << 3,
  no_schema = 1 << 4,
  compress = 1 << 5,
  odbc = 1 << 6,
  local_files = 1 << 7,
  ignore_space = 1 << 8,
  protocol_41 = 1 << 9,
  interactive = 1 << 10,
  ssl = 1 << 11,
  ignore_sigpipe = 1 << 12,
  transactions = 1 << 13,
  reserved = 1 << 14,
  secure_connection = 1 << 15,
  multi_statements = 1 << 16,
  multi_results = 1 << 17,
  ps_multi_results = 1 << 18,
  plugin_auth = 1 << 19,
  connect_attrs = 1 << 20,
  plugin_auth_lenenc_client_data = 1 << 21,
  can_handle_expired_passwords = 1 << 22,
  session_track = 1 << 23,
  deprecate_eof = 1 << 24,
};

auto has_cap(uint32_t caps, capability c) -> bool {
  return (caps & std::to_underlying(c)) != 0;
}

/// MySQL server handshake information.
struct handshake_info {
  uint8_t protocol_version = 0;
  std::string server_version;
  uint32_t connection_id = 0;
  std::vector<std::byte> auth_plugin_data;
  uint32_t capabilities = 0;
  uint8_t charset = 0;
  uint16_t status_flags = 0;
  std::string auth_plugin_name;
};

// -- Wire Protocol Constants -------------------------------------------------

/// MySQL wire protocol packet markers.
namespace packet {
inline constexpr auto ok = std::byte{0x00};
inline constexpr auto auth_more_data = std::byte{0x01};
inline constexpr auto auth_switch = std::byte{0xfe};
inline constexpr auto eof = std::byte{0xfe};
inline constexpr auto error = std::byte{0xff};
inline constexpr auto com_query = std::byte{0x03};
} // namespace packet

/// caching_sha2_password status bytes.
namespace caching_sha2 {
inline constexpr auto fast_auth_success = std::byte{0x03};
inline constexpr auto full_auth_required = std::byte{0x04};
} // namespace caching_sha2

/// Length-encoded integer sentinel bytes.
namespace lenenc {
inline constexpr auto null_marker = std::byte{0xfb};
inline constexpr auto two_byte = std::byte{0xfc};
inline constexpr auto three_byte = std::byte{0xfd};
inline constexpr auto eight_byte = std::byte{0xfe};
} // namespace lenenc

/// Handshake magic numbers.
inline constexpr auto protocol_version_10 = std::byte{10};
inline constexpr auto charset_utf8mb4 = uint8_t{255};
inline constexpr uint32_t max_packet_size = 0xffffff;
inline constexpr size_t auth_data_part1_len = 8;
inline constexpr size_t client_reserved_bytes = 23;
inline constexpr size_t handshake_reserved = 10;

// -- Wire Protocol Helpers ---------------------------------------------------

/// Reads a little-endian integer from a byte buffer.
template <class T>
auto read_int_le(std::span<std::byte const> data) -> T {
  auto value = T{};
  std::memcpy(&value, data.data(), sizeof(T));
  return detail::swap<std::endian::little, std::endian::native>(value);
}

/// Writes a little-endian integer to a byte buffer.
template <class T>
void write_int_le(std::vector<std::byte>& buffer, T value) {
  value = detail::swap<std::endian::native, std::endian::little>(value);
  auto bytes = as_bytes(value);
  buffer.insert(buffer.end(), bytes.begin(), bytes.end());
}

/// Writes a null-terminated string to a byte buffer.
void write_null_string(std::vector<std::byte>& buffer, std::string_view str) {
  auto bytes = as_bytes(str);
  buffer.insert(buffer.end(), bytes.begin(), bytes.end());
  buffer.push_back(std::byte{0});
}

/// Writes a length-encoded integer to a byte buffer.
void write_lenenc_int(std::vector<std::byte>& buffer, uint64_t value) {
  if (value < 251) {
    buffer.push_back(static_cast<std::byte>(value));
  } else if (value < 65536) {
    buffer.push_back(lenenc::two_byte);
    write_int_le<uint16_t>(buffer, static_cast<uint16_t>(value));
  } else if (value < 16777216) {
    buffer.push_back(lenenc::three_byte);
    buffer.push_back(static_cast<std::byte>(value));
    buffer.push_back(static_cast<std::byte>(value >> 8));
    buffer.push_back(static_cast<std::byte>(value >> 16));
  } else {
    buffer.push_back(lenenc::eight_byte);
    write_int_le<uint64_t>(buffer, value);
  }
}

/// Lightweight reader over a span of bytes, eliminating manual offset tracking.
class packet_reader {
public:
  explicit packet_reader(std::span<std::byte const> data) : data_{data} {
  }

  template <class T>
  auto read_int() -> T {
    auto result = read_int_le<T>(data_.subspan(offset_));
    offset_ += sizeof(T);
    return result;
  }

  auto read_lenenc_int() -> MysqlResult<uint64_t> {
    if (offset_ >= data_.size()) {
      return Err{mysql_error::protocol("empty data for lenenc int")};
    }
    auto first = data_[offset_];
    if (first < lenenc::null_marker) {
      offset_ += 1;
      return uint64_t{std::to_integer<uint8_t>(first)};
    }
    if (first == lenenc::two_byte) {
      if (data_.size() < offset_ + 3) {
        return Err{mysql_error::protocol("truncated 2-byte lenenc int")};
      }
      offset_ += 1;
      auto val = read_int_le<uint16_t>(data_.subspan(offset_));
      offset_ += 2;
      return uint64_t{val};
    }
    if (first == lenenc::three_byte) {
      if (data_.size() < offset_ + 4) {
        return Err{mysql_error::protocol("truncated 3-byte lenenc int")};
      }
      offset_ += 1;
      auto b = data_.subspan(offset_);
      auto val = uint32_t{std::to_integer<uint8_t>(b[0])}
                 | (uint32_t{std::to_integer<uint8_t>(b[1])} << 8)
                 | (uint32_t{std::to_integer<uint8_t>(b[2])} << 16);
      offset_ += 3;
      return uint64_t{val};
    }
    if (first == lenenc::eight_byte) {
      if (data_.size() < offset_ + 9) {
        return Err{mysql_error::protocol("truncated 8-byte lenenc int")};
      }
      offset_ += 1;
      auto val = read_int_le<uint64_t>(data_.subspan(offset_));
      offset_ += 8;
      return uint64_t{val};
    }
    // 0xfb = NULL marker, 0xff = error marker.
    offset_ += 1;
    return uint64_t{0};
  }

  auto read_lenenc_string() -> MysqlResult<std::string> {
    TRY(auto length, read_lenenc_int());
    if (data_.size() < offset_ + length) {
      return Err{mysql_error::protocol("truncated lenenc string")};
    }
    auto str_data = data_.subspan(offset_, length);
    offset_ += length;
    return std::string(reinterpret_cast<char const*>(str_data.data()), length);
  }

  auto read_null_string() -> std::string {
    auto remaining = data_.subspan(offset_);
    auto it = std::ranges::find(remaining, std::byte{0});
    if (it == remaining.end()) {
      return "";
    }
    auto length = static_cast<size_t>(std::distance(remaining.begin(), it));
    auto result
      = std::string(reinterpret_cast<char const*>(remaining.data()), length);
    offset_ += length + 1;
    return result;
  }

  auto read_bytes(size_t n) -> std::span<std::byte const> {
    auto result = data_.subspan(offset_, n);
    offset_ += n;
    return result;
  }

  void skip(size_t n) {
    offset_ += n;
  }

  auto remaining() const -> size_t {
    return offset_ < data_.size() ? data_.size() - offset_ : 0;
  }

  auto at_end() const -> bool {
    return offset_ >= data_.size();
  }

  auto current_byte() const -> std::byte {
    return data_[offset_];
  }

  auto data() const -> std::span<std::byte const> {
    return data_.subspan(offset_);
  }

private:
  std::span<std::byte const> data_;
  size_t offset_ = 0;
};

/// Combine order for the auth hash computation.
enum class auth_combine_order {
  scramble_first, // mysql_native_password: hash(scramble + hash2)
  hash_first,     // caching_sha2_password: hash(hash2 + scramble)
};

/// Computes a MySQL auth hash: hash(password) XOR hash(combined).
/// The combine order differs between mysql_native_password and
/// caching_sha2_password.
template <size_t DigestLen, class HashFn>
auto compute_auth_hash(HashFn hash_fn, std::string_view password,
                       std::span<std::byte const> scramble,
                       auth_combine_order order) -> std::vector<std::byte> {
  if (password.empty()) {
    return {};
  }
  auto pw_bytes = as_bytes(password);
  // hash(password).
  auto hash1 = std::array<uint8_t, DigestLen>{};
  hash_fn(reinterpret_cast<uint8_t const*>(pw_bytes.data()), pw_bytes.size(),
          hash1.data());
  // hash(hash(password)).
  auto hash2 = std::array<uint8_t, DigestLen>{};
  hash_fn(hash1.data(), hash1.size(), hash2.data());
  // Combine scramble and hash2 in the appropriate order.
  auto combined = std::vector<uint8_t>{};
  combined.reserve(scramble.size() + hash2.size());
  auto scramble_u8 = std::span{
    reinterpret_cast<uint8_t const*>(scramble.data()), scramble.size()};
  if (order == auth_combine_order::scramble_first) {
    combined.insert(combined.end(), scramble_u8.begin(), scramble_u8.end());
    combined.insert(combined.end(), hash2.begin(), hash2.end());
  } else {
    combined.insert(combined.end(), hash2.begin(), hash2.end());
    combined.insert(combined.end(), scramble_u8.begin(), scramble_u8.end());
  }
  auto hash3 = std::array<uint8_t, DigestLen>{};
  hash_fn(combined.data(), combined.size(), hash3.data());
  // XOR hash(password) with result.
  auto result = std::vector<std::byte>(DigestLen);
  for (auto i = size_t{0}; i < DigestLen; ++i) {
    result[i] = static_cast<std::byte>(hash1[i] ^ hash3[i]);
  }
  return result;
}

/// Computes the MySQL native password authentication response.
auto compute_native_password(std::string_view password,
                             std::span<std::byte const> scramble)
  -> std::vector<std::byte> {
  return compute_auth_hash<SHA_DIGEST_LENGTH>(
    SHA1, password, scramble, auth_combine_order::scramble_first);
}

/// Computes the MySQL caching_sha2_password authentication response.
auto compute_caching_sha2_password(std::string_view password,
                                   std::span<std::byte const> scramble)
  -> std::vector<std::byte> {
  return compute_auth_hash<SHA256_DIGEST_LENGTH>(
    SHA256, password, scramble, auth_combine_order::hash_first);
}

/// Wraps an identifier with backticks, escaping embedded backticks.
auto quote_identifier(std::string_view name) -> std::string {
  auto result = std::string{"`"};
  for (auto c : name) {
    if (c == '`') {
      result += "``";
    } else {
      result += c;
    }
  }
  result += '`';
  return result;
}

/// Wraps a SQL string literal with single quotes and escapes embedded quotes.
auto quote_string_literal(std::string_view value) -> std::string {
  auto result = std::string{"'"};
  for (auto c : value) {
    if (c == '\'') {
      result += "''";
    } else {
      result += c;
    }
  }
  result += '\'';
  return result;
}

/// Parses an error packet into a mysql_error.
auto parse_error_from_packet(std::span<std::byte const> data) -> mysql_error {
  TENZIR_ASSERT(not data.empty() and data[0] == packet::error);
  if (data.size() < 3) {
    return mysql_error::protocol("error packet too short");
  }
  auto reader = packet_reader{data};
  reader.skip(1); // Error marker.
  auto code = reader.read_int<uint16_t>();
  // Skip SQL state marker.
  if (not reader.at_end()
      and reader.current_byte() == static_cast<std::byte>('#')) {
    reader.skip(1);
    if (reader.remaining() >= 5) {
      reader.skip(5);
    }
  }
  auto msg = std::string{};
  if (not reader.at_end()) {
    auto rest = reader.data();
    msg = std::string(reinterpret_cast<char const*>(rest.data()), rest.size());
  }
  return {code, std::move(msg)};
}

/// Emits a mysql_error as a diagnostic.
void emit_mysql_error(mysql_error const& err, diagnostic_handler& dh) {
  if (err.code != 0) {
    diagnostic::error("MySQL error {}: {}", err.code, err.message).emit(dh);
  } else {
    diagnostic::error("MySQL error: {}", err.message).emit(dh);
  }
}

/// Parses a column definition packet.
auto parse_column_definition(std::span<std::byte const> data)
  -> MysqlResult<column_info> {
  auto col = column_info{};
  auto reader = packet_reader{data};
  TRY(col.catalog, reader.read_lenenc_string());
  TRY(col.schema, reader.read_lenenc_string());
  TRY(col.table, reader.read_lenenc_string());
  TRY(col.org_table, reader.read_lenenc_string());
  TRY(col.name, reader.read_lenenc_string());
  TRY(col.org_name, reader.read_lenenc_string());
  // Fixed-length fields after 0x0c length marker.
  if (reader.remaining() < 1) {
    return Err{mysql_error::protocol("column definition truncated")};
  }
  TRY(auto filler, reader.read_lenenc_int());
  TENZIR_UNUSED(filler);
  if (reader.remaining() < 12) {
    return Err{mysql_error::protocol("column definition truncated")};
  }
  col.charset = reader.read_int<uint16_t>();
  col.length = reader.read_int<uint32_t>();
  col.type = static_cast<mysql_type>(
    std::to_integer<uint8_t>(reader.read_bytes(1)[0]));
  col.flags = reader.read_int<uint16_t>();
  col.decimals = std::to_integer<uint8_t>(reader.read_bytes(1)[0]);
  return col;
}

/// Build a COM_QUERY packet.
auto make_query_packet(std::string_view sql) -> std::vector<std::byte> {
  auto pkt = std::vector<std::byte>{};
  pkt.reserve(1 + sql.size());
  pkt.push_back(packet::com_query);
  auto bytes = as_bytes(sql);
  pkt.insert(pkt.end(), bytes.begin(), bytes.end());
  return pkt;
}

/// Check if packet indicates end-of-result-set.
auto is_eof(std::span<std::byte const> pkt) -> bool {
  return not pkt.empty() and pkt[0] == packet::eof and pkt.size() < 9;
}

/// Check if packet is an error packet. Returns the error if so.
auto as_error(std::span<std::byte const> data) -> std::optional<mysql_error> {
  if (not data.empty() and data[0] == packet::error) {
    return parse_error_from_packet(data);
  }
  return std::nullopt;
}

/// Returns the base set of client capability flags shared by SSL request and
/// auth response packets.
auto base_client_capabilities() -> uint32_t {
  return std::to_underlying(capability::long_password)
         | std::to_underlying(capability::protocol_41)
         | std::to_underlying(capability::secure_connection)
         | std::to_underlying(capability::plugin_auth)
         | std::to_underlying(capability::plugin_auth_lenenc_client_data);
}

// -- Async Connection --------------------------------------------------------

/// Async MySQL connection using folly::coro::Transport.
///
/// All Transport operations are scheduled on the EventBase thread via
/// scheduleOn() to ensure AsyncSocket callbacks fire correctly.
class async_connection {
public:
  async_connection() = default;

  /// Connect to MySQL server asynchronously.
  static auto
  connect(folly::EventBase* evb, std::string const& host, uint16_t port,
          std::chrono::milliseconds timeout = std::chrono::seconds{30})
    -> Task<async_connection> {
    auto addr = folly::SocketAddress{host, port};
    auto transport = co_await co_withExecutor(
      evb, folly::coro::Transport::newConnectedSocket(evb, addr, timeout));
    co_return async_connection{std::move(transport), evb};
  }

  /// Read a MySQL packet asynchronously.
  auto read_packet() -> Task<MysqlResult<std::vector<std::byte>>> {
    auto payload = std::vector<std::byte>{};
    while (true) {
      // Read 4-byte header: 3 bytes length + 1 byte sequence.
      auto header = std::array<unsigned char, 4>{};
      CO_TRY(co_await read_exact(
        folly::MutableByteRange{header.data(), header.size()}, "header"));
      auto len = uint32_t{header[0]} | (uint32_t{header[1]} << 8)
                 | (uint32_t{header[2]} << 16);
      sequence_id_ = header[3];
      // MySQL can split logical packets across continuation frames of
      // exactly max_packet_size bytes.
      auto offset = payload.size();
      payload.resize(payload.size() + len);
      if (len > 0) {
        CO_TRY(co_await read_exact(
          folly::MutableByteRange{
            reinterpret_cast<unsigned char*>(payload.data()) + offset, len},
          "payload"));
      }
      if (len < max_packet_size) {
        break;
      }
    }
    co_return payload;
  }

  /// Write a MySQL packet asynchronously.
  auto write_packet(std::span<std::byte const> payload, uint8_t seq)
    -> Task<MysqlResult<void>> {
    auto pkt = std::vector<unsigned char>(4 + payload.size());
    pkt[0] = static_cast<unsigned char>(payload.size() & 0xFF);
    pkt[1] = static_cast<unsigned char>((payload.size() >> 8) & 0xFF);
    pkt[2] = static_cast<unsigned char>((payload.size() >> 16) & 0xFF);
    pkt[3] = seq;
    std::memcpy(pkt.data() + 4, payload.data(), payload.size());
    co_await co_withExecutor(
      evb_, transport_->write(folly::ByteRange{pkt.data(), pkt.size()}));
    sequence_id_ = seq;
    co_return {};
  }

  /// Perform the initial handshake with the server.
  auto perform_handshake() -> Task<MysqlResult<handshake_info>> {
    CO_TRY(auto data, co_await read_packet());
    if (data.empty()) {
      co_return Err{mysql_error::protocol("empty handshake packet")};
    }
    // Check for error packet.
    if (data[0] == packet::error) {
      co_return Err{parse_error_from_packet(data)};
    }
    auto info = handshake_info{};
    auto reader = packet_reader{data};
    // Protocol version.
    info.protocol_version = std::to_integer<uint8_t>(reader.read_bytes(1)[0]);
    if (info.protocol_version
        != std::to_integer<uint8_t>(protocol_version_10)) {
      co_return Err{mysql_error::protocol(fmt::format(
        "unsupported protocol version: {}", info.protocol_version))};
    }
    // Server version (null-terminated).
    info.server_version = reader.read_null_string();
    // Connection ID.
    info.connection_id = reader.read_int<uint32_t>();
    // Auth plugin data part 1 (8 bytes).
    auto part1 = reader.read_bytes(auth_data_part1_len);
    info.auth_plugin_data.insert(info.auth_plugin_data.end(), part1.begin(),
                                 part1.end());
    // Filler.
    reader.skip(1);
    // Capability flags (lower 2 bytes).
    info.capabilities = reader.read_int<uint16_t>();
    if (not reader.at_end()) {
      // Character set.
      info.charset = std::to_integer<uint8_t>(reader.read_bytes(1)[0]);
      // Status flags.
      info.status_flags = reader.read_int<uint16_t>();
      // Capability flags (upper 2 bytes).
      info.capabilities
        |= static_cast<uint32_t>(reader.read_int<uint16_t>()) << 16;
      // Auth plugin data length.
      auto auth_data_len = uint8_t{0};
      if (has_cap(info.capabilities, capability::plugin_auth)) {
        auth_data_len = std::to_integer<uint8_t>(reader.read_bytes(1)[0]);
      } else {
        reader.skip(1);
      }
      // Reserved (10 bytes).
      reader.skip(handshake_reserved);
      // Auth plugin data part 2.
      if (has_cap(info.capabilities, capability::secure_connection)) {
        auto part2_len
          = static_cast<size_t>(std::max(13, int(auth_data_len) - 8));
        if (reader.remaining() >= part2_len) {
          auto part2 = reader.read_bytes(part2_len);
          // Don't include the trailing null byte.
          auto end = part2.end();
          while (end > part2.begin() and *(end - 1) == std::byte{0}) {
            --end;
          }
          info.auth_plugin_data.insert(info.auth_plugin_data.end(),
                                       part2.begin(), end);
        }
      }
      // Auth plugin name.
      if (has_cap(info.capabilities, capability::plugin_auth)) {
        info.auth_plugin_name = reader.read_null_string();
      }
    }
    co_return info;
  }

  /// Send the SSL request packet (capabilities + padding, no auth data).
  /// This tells the server we want to upgrade to TLS before authenticating.
  auto send_ssl_request(bool has_database) -> Task<MysqlResult<void>> {
    auto buf = std::vector<std::byte>{};
    buf.reserve(32);
    auto caps
      = base_client_capabilities() | std::to_underlying(capability::ssl);
    if (has_database) {
      caps |= std::to_underlying(capability::connect_with_db);
    }
    write_int_le<uint32_t>(buf, caps);
    write_int_le<uint32_t>(buf, max_packet_size);
    buf.push_back(static_cast<std::byte>(charset_utf8mb4));
    buf.insert(buf.end(), client_reserved_bytes, std::byte{0});
    CO_TRY(co_await write_packet(buf, 1));
    co_return {};
  }

  /// Upgrade the connection to TLS.
  auto upgrade_to_tls(std::shared_ptr<folly::SSLContext> ctx,
                      std::string const& hostname) -> Task<MysqlResult<void>> {
    try {
      co_await upgrade_transport_to_tls_client(transport_, std::move(ctx),
                                               std::string{hostname});
    } catch (std::exception const& ex) {
      co_return Err{
        mysql_error{0, fmt::format("TLS handshake failed: {}", ex.what())}};
    }
    is_tls_ = true;
    co_return {};
  }

  /// Handle caching_sha2_password AuthMoreData sub-protocol.
  /// Handles both fast auth (0x03) and full auth (0x04) responses.
  auto handle_caching_sha2_more_data(std::span<std::byte const> response,
                                     std::string const& password)
    -> Task<MysqlResult<void>> {
    TENZIR_ASSERT(response.size() > 1);
    // 0x03 = fast auth success (cached), read the final OK.
    if (response[1] == caching_sha2::fast_auth_success) {
      CO_TRY(auto final_resp, co_await read_packet());
      if (final_resp[0] == packet::ok) {
        co_return {};
      }
      co_return Err{
        mysql_error::protocol("unexpected response after fast auth")};
    }
    // 0x04 = full auth required. Send password as null-terminated cleartext.
    if (response[1] == caching_sha2::full_auth_required) {
      if (not is_tls_) {
        co_return Err{mysql_error::protocol("full auth requires TLS (RSA "
                                            "encryption not implemented)")};
      }
      auto pw_bytes = as_bytes(password);
      auto pw_packet = std::vector<std::byte>{};
      pw_packet.reserve(pw_bytes.size() + 1);
      pw_packet.insert(pw_packet.end(), pw_bytes.begin(), pw_bytes.end());
      pw_packet.push_back(std::byte{0});
      CO_TRY(co_await write_packet(pw_packet, sequence_id_ + 1));
      CO_TRY(auto final_resp, co_await read_packet());
      if (final_resp[0] == packet::ok) {
        co_return {};
      }
      if (final_resp[0] == packet::error) {
        co_return Err{parse_error_from_packet(final_resp)};
      }
      co_return Err{
        mysql_error::protocol("unexpected response after full auth")};
    }
    co_return Err{mysql_error::protocol(
      fmt::format("unexpected caching_sha2 status: 0x{:02x}",
                  std::to_integer<uint8_t>(response[1])))};
  }

  /// Send the authentication response.
  /// Uses the internally tracked sequence number and TLS state.
  auto
  send_auth_response(handshake_info const& handshake, std::string const& user,
                     std::string const& password, std::string const& database)
    -> Task<MysqlResult<void>> {
    auto buf = std::vector<std::byte>{};
    buf.reserve(32 + user.size() + 1 + 32 + database.size() + 1
                + handshake.auth_plugin_name.size() + 1);
    // Client capabilities.
    auto caps = base_client_capabilities();
    if (not database.empty()) {
      caps |= std::to_underlying(capability::connect_with_db);
    }
    if (is_tls_) {
      caps |= std::to_underlying(capability::ssl);
    }
    write_int_le<uint32_t>(buf, caps);
    // Max packet size.
    write_int_le<uint32_t>(buf, max_packet_size);
    // Character set (utf8mb4 = 255).
    buf.push_back(static_cast<std::byte>(charset_utf8mb4));
    // Reserved (23 bytes).
    buf.insert(buf.end(), client_reserved_bytes, std::byte{0});
    // Username (null-terminated).
    write_null_string(buf, user);
    // Auth response.
    auto auth_response = std::vector<std::byte>{};
    if (handshake.auth_plugin_name == "mysql_native_password") {
      auth_response
        = compute_native_password(password, handshake.auth_plugin_data);
    } else if (handshake.auth_plugin_name == "caching_sha2_password") {
      auth_response
        = compute_caching_sha2_password(password, handshake.auth_plugin_data);
    }
    write_lenenc_int(buf, auth_response.size());
    buf.insert(buf.end(), auth_response.begin(), auth_response.end());
    // Database (if specified).
    if (not database.empty()) {
      write_null_string(buf, database);
    }
    // Auth plugin name.
    write_null_string(buf, handshake.auth_plugin_name);
    // Send packet with the next sequence number.
    CO_TRY(co_await write_packet(buf, sequence_id_ + 1));
    // Read response.
    CO_TRY(auto resp_data, co_await read_packet());
    if (resp_data.empty()) {
      co_return Err{mysql_error::protocol("empty auth response")};
    }
    // OK packet — authentication successful.
    if (resp_data[0] == packet::ok) {
      co_return {};
    }
    // Error packet.
    if (resp_data[0] == packet::error) {
      co_return Err{parse_error_from_packet(resp_data)};
    }
    // Auth switch request: server wants a different auth plugin.
    if (resp_data[0] == packet::auth_switch) {
      auto reader = packet_reader{resp_data};
      reader.skip(1); // Skip 0xFE marker.
      auto plugin_name = reader.read_null_string();
      auto new_scramble = std::vector<std::byte>{};
      if (reader.remaining() > 0) {
        auto rest = reader.data();
        new_scramble.assign(rest.begin(), rest.end());
        // Strip trailing null byte if present.
        if (not new_scramble.empty() and new_scramble.back() == std::byte{0}) {
          new_scramble.pop_back();
        }
      }
      // Compute auth response for the requested plugin.
      auto switch_auth = std::vector<std::byte>{};
      if (plugin_name == "mysql_native_password") {
        switch_auth = compute_native_password(password, new_scramble);
      } else if (plugin_name == "caching_sha2_password") {
        switch_auth = compute_caching_sha2_password(password, new_scramble);
      } else {
        co_return Err{mysql_error::protocol(
          fmt::format("unsupported auth plugin: {}", plugin_name))};
      }
      CO_TRY(co_await write_packet(switch_auth, sequence_id_ + 1));
      CO_TRY(auto switch_resp, co_await read_packet());
      if (switch_resp.empty()) {
        co_return Err{mysql_error::protocol("empty auth switch response")};
      }
      if (switch_resp[0] == packet::ok) {
        co_return {};
      }
      if (switch_resp[0] == packet::error) {
        co_return Err{parse_error_from_packet(switch_resp)};
      }
      // caching_sha2_password may send AuthMoreData after switch.
      if (switch_resp[0] == packet::auth_more_data and switch_resp.size() > 1) {
        CO_TRY(co_await handle_caching_sha2_more_data(switch_resp, password));
        co_return {};
      }
      co_return Err{mysql_error::protocol(
        fmt::format("unexpected auth switch response: 0x{:02x}",
                    std::to_integer<uint8_t>(switch_resp[0])))};
    }
    // caching_sha2_password more-data response.
    if (resp_data[0] == packet::auth_more_data and resp_data.size() > 1) {
      CO_TRY(co_await handle_caching_sha2_more_data(resp_data, password));
      co_return {};
    }
    co_return Err{mysql_error::protocol(
      fmt::format("unexpected auth response: 0x{:02x}",
                  std::to_integer<uint8_t>(resp_data[0])))};
  }

private:
  auto read_exact(folly::MutableByteRange buffer, std::string_view what)
    -> Task<MysqlResult<void>> {
    auto bytes_read = size_t{0};
    while (bytes_read < buffer.size()) {
      auto chunk = co_await co_withExecutor(
        evb_,
        transport_->read(folly::MutableByteRange{buffer.data() + bytes_read,
                                                 buffer.size() - bytes_read},
                         read_timeout_));
      if (chunk == 0) {
        co_return Err{
          mysql_error::protocol(fmt::format("short {} read", what))};
      }
      bytes_read += chunk;
    }
    co_return {};
  }

  explicit async_connection(folly::coro::Transport transport,
                            folly::EventBase* evb)
    : transport_{std::in_place, std::move(transport)}, evb_{evb} {
  }

  Box<folly::coro::Transport> transport_;
  folly::EventBase* evb_ = nullptr;
  std::chrono::milliseconds read_timeout_{std::chrono::seconds{30}};
  uint8_t sequence_id_ = 0;
  bool is_tls_ = false;
};

// -- Result Set Metadata -----------------------------------------------------

/// Result set metadata after reading column definitions.
struct result_set_meta {
  std::vector<column_info> columns;
};

/// Read result set metadata (column count + column definitions).
auto read_result_set_meta(async_connection& conn)
  -> Task<MysqlResult<result_set_meta>> {
  CO_TRY(auto response, co_await conn.read_packet());
  if (auto err = as_error(response)) {
    co_return Err{std::move(*err)};
  }
  if (response.empty()) {
    co_return Err{mysql_error::protocol("empty COM_QUERY response")};
  }
  if (response[0] == packet::ok) {
    co_return result_set_meta{};
  }
  auto reader = packet_reader{response};
  CO_TRY(auto column_count, reader.read_lenenc_int());
  auto meta = result_set_meta{};
  meta.columns.reserve(column_count);
  for (auto i = uint64_t{0}; i < column_count; ++i) {
    CO_TRY(auto col_packet, co_await conn.read_packet());
    CO_TRY(auto col, parse_column_definition(col_packet));
    meta.columns.push_back(std::move(col));
  }
  // Skip EOF packet after column definitions.
  (void)co_await conn.read_packet();
  co_return meta;
}

// -- Async Client ------------------------------------------------------------

/// Configuration for connecting to a MySQL server.
struct client_config {
  std::string host = "localhost";
  uint16_t port = 3306;
  std::string user = "root";
  std::string password;
  std::string database;
  std::shared_ptr<folly::SSLContext> ssl_context;
};

/// Configuration for a query execution.
struct query_config {
  std::string_view sql;
  std::string schema_name = "mysql.query";
  int64_t batch_size = 10000;
};

using result_row = std::vector<std::optional<std::string>>;

/// Parse a row packet into nullable string cells.
auto parse_row_as_strings(std::span<std::byte const> data, size_t column_count)
  -> MysqlResult<result_row> {
  auto reader = packet_reader{data};
  auto row = result_row{};
  row.reserve(column_count);
  for (auto i = size_t{0}; i < column_count; ++i) {
    if (reader.at_end()) {
      row.emplace_back(std::nullopt);
      continue;
    }
    if (reader.current_byte() == lenenc::null_marker) {
      reader.skip(1);
      row.emplace_back(std::nullopt);
      continue;
    }
    TRY(auto value, reader.read_lenenc_string());
    row.emplace_back(std::move(value));
  }
  return row;
}

/// Extract the first column from each row, skipping NULLs.
auto extract_first_column(std::vector<result_row> rows)
  -> MysqlResult<std::vector<std::string>> {
  auto result = std::vector<std::string>{};
  result.reserve(rows.size());
  for (auto& row : rows) {
    if (row.empty()) {
      return Err{mysql_error::protocol("expected at least one column")};
    }
    if (row[0]) {
      result.push_back(std::move(*row[0]));
    }
  }
  return result;
}

/// Parse the first row and first column as optional unsigned 64-bit integer.
auto parse_first_optional_uint64(std::vector<result_row> const& rows)
  -> MysqlResult<std::optional<uint64_t>> {
  if (rows.empty()) {
    return std::optional<uint64_t>{};
  }
  if (rows[0].empty()) {
    return Err{mysql_error::protocol("expected at least one column")};
  }
  if (not rows[0][0]) {
    return std::optional<uint64_t>{};
  }
  auto value = uint64_t{};
  auto& cell = *rows[0][0];
  auto [ptr, ec]
    = std::from_chars(cell.data(), cell.data() + cell.size(), value);
  if (ec != std::errc{} or ptr != cell.data() + cell.size()) {
    return Err{
      mysql_error::protocol(fmt::format("invalid uint64 value `{}`", cell))};
  }
  return std::optional<uint64_t>{value};
}

/// Parse a row packet into a series_builder.
void parse_row_into_builder(std::span<std::byte const> data,
                            std::vector<column_info> const& cols,
                            series_builder& builder) {
  auto event = builder.record();
  auto reader = packet_reader{data};
  for (auto const& col : cols) {
    auto field = event.field(col.name);
    if (reader.at_end()) {
      field.null();
      continue;
    }
    // NULL value.
    if (reader.current_byte() == lenenc::null_marker) {
      field.null();
      reader.skip(1);
      continue;
    }
    auto str_result = reader.read_lenenc_string();
    if (str_result.is_err()) {
      field.null();
      break;
    }
    auto str = std::move(str_result).unwrap();
    // Convert based on column type.
    switch (col.type) {
      case mysql_type::tiny:
      case mysql_type::short_:
      case mysql_type::long_:
      case mysql_type::longlong:
      case mysql_type::int24:
      case mysql_type::year: {
        if (is_unsigned(col)) {
          auto value = uint64_t{};
          auto [ptr, ec]
            = std::from_chars(str.data(), str.data() + str.size(), value);
          if (ec == std::errc{} and ptr == str.data() + str.size()) {
            field.data(value);
          } else {
            field.data(std::string_view{str});
          }
        } else {
          auto value = int64_t{};
          auto [ptr, ec]
            = std::from_chars(str.data(), str.data() + str.size(), value);
          if (ec == std::errc{} and ptr == str.data() + str.size()) {
            field.data(value);
          } else {
            field.data(std::string_view{str});
          }
        }
        break;
      }
      case mysql_type::float_:
      case mysql_type::double_:
      case mysql_type::decimal:
      case mysql_type::newdecimal: {
        auto value = double{};
        if (parsers::real(str, value)) {
          field.data(value);
        } else {
          field.data(std::string_view{str});
        }
        break;
      }
      case mysql_type::tiny_blob:
      case mysql_type::medium_blob:
      case mysql_type::long_blob:
      case mysql_type::blob:
        if (is_binary(col)) {
          field.data(blob{std::span{
            reinterpret_cast<std::byte const*>(str.data()), str.size()}});
        } else {
          field.data(std::move(str));
        }
        break;
      default:
        field.data(std::move(str));
        break;
    }
  }
}

class async_client {
public:
  /// Connect to MySQL server asynchronously.
  static auto make(folly::EventBase* evb, client_config config)
    -> Task<MysqlResult<Box<async_client>>> {
    auto conn = async_connection{};
    try {
      conn = co_await async_connection::connect(evb, config.host, config.port);
    } catch (folly::AsyncSocketException const& e) {
      co_return Err{
        mysql_error{0, fmt::format("connect failed: {}", e.what())}};
    }
    CO_TRY(auto handshake, co_await conn.perform_handshake());
    // TLS upgrade if requested.
    if (config.ssl_context) {
      if (not has_cap(handshake.capabilities, capability::ssl)) {
        co_return Err{mysql_error{0, "server does not support TLS"}};
      }
      CO_TRY(co_await conn.send_ssl_request(not config.database.empty()));
      CO_TRY(co_await conn.upgrade_to_tls(std::move(config.ssl_context),
                                          config.host));
    }
    CO_TRY(co_await conn.send_auth_response(handshake, config.user,
                                            config.password, config.database));
    co_return Box<async_client>{std::in_place, std::move(conn)};
  }

  /// Execute query and stream results as table_slices.
  auto query(query_config cfg) -> AsyncGenerator<MysqlResult<table_slice>> {
    auto columns_result = co_await start_query(cfg.sql);
    if (columns_result.is_err()) {
      co_yield Err{std::move(columns_result).unwrap_err()};
      co_return;
    }
    auto columns = std::move(columns_result).unwrap();
    if (columns.empty()) {
      co_return;
    }
    // Stream rows, building table_slices.
    auto builder = series_builder{};
    auto row_count = int64_t{0};
    while (true) {
      auto row_packet_result = co_await read_next_row_packet();
      if (row_packet_result.is_err()) {
        co_yield Err{std::move(row_packet_result).unwrap_err()};
        co_return;
      }
      auto maybe_row_packet = std::move(row_packet_result).unwrap();
      if (not maybe_row_packet) {
        break;
      }
      // Parse row into builder.
      parse_row_into_builder(*maybe_row_packet, columns, builder);
      ++row_count;
      if (row_count >= cfg.batch_size) {
        for (auto&& slice : builder.finish_as_table_slice(cfg.schema_name)) {
          co_yield std::move(slice);
        }
        builder = series_builder{};
        row_count = 0;
      }
    }
    if (row_count > 0) {
      for (auto&& slice : builder.finish_as_table_slice(cfg.schema_name)) {
        co_yield std::move(slice);
      }
    }
  }

  /// Execute query and return all rows as nullable strings.
  auto query_rows(std::string_view sql)
    -> Task<MysqlResult<std::vector<result_row>>> {
    CO_TRY(auto columns, co_await start_query(sql));
    if (columns.empty()) {
      co_return std::vector<result_row>{};
    }
    auto rows = std::vector<result_row>{};
    while (true) {
      CO_TRY(auto maybe_row_packet, co_await read_next_row_packet());
      if (not maybe_row_packet) {
        break;
      }
      CO_TRY(auto row, parse_row_as_strings(*maybe_row_packet, columns.size()));
      rows.push_back(std::move(row));
    }
    co_return rows;
  }

  explicit async_client(async_connection conn) : conn_{std::move(conn)} {
  }

private:
  auto start_query(std::string_view sql)
    -> Task<MysqlResult<std::vector<column_info>>> {
    auto pkt = make_query_packet(sql);
    CO_TRY(co_await conn_.write_packet(pkt, 0));
    CO_TRY(auto meta, co_await read_result_set_meta(conn_));
    co_return std::move(meta).columns;
  }

  auto read_next_row_packet()
    -> Task<MysqlResult<std::optional<std::vector<std::byte>>>> {
    CO_TRY(auto row_packet, co_await conn_.read_packet());
    if (is_eof(row_packet)) {
      co_return std::optional<std::vector<std::byte>>{};
    }
    if (auto err = as_error(row_packet)) {
      co_return Err{std::move(*err)};
    }
    co_return std::optional<std::vector<std::byte>>{std::move(row_packet)};
  }

  async_connection conn_;
};

// -- Operator Implementation -------------------------------------------------

/// Arguments for the from_mysql operator.
struct FromMySQLArgs {
  std::optional<located<std::string>> table;
  std::optional<located<std::string>> host;
  std::optional<located<int64_t>> port;
  std::optional<located<std::string>> user;
  std::optional<located<std::string>> password;
  std::optional<located<std::string>> database;
  std::optional<located<std::string>> sql;
  std::optional<located<std::string>> show;
  std::optional<located<bool>> live;
  std::optional<located<std::string>> tracking_column;
  std::optional<located<data>> tls;
};

class FromMySQL final : public Operator<void, table_slice> {
public:
  FromMySQL() = default;

  explicit FromMySQL(FromMySQLArgs args) : args_{std::move(args)} {
  }

private:
  auto find_tracking_candidates(std::string const& table,
                                bool auto_increment_only)
    -> Task<MysqlResult<std::vector<std::string>>> {
    auto sql = fmt::format(
      "SELECT column_name "
      "FROM information_schema.columns "
      "WHERE table_schema = DATABASE() "
      "AND table_name = {}",
      quote_string_literal(table));
    if (auto_increment_only) {
      sql += " AND column_key = 'PRI' AND LOCATE('auto_increment', extra) > 0";
    } else {
      sql += " AND data_type IN ('tinyint', 'smallint', 'mediumint', 'int', "
             "'bigint')";
    }
    sql += " ORDER BY ordinal_position";
    auto rows = co_await client_->query_rows(sql);
    if (rows.is_err()) {
      co_return Err{std::move(rows).unwrap_err()};
    }
    co_return extract_first_column(std::move(rows).unwrap());
  }

  auto resolve_tracking_column(OpCtx& ctx) -> Task<bool> {
    TENZIR_ASSERT(args_.table);
    if (args_.tracking_column) {
      auto sql = fmt::format(
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = DATABASE() "
        "AND table_name = {} "
        "AND column_name = {} "
        "AND data_type IN ('tinyint', 'smallint', 'mediumint', 'int', "
        "'bigint')",
        quote_string_literal(args_.table->inner),
        quote_string_literal(args_.tracking_column->inner));
      auto rows = co_await client_->query_rows(sql);
      if (rows.is_err()) {
        emit_mysql_error(std::move(rows).unwrap_err(), ctx);
        co_return false;
      }
      auto result = extract_first_column(std::move(rows).unwrap());
      if (result.is_err()) {
        emit_mysql_error(std::move(result).unwrap_err(), ctx);
        co_return false;
      }
      if (result.unwrap().empty()) {
        diagnostic::error("`tracking_column` `{}` does not exist in `{}` or "
                          "is not an integer column",
                          args_.tracking_column->inner, args_.table->inner)
          .primary(args_.tracking_column->source)
          .emit(ctx);
        co_return false;
      }
      tracking_column_ = args_.tracking_column->inner;
      co_return true;
    }
    auto auto_increment = co_await find_tracking_candidates(args_.table->inner,
                                                            true);
    if (auto_increment.is_err()) {
      emit_mysql_error(std::move(auto_increment).unwrap_err(), ctx);
      co_return false;
    }
    auto auto_increment_columns = std::move(auto_increment).unwrap();
    if (auto_increment_columns.size() > 1) {
      diagnostic::error("multiple auto-increment tracking columns found for "
                        "table `{}`: {}",
                        args_.table->inner,
                        fmt::join(auto_increment_columns, ", "))
        .primary(args_.table->source)
        .hint("set `tracking_column` explicitly")
        .emit(ctx);
      co_return false;
    }
    if (auto_increment_columns.size() == 1) {
      tracking_column_ = std::move(auto_increment_columns[0]);
      co_return true;
    }
    auto integer_candidates = co_await find_tracking_candidates(
      args_.table->inner, false);
    if (integer_candidates.is_err()) {
      emit_mysql_error(std::move(integer_candidates).unwrap_err(), ctx);
      co_return false;
    }
    auto candidates = std::move(integer_candidates).unwrap();
    if (candidates.empty()) {
      diagnostic::error("could not find a suitable `tracking_column` for table "
                        "`{}`",
                        args_.table->inner)
        .primary(args_.table->source)
        .hint("set `tracking_column` to an integer column")
        .emit(ctx);
      co_return false;
    }
    if (candidates.size() > 1) {
      diagnostic::error("ambiguous tracking columns for table `{}`: {}",
                        args_.table->inner, fmt::join(candidates, ", "))
        .primary(args_.table->source)
        .hint("set `tracking_column` explicitly")
        .emit(ctx);
      co_return false;
    }
    tracking_column_ = std::move(candidates[0]);
    co_return true;
  }

  auto query_max_tracking_value() -> Task<MysqlResult<uint64_t>> {
    TENZIR_ASSERT(not tracking_column_.empty());
    TENZIR_ASSERT(not table_name_.empty());
    auto sql = fmt::format("SELECT MAX({}) FROM {}", quote_identifier(
                             tracking_column_),
                           quote_identifier(table_name_));
    auto rows = co_await client_->query_rows(sql);
    if (rows.is_err()) {
      co_return Err{std::move(rows).unwrap_err()};
    }
    auto value = parse_first_optional_uint64(std::move(rows).unwrap());
    if (value.is_err()) {
      co_return Err{std::move(value).unwrap_err()};
    }
    co_return std::move(value).unwrap().value_or(uint64_t{0});
  }

  auto initialize_live(OpCtx& ctx) -> Task<bool> {
    if (tracking_column_.empty()) {
      if (not(co_await resolve_tracking_column(ctx))) {
        co_return false;
      }
    }
    auto watermark = co_await query_max_tracking_value();
    if (watermark.is_err()) {
      emit_mysql_error(std::move(watermark).unwrap_err(), ctx);
      co_return false;
    }
    live_watermark_ = std::move(watermark).unwrap();
    live_initialized_ = true;
    co_return true;
  }

  auto stream_live_window(uint64_t lower, uint64_t upper,
                          Push<table_slice>& push, OpCtx& ctx) -> Task<bool> {
    auto sql = fmt::format("SELECT * FROM {} WHERE {} > {} AND {} <= {} "
                           "ORDER BY {}",
                           quote_identifier(table_name_),
                           quote_identifier(tracking_column_), lower,
                           quote_identifier(tracking_column_), upper,
                           quote_identifier(tracking_column_));
    auto slice_stream = client_->query({
      .sql = sql,
      .schema_name = schema_name_,
    });
    while (auto slice_result = co_await slice_stream.next()) {
      if (slice_result->is_err()) {
        emit_mysql_error(std::move(*slice_result).unwrap_err(), ctx);
        co_return false;
      }
      co_await push(std::move(*slice_result).unwrap());
    }
    co_return true;
  }

public:
  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    if (done_) {
      co_return;
    }
    live_ = args_.live and args_.live->inner;
    if (args_.table) {
      table_name_ = args_.table->inner;
    }
    if (args_.tracking_column) {
      tracking_column_ = args_.tracking_column->inner;
    }
    // Build client configuration.
    auto config = client_config{};
    if (args_.host) {
      config.host = args_.host->inner;
    }
    if (args_.port) {
      config.port = static_cast<uint16_t>(args_.port->inner);
    }
    if (args_.user) {
      config.user = args_.user->inner;
    }
    if (args_.password) {
      config.password = args_.password->inner;
    }
    if (args_.database) {
      config.database = args_.database->inner;
    }
    // Build SSL context from TLS options.
    if (args_.tls) {
      auto result = tls_options{*args_.tls}.make_folly_ssl_context(ctx);
      if (not result) {
        done_ = true;
        co_return;
      }
      config.ssl_context = std::move(*result);
    }
    // Build the SQL query.
    if (args_.show) {
      if (args_.show->inner == "tables") {
        query_ = "SELECT table_schema AS `database`, table_name AS `table` "
                 "FROM information_schema.tables "
                 "WHERE table_schema = DATABASE() "
                 "AND table_type = 'BASE TABLE' "
                 "ORDER BY table_name";
        schema_name_ = "mysql.tables";
      } else if (args_.show->inner == "columns") {
        if (not args_.table) {
          diagnostic::error("`show=\"columns\"` requires `table` to be set")
            .primary(args_.show->source)
            .emit(ctx);
          done_ = true;
          co_return;
        }
        query_ = fmt::format("SHOW COLUMNS FROM {}",
                             quote_identifier(args_.table->inner));
        schema_name_ = fmt::format("mysql.columns.{}", args_.table->inner);
      } else {
        diagnostic::error("invalid show mode `{}`", args_.show->inner)
          .primary(args_.show->source)
          .hint("expected `tables` or `columns`")
          .emit(ctx);
        done_ = true;
        co_return;
      }
    } else if (args_.sql) {
      query_ = args_.sql->inner;
      schema_name_ = "mysql.query";
    } else if (args_.table) {
      query_
        = fmt::format("SELECT * FROM {}", quote_identifier(args_.table->inner));
      schema_name_ = fmt::format("mysql.{}", args_.table->inner);
    } else {
      diagnostic::error("no query specified")
        .hint("specify `table`, `sql`, or `show`")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    // Use the global IO executor's EventBase for async socket I/O.
    auto* evb = folly::getGlobalIOExecutor()->getEventBase();
    // Connect asynchronously.
    auto result = co_await async_client::make(evb, std::move(config));
    if (result.is_err()) {
      emit_mysql_error(std::move(result).unwrap_err(), ctx);
      done_ = true;
      co_return;
    }
    client_ = std::move(result).unwrap();
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (live_ and live_initialized_) {
      co_await folly::coro::sleep(std::chrono::duration_cast<
                                  folly::HighResDuration>(live_poll_interval_));
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result);
    if (done_) {
      co_return;
    }
    if (live_) {
      if (not live_initialized_) {
        if (not(co_await initialize_live(ctx))) {
          done_ = true;
        }
        co_return;
      }
      auto upper_result = co_await query_max_tracking_value();
      if (upper_result.is_err()) {
        emit_mysql_error(std::move(upper_result).unwrap_err(), ctx);
        done_ = true;
        co_return;
      }
      auto upper = std::move(upper_result).unwrap();
      if (upper <= live_watermark_) {
        co_return;
      }
      if (not(co_await stream_live_window(live_watermark_, upper, push, ctx))) {
        done_ = true;
        co_return;
      }
      live_watermark_ = upper;
      co_return;
    }
    auto slice_stream = client_->query({
      .sql = query_,
      .schema_name = schema_name_,
    });
    while (auto slice_result = co_await slice_stream.next()) {
      if (slice_result->is_err()) {
        emit_mysql_error(std::move(*slice_result).unwrap_err(), ctx);
        break;
      }
      co_await push(std::move(*slice_result).unwrap());
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
    serde("live", live_);
    serde("table_name", table_name_);
    serde("tracking_column", tracking_column_);
    serde("live_initialized", live_initialized_);
    serde("live_watermark", live_watermark_);
  }

private:
  FromMySQLArgs args_;
  Box<async_client> client_;
  std::string query_;
  std::string schema_name_;
  std::string table_name_;
  std::string tracking_column_;
  bool live_ = false;
  bool live_initialized_ = false;
  uint64_t live_watermark_ = 0;
  static constexpr auto live_poll_interval_ = std::chrono::seconds{1};
  bool done_ = false;
};

// -- Plugin Registration -----------------------------------------------------

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_mysql";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromMySQLArgs, FromMySQL>{};
    auto table_arg = d.named("table", &FromMySQLArgs::table);
    // Connection options.
    d.named("host", &FromMySQLArgs::host);
    auto port_arg = d.named("port", &FromMySQLArgs::port);
    d.named("user", &FromMySQLArgs::user);
    d.named("password", &FromMySQLArgs::password);
    d.named("database", &FromMySQLArgs::database);
    // TLS option.
    auto tls_arg = d.named("tls", &FromMySQLArgs::tls);
    // Query options.
    auto sql_arg = d.named("sql", &FromMySQLArgs::sql);
    auto show = d.named("show", &FromMySQLArgs::show);
    auto live_arg = d.named("live", &FromMySQLArgs::live);
    auto tracking_column_arg
      = d.named("tracking_column", &FromMySQLArgs::tracking_column);
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto has_table = ctx.get(table_arg).has_value();
      auto has_sql = ctx.get(sql_arg).has_value();
      auto has_show = ctx.get(show).has_value();
      auto is_live = false;
      if (auto live = ctx.get(live_arg)) {
        is_live = live->inner;
      }
      if (ctx.get(tracking_column_arg) and not is_live) {
        diagnostic::error("`tracking_column` requires `live=true`")
          .primary(ctx.get_location(tracking_column_arg).value())
          .emit(ctx);
        return {};
      }
      if (is_live) {
        if (not has_table) {
          diagnostic::error("`live=true` requires `table`")
            .primary(ctx.get_location(live_arg).value())
            .emit(ctx);
          return {};
        }
        if (has_sql) {
          diagnostic::error("`live=true` does not support `sql`")
            .primary(ctx.get_location(sql_arg).value())
            .emit(ctx);
          return {};
        }
        if (has_show) {
          diagnostic::error("`live=true` does not support `show`")
            .primary(ctx.get_location(show).value())
            .emit(ctx);
          return {};
        }
      }
      // At least one query option must be specified.
      if (not has_table and not has_sql and not has_show) {
        diagnostic::error("no query specified")
          .hint("specify `table`, `sql`, or `show`")
          .emit(ctx);
        return {};
      }
      // Mutual exclusivity.
      if (has_sql and has_show) {
        diagnostic::error("`sql` and `show` are mutually exclusive").emit(ctx);
        return {};
      }
      if (has_sql and has_table) {
        diagnostic::error("`sql` and `table` are mutually exclusive").emit(ctx);
        return {};
      }
      // Validate port range.
      if (auto port = ctx.get(port_arg)) {
        constexpr auto max_port = int64_t{std::numeric_limits<uint16_t>::max()};
        if (port->inner < 0 or port->inner > max_port) {
          diagnostic::error("`port` must be in range [0, {}]", max_port)
            .primary(ctx.get_location(port_arg).value())
            .emit(ctx);
        }
      }
      // Validate show mode.
      if (has_show) {
        auto show_value = ctx.get(show);
        if (not show_value) {
          return {};
        }
        auto& mode = show_value->inner;
        if (mode != "tables" and mode != "columns") {
          diagnostic::error("invalid show mode `{}`", mode)
            .primary(ctx.get_location(show).value())
            .hint("expected `tables` or `columns`")
            .emit(ctx);
        }
        if (mode == "columns" and not has_table) {
          diagnostic::error("`show=\"columns\"` requires `table` to be set")
            .primary(ctx.get_location(show).value())
            .emit(ctx);
        }
      }
      // Validate TLS record structure.
      if (auto tls_val = ctx.get(tls_arg)) {
        (void)tls_options{*tls_val}.validate(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::mysql

TENZIR_REGISTER_PLUGIN(tenzir::plugins::mysql::Plugin)
