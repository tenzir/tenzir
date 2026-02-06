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
#include <tenzir/detail/byteswap.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/Transport.h>
#include <openssl/sha.h>

#include <charconv>
#include <cstring>
#include <utility>

// TODO: Consider moving these into a common place.
#define TENZIR_TRY_COMMON_(var, expr, ret)                                     \
  auto var = (expr);                                                           \
  if (not tenzir::tryable<decltype(var)>::is_success(var)) [[unlikely]] {      \
    ret tenzir::tryable<decltype(var)>::get_error(std::move(var));             \
  }

#define TENZIR_TRY_EXTRACT_(decl, var, expr, ret)                              \
  TENZIR_TRY_COMMON_(var, expr, ret);                                          \
  decl = tenzir::tryable<decltype(var)>::get_success(std::move(var))

#define TENZIR_TRY_DISCARD_(var, expr, ret)                                    \
  TENZIR_TRY_COMMON_(var, expr, ret)                                           \
  if (false) {                                                                 \
    /* trigger [[nodiscard]] */                                                \
    tenzir::tryable<decltype(var)>::get_success(std::move(var));               \
  }

#define TENZIR_TRY_X_1(expr, ret)                                              \
  TENZIR_TRY_DISCARD_(TENZIR_PP_PASTE2(_try, __COUNTER__), expr, ret)

#define TENZIR_TRY_X_2(decl, expr, ret)                                        \
  TENZIR_TRY_EXTRACT_(decl, TENZIR_PP_PASTE2(_try, __COUNTER__), expr, ret)

#define TENZIR_CO_TRY(...)                                                     \
  TENZIR_PP_OVERLOAD(TENZIR_TRY_X_, __VA_ARGS__)(__VA_ARGS__, co_return)

#define CO_TRY TENZIR_CO_TRY

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
//     |  +--- AuthSwitch (0xFE) ------------+ (not implemented)
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
    auto transport
      = co_await folly::coro::co_invoke(
          [&]() -> folly::coro::Task<folly::coro::Transport> {
            co_return co_await folly::coro::Transport::newConnectedSocket(
              evb, addr, timeout);
          })
          .scheduleOn(evb);
    co_return async_connection{std::move(transport), evb};
  }

  /// Read a MySQL packet asynchronously.
  auto read_packet() -> Task<MysqlResult<std::vector<std::byte>>> {
    // Read 4-byte header: 3 bytes length + 1 byte sequence.
    auto header = std::array<unsigned char, 4>{};
    auto n
      = co_await folly::coro::co_invoke([&]() -> folly::coro::Task<size_t> {
          co_return co_await transport_->read(
            folly::MutableByteRange{header.data(), 4}, read_timeout_);
        }).scheduleOn(evb_);
    if (n != 4) {
      co_return Err{mysql_error::protocol("short header read")};
    }
    auto len = uint32_t{header[0]} | (uint32_t{header[1]} << 8)
               | (uint32_t{header[2]} << 16);
    sequence_id_ = header[3];
    // Read payload.
    auto payload = std::vector<std::byte>(len);
    auto read
      = co_await folly::coro::co_invoke([&]() -> folly::coro::Task<size_t> {
          co_return co_await transport_->read(
            folly::MutableByteRange{
              reinterpret_cast<unsigned char*>(payload.data()), len},
            read_timeout_);
        }).scheduleOn(evb_);
    if (read != len) {
      co_return Err{mysql_error::protocol("short payload read")};
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
    co_await folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
      co_await transport_->write(folly::ByteRange{pkt.data(), pkt.size()});
    }).scheduleOn(evb_);
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

  /// Send the authentication response.
  auto
  send_auth_response(handshake_info const& handshake, std::string const& user,
                     std::string const& password, std::string const& database)
    -> Task<MysqlResult<void>> {
    auto buf = std::vector<std::byte>{};
    // Client capabilities.
    auto caps
      = std::to_underlying(capability::long_password)
        | std::to_underlying(capability::protocol_41)
        | std::to_underlying(capability::secure_connection)
        | std::to_underlying(capability::plugin_auth)
        | std::to_underlying(capability::plugin_auth_lenenc_client_data);
    if (not database.empty()) {
      caps |= std::to_underlying(capability::connect_with_db);
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
    // Send packet.
    CO_TRY(co_await write_packet(buf, 1));
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
    // Auth switch request.
    if (resp_data[0] == packet::auth_switch) {
      co_return Err{
        mysql_error::protocol("auth switch request not implemented")};
    }
    // caching_sha2_password more-data response.
    if (resp_data[0] == packet::auth_more_data and resp_data.size() > 1) {
      // 0x03 = fast auth success (cached), read the final OK.
      if (resp_data[1] == caching_sha2::fast_auth_success) {
        CO_TRY(auto final_resp, co_await read_packet());
        if (final_resp[0] == packet::ok) {
          co_return {};
        }
        co_return Err{
          mysql_error::protocol("unexpected response after fast auth")};
      }
      // 0x04 = full auth required. Send password as null-terminated cleartext.
      if (resp_data[1] == caching_sha2::full_auth_required) {
        auto pw_packet = std::vector<std::byte>{};
        auto pw_bytes = as_bytes(password);
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
    }
    co_return Err{mysql_error::protocol(
      fmt::format("unexpected auth response: 0x{:02x}",
                  std::to_integer<uint8_t>(resp_data[0])))};
  }

private:
  explicit async_connection(folly::coro::Transport transport,
                            folly::EventBase* evb)
    : transport_{std::in_place, std::move(transport)}, evb_{evb} {
  }

  Box<folly::coro::Transport> transport_;
  folly::EventBase* evb_ = nullptr;
  std::chrono::milliseconds read_timeout_{std::chrono::seconds{30}};
  uint8_t sequence_id_ = 0;
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
};

/// Configuration for a query execution.
struct query_config {
  std::string_view sql;
  std::string schema_name = "mysql.query";
  int64_t batch_size = 10000;
};

/// Parse a row packet into a series_builder.
void parse_row_into_builder(std::span<std::byte const> data,
                            std::vector<column_info> const& cols,
                            series_builder& builder) {
  auto event = builder.record();
  auto reader = packet_reader{data};
  auto try_int = [](std::string const& s, bool is_uint) -> data_view2 {
    if (is_uint) {
      auto value = uint64_t{};
      auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
      if (ec == std::errc{} and ptr == s.data() + s.size()) {
        return value;
      }
    } else {
      auto value = int64_t{};
      auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
      if (ec == std::errc{} and ptr == s.data() + s.size()) {
        return value;
      }
    }
    return std::string_view{s};
  };
  auto try_double = [](std::string const& s) -> data_view2 {
    auto value = double{};
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    if (ec == std::errc{} and ptr == s.data() + s.size()) {
      return value;
    }
    return std::string_view{s};
  };
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
    auto str = std::move(str_result).value();
    // Convert based on column type.
    switch (col.type) {
      case mysql_type::tiny:
      case mysql_type::short_:
      case mysql_type::long_:
      case mysql_type::longlong:
      case mysql_type::int24:
      case mysql_type::year:
        field.data(try_int(str, is_unsigned(col)));
        break;
      case mysql_type::float_:
      case mysql_type::double_:
      case mysql_type::decimal:
      case mysql_type::newdecimal:
        field.data(try_double(str));
        break;
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
    CO_TRY(co_await conn.send_auth_response(handshake, config.user,
                                            config.password, config.database));
    co_return Box<async_client>{std::in_place, std::move(conn)};
  }

  /// Execute query and stream results as table_slices.
  auto query(query_config cfg) -> AsyncGenerator<MysqlResult<table_slice>> {
    // Send COM_QUERY.
    auto pkt = make_query_packet(cfg.sql);
    auto write_result = co_await conn_.write_packet(pkt, 0);
    if (write_result.is_err()) {
      co_yield Err{std::move(write_result).unwrap_err()};
      co_return;
    }
    // Read result set metadata.
    auto meta = co_await read_result_set_meta(conn_);
    if (meta.is_err()) {
      co_yield Err{std::move(meta).unwrap_err()};
      co_return;
    }
    auto columns = std::move(meta).value().columns;
    // Stream rows, building table_slices.
    auto builder = series_builder{};
    auto row_count = int64_t{0};
    while (true) {
      auto row_packet = co_await conn_.read_packet();
      if (row_packet.is_err()) {
        co_yield Err{std::move(row_packet).unwrap_err()};
        co_return;
      }
      if (is_eof(row_packet.value())) {
        break;
      }
      if (auto err = as_error(row_packet.value())) {
        co_yield Err{std::move(*err)};
        co_return;
      }
      // Parse row into builder.
      parse_row_into_builder(row_packet.value(), columns, builder);
      ++row_count;
      if (row_count >= cfg.batch_size) {
        co_yield builder.finish_assert_one_slice(cfg.schema_name);
        builder = series_builder{};
        row_count = 0;
      }
    }
    if (row_count > 0) {
      co_yield builder.finish_assert_one_slice(cfg.schema_name);
    }
  }

  explicit async_client(async_connection conn) : conn_{std::move(conn)} {
  }

private:
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
};

class FromMySQL final : public Operator<void, table_slice> {
public:
  FromMySQL() = default;

  explicit FromMySQL(FromMySQLArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    if (done_) {
      co_return;
    }
    // Build client configuration.
    auto config = client_config{};
    if (args_.host) {
      config.host = args_.host->inner;
    }
    if (args_.port) {
      config.port = detail::narrow<uint16_t>(args_.port->inner);
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
    // Build the SQL query.
    if (args_.show) {
      if (args_.show->inner == "tables") {
        query_ = "SHOW TABLES";
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
    client_ = std::move(result).value();
  }

  auto await_task() const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result);
    if (done_) {
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
      co_await push(std::move(*slice_result).value());
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  FromMySQLArgs args_;
  Box<async_client> client_;
  std::string query_;
  std::string schema_name_;
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
    // Positional: table name.
    auto table_arg = d.positional("table", &FromMySQLArgs::table);
    // Connection options.
    d.named("host", &FromMySQLArgs::host);
    d.named("port", &FromMySQLArgs::port);
    d.named("user", &FromMySQLArgs::user);
    d.named("password", &FromMySQLArgs::password);
    d.named("database", &FromMySQLArgs::database);
    // Query options.
    auto sql_arg = d.named("sql", &FromMySQLArgs::sql);
    auto show = d.named("show", &FromMySQLArgs::show);
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto has_table = ctx.get(table_arg).has_value();
      auto has_sql = ctx.get(sql_arg).has_value();
      auto has_show = ctx.get(show).has_value();
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
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::mysql

TENZIR_REGISTER_PLUGIN(tenzir::plugins::mysql::Plugin)
