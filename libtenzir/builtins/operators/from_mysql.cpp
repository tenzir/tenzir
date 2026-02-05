//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// -- Section 1: Includes ----------------------------------------------------

#include <tenzir/any.hpp>
#include <tenzir/async.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/Transport.h>
#include <openssl/sha.h>

namespace tenzir::plugins::mysql {

namespace {

// -- Section 2: Protocol Types -----------------------------------------------

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
  return (col.flags & static_cast<uint16_t>(column_flag::unsigned_)) != 0;
}

auto is_binary(column_info const& col) -> bool {
  return (col.flags & static_cast<uint16_t>(column_flag::binary)) != 0;
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

/// MySQL server handshake information.
struct handshake_info {
  uint8_t protocol_version = 0;
  std::string server_version;
  uint32_t connection_id = 0;
  std::vector<uint8_t> auth_plugin_data;
  uint32_t capabilities = 0;
  uint8_t charset = 0;
  uint16_t status_flags = 0;
  std::string auth_plugin_name;
};

// -- Section 3: Wire Protocol ------------------------------------------------

/// Reads a little-endian integer from a buffer.
template <class T>
auto read_int_le(std::span<uint8_t const> data) -> T {
  auto result = T{0};
  for (auto i = size_t{0}; i < sizeof(T) and i < data.size(); ++i) {
    result |= static_cast<T>(data[i]) << (8 * i);
  }
  return result;
}

/// Writes a little-endian integer to a buffer.
template <class T>
void write_int_le(std::vector<uint8_t>& buffer, T value) {
  for (auto i = size_t{0}; i < sizeof(T); ++i) {
    buffer.push_back(static_cast<uint8_t>(value >> (8 * i)));
  }
}

/// Reads a length-encoded integer from a buffer.
auto read_lenenc_int(std::span<uint8_t const> data)
  -> MysqlResult<std::pair<uint64_t, size_t>> {
  if (data.empty()) {
    return Err{mysql_error::protocol("empty data for lenenc int")};
  }
  auto first = data[0];
  if (first < 0xfb) {
    return std::pair{uint64_t{first}, size_t{1}};
  }
  if (first == 0xfc) {
    if (data.size() < 3) {
      return Err{mysql_error::protocol("truncated 2-byte lenenc int")};
    }
    return std::pair{uint64_t{read_int_le<uint16_t>(data.subspan(1))},
                     size_t{3}};
  }
  if (first == 0xfd) {
    if (data.size() < 4) {
      return Err{mysql_error::protocol("truncated 3-byte lenenc int")};
    }
    auto val = uint32_t{data[1]} | (uint32_t{data[2]} << 8)
               | (uint32_t{data[3]} << 16);
    return std::pair{uint64_t{val}, size_t{4}};
  }
  if (first == 0xfe) {
    if (data.size() < 9) {
      return Err{mysql_error::protocol("truncated 8-byte lenenc int")};
    }
    return std::pair{uint64_t{read_int_le<uint64_t>(data.subspan(1))},
                     size_t{9}};
  }
  // 0xfb = NULL, 0xff = error marker
  return std::pair{uint64_t{0}, size_t{1}};
}

/// Reads a length-encoded string from a buffer.
auto read_lenenc_string(std::span<uint8_t const> data)
  -> MysqlResult<std::pair<std::string, size_t>> {
  auto li = read_lenenc_int(data);
  if (li.is_err()) {
    return Err{std::move(li).unwrap_err()};
  }
  auto [length, consumed] = li.value();
  if (data.size() < consumed + length) {
    return Err{mysql_error::protocol("truncated lenenc string")};
  }
  auto str_data = data.subspan(consumed, length);
  return std::pair{std::string(reinterpret_cast<char const*>(str_data.data()),
                               length),
                   consumed + length};
}

/// Reads a null-terminated string from a buffer.
auto read_null_string(std::span<uint8_t const> data)
  -> std::pair<std::string, size_t> {
  auto it = std::find(data.begin(), data.end(), 0);
  if (it == data.end()) {
    return {"", 0};
  }
  auto length = static_cast<size_t>(std::distance(data.begin(), it));
  return {std::string(reinterpret_cast<char const*>(data.data()), length),
          length + 1};
}

/// Writes a length-encoded integer to a buffer.
void write_lenenc_int(std::vector<uint8_t>& buffer, uint64_t value) {
  if (value < 251) {
    buffer.push_back(static_cast<uint8_t>(value));
  } else if (value < 65536) {
    buffer.push_back(0xfc);
    write_int_le<uint16_t>(buffer, static_cast<uint16_t>(value));
  } else if (value < 16777216) {
    buffer.push_back(0xfd);
    buffer.push_back(static_cast<uint8_t>(value));
    buffer.push_back(static_cast<uint8_t>(value >> 8));
    buffer.push_back(static_cast<uint8_t>(value >> 16));
  } else {
    buffer.push_back(0xfe);
    write_int_le<uint64_t>(buffer, value);
  }
}

/// Writes a null-terminated string to a buffer.
void write_null_string(std::vector<uint8_t>& buffer, std::string_view str) {
  buffer.insert(buffer.end(), str.begin(), str.end());
  buffer.push_back(0);
}

/// Parses a column definition packet.
auto parse_column_definition(std::span<uint8_t const> data)
  -> MysqlResult<column_info> {
  auto col = column_info{};
  auto offset = size_t{0};
  auto read_str = [&]() -> MysqlResult<std::string> {
    auto result = read_lenenc_string(data.subspan(offset));
    if (result.is_err()) {
      return Err{std::move(result).unwrap_err()};
    }
    auto [str, consumed] = std::move(result).value();
    offset += consumed;
    return str;
  };
  auto s = read_str();
  if (s.is_err()) {
    return Err{std::move(s).unwrap_err()};
  }
  col.catalog = std::move(s).value();
  s = read_str();
  if (s.is_err()) {
    return Err{std::move(s).unwrap_err()};
  }
  col.schema = std::move(s).value();
  s = read_str();
  if (s.is_err()) {
    return Err{std::move(s).unwrap_err()};
  }
  col.table = std::move(s).value();
  s = read_str();
  if (s.is_err()) {
    return Err{std::move(s).unwrap_err()};
  }
  col.org_table = std::move(s).value();
  s = read_str();
  if (s.is_err()) {
    return Err{std::move(s).unwrap_err()};
  }
  col.name = std::move(s).value();
  s = read_str();
  if (s.is_err()) {
    return Err{std::move(s).unwrap_err()};
  }
  col.org_name = std::move(s).value();
  // Fixed-length fields after 0x0c length marker.
  if (data.size() < offset + 1) {
    return Err{mysql_error::protocol("column definition truncated")};
  }
  auto filler = read_lenenc_int(data.subspan(offset));
  if (filler.is_err()) {
    return Err{std::move(filler).unwrap_err()};
  }
  offset += filler.value().second;
  if (data.size() < offset + 12) {
    return Err{mysql_error::protocol("column definition truncated")};
  }
  col.charset = read_int_le<uint16_t>(data.subspan(offset));
  offset += 2;
  col.length = read_int_le<uint32_t>(data.subspan(offset));
  offset += 4;
  col.type = static_cast<mysql_type>(data[offset]);
  offset += 1;
  col.flags = read_int_le<uint16_t>(data.subspan(offset));
  offset += 2;
  col.decimals = data[offset];
  return col;
}

/// Computes the MySQL native password authentication response.
auto compute_native_password(std::string_view password,
                             std::span<uint8_t const> scramble)
  -> std::vector<uint8_t> {
  if (password.empty()) {
    return {};
  }
  // SHA1(password)
  auto hash1 = std::array<uint8_t, SHA_DIGEST_LENGTH>{};
  SHA1(reinterpret_cast<uint8_t const*>(password.data()), password.size(),
       hash1.data());
  // SHA1(SHA1(password))
  auto hash2 = std::array<uint8_t, SHA_DIGEST_LENGTH>{};
  SHA1(hash1.data(), hash1.size(), hash2.data());
  // SHA1(scramble + SHA1(SHA1(password)))
  auto combined = std::vector<uint8_t>{};
  combined.insert(combined.end(), scramble.begin(), scramble.end());
  combined.insert(combined.end(), hash2.begin(), hash2.end());
  auto hash3 = std::array<uint8_t, SHA_DIGEST_LENGTH>{};
  SHA1(combined.data(), combined.size(), hash3.data());
  // XOR SHA1(password) with result.
  auto result = std::vector<uint8_t>(SHA_DIGEST_LENGTH);
  for (auto i = size_t{0}; i < SHA_DIGEST_LENGTH; ++i) {
    result[i] = hash1[i] ^ hash3[i];
  }
  return result;
}

/// Computes the MySQL caching_sha2_password authentication response.
auto compute_caching_sha2_password(std::string_view password,
                                   std::span<uint8_t const> scramble)
  -> std::vector<uint8_t> {
  if (password.empty()) {
    return {};
  }
  // SHA256(password)
  auto hash1 = std::array<uint8_t, SHA256_DIGEST_LENGTH>{};
  SHA256(reinterpret_cast<uint8_t const*>(password.data()), password.size(),
         hash1.data());
  // SHA256(SHA256(password))
  auto hash2 = std::array<uint8_t, SHA256_DIGEST_LENGTH>{};
  SHA256(hash1.data(), hash1.size(), hash2.data());
  // SHA256(SHA256(SHA256(password)) + scramble)
  auto combined = std::vector<uint8_t>{};
  combined.insert(combined.end(), hash2.begin(), hash2.end());
  combined.insert(combined.end(), scramble.begin(), scramble.end());
  auto hash3 = std::array<uint8_t, SHA256_DIGEST_LENGTH>{};
  SHA256(combined.data(), combined.size(), hash3.data());
  // XOR SHA256(password) with result.
  auto result = std::vector<uint8_t>(SHA256_DIGEST_LENGTH);
  for (auto i = size_t{0}; i < SHA256_DIGEST_LENGTH; ++i) {
    result[i] = hash1[i] ^ hash3[i];
  }
  return result;
}

/// Parses an error packet into a mysql_error.
auto parse_error_from_packet(std::span<uint8_t const> data) -> mysql_error {
  TENZIR_ASSERT(not data.empty() and data[0] == 0xff);
  if (data.size() < 3) {
    return mysql_error::protocol("error packet too short");
  }
  auto code = read_int_le<uint16_t>(data.subspan(1));
  auto offset = size_t{3};
  // Skip SQL state marker.
  if (offset < data.size() and data[offset] == '#') {
    offset += 1;
    if (data.size() >= offset + 5) {
      offset += 5;
    }
  }
  auto msg = std::string{};
  if (offset < data.size()) {
    msg = std::string(reinterpret_cast<char const*>(data.data() + offset),
                      data.size() - offset);
  }
  return {code, std::move(msg)};
}

// -- Section 4: Async Connection ---------------------------------------------

/// Async MySQL connection using folly::coro::Transport.
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
      = co_await folly::coro::Transport::newConnectedSocket(evb, addr, timeout);
    co_return async_connection{std::move(transport)};
  }
  /// Read a MySQL packet asynchronously.
  auto read_packet() -> Task<MysqlResult<std::vector<uint8_t>>> {
    // Read 4-byte header: 3 bytes length + 1 byte sequence.
    auto header = std::array<uint8_t, 4>{};
    auto n = co_await transport_->read(
      folly::MutableByteRange{header.data(), 4}, read_timeout_);
    if (n != 4) {
      co_return Err{mysql_error::protocol("short header read")};
    }
    auto len = uint32_t{header[0]} | (uint32_t{header[1]} << 8)
               | (uint32_t{header[2]} << 16);
    sequence_id_ = header[3];
    // Read payload.
    auto payload = std::vector<uint8_t>(len);
    auto read = co_await transport_->read(
      folly::MutableByteRange{payload.data(), len}, read_timeout_);
    if (read != len) {
      co_return Err{mysql_error::protocol("short payload read")};
    }
    co_return payload;
  }
  /// Write a MySQL packet asynchronously.
  auto write_packet(std::span<uint8_t const> payload, uint8_t seq)
    -> Task<MysqlResult<void>> {
    auto packet = std::vector<uint8_t>(4 + payload.size());
    packet[0] = static_cast<uint8_t>(payload.size() & 0xFF);
    packet[1] = static_cast<uint8_t>((payload.size() >> 8) & 0xFF);
    packet[2] = static_cast<uint8_t>((payload.size() >> 16) & 0xFF);
    packet[3] = seq;
    std::copy(payload.begin(), payload.end(), packet.begin() + 4);
    co_await transport_->write(folly::ByteRange{packet.data(), packet.size()});
    co_return {};
  }
  /// Perform the initial handshake with the server.
  auto perform_handshake() -> Task<MysqlResult<handshake_info>> {
    auto pkt = co_await read_packet();
    if (pkt.is_err()) {
      co_return Err{std::move(pkt).unwrap_err()};
    }
    auto data = std::move(pkt).value();
    if (data.empty()) {
      co_return Err{mysql_error::protocol("empty handshake packet")};
    }
    // Check for error packet.
    if (data[0] == 0xff) {
      co_return Err{parse_error_from_packet(data)};
    }
    auto info = handshake_info{};
    auto offset = size_t{0};
    // Protocol version.
    info.protocol_version = data[offset++];
    if (info.protocol_version != 10) {
      co_return Err{mysql_error::protocol(fmt::format(
        "unsupported protocol version: {}", info.protocol_version))};
    }
    // Server version (null-terminated).
    auto [version, version_len]
      = read_null_string(std::span{data}.subspan(offset));
    info.server_version = std::move(version);
    offset += version_len;
    // Connection ID.
    info.connection_id = read_int_le<uint32_t>(std::span{data}.subspan(offset));
    offset += 4;
    // Auth plugin data part 1 (8 bytes).
    info.auth_plugin_data.insert(info.auth_plugin_data.end(),
                                 data.begin() + offset,
                                 data.begin() + offset + 8);
    offset += 8;
    // Filler.
    offset += 1;
    // Capability flags (lower 2 bytes).
    info.capabilities = read_int_le<uint16_t>(std::span{data}.subspan(offset));
    offset += 2;
    if (offset < data.size()) {
      // Character set.
      info.charset = data[offset++];
      // Status flags.
      info.status_flags
        = read_int_le<uint16_t>(std::span{data}.subspan(offset));
      offset += 2;
      // Capability flags (upper 2 bytes).
      info.capabilities |= static_cast<uint32_t>(read_int_le<uint16_t>(
                             std::span{data}.subspan(offset)))
                           << 16;
      offset += 2;
      // Auth plugin data length.
      auto auth_data_len = uint8_t{0};
      if (info.capabilities & static_cast<uint32_t>(capability::plugin_auth)) {
        auth_data_len = data[offset];
      }
      offset += 1;
      // Reserved (10 bytes).
      offset += 10;
      // Auth plugin data part 2.
      if (info.capabilities
          & static_cast<uint32_t>(capability::secure_connection)) {
        auto part2_len = static_cast<size_t>(std::max(13, auth_data_len - 8));
        if (offset + part2_len <= data.size()) {
          // Don't include the trailing null byte.
          auto end = data.begin() + offset + part2_len;
          while (end > data.begin() + offset and *(end - 1) == 0) {
            --end;
          }
          info.auth_plugin_data.insert(info.auth_plugin_data.end(),
                                       data.begin() + offset, end);
          offset += part2_len;
        }
      }
      // Auth plugin name.
      if (info.capabilities & static_cast<uint32_t>(capability::plugin_auth)) {
        auto [plugin_name, plugin_len]
          = read_null_string(std::span{data}.subspan(offset));
        info.auth_plugin_name = std::move(plugin_name);
      }
    }
    co_return info;
  }
  /// Send the authentication response.
  auto
  send_auth_response(handshake_info const& handshake, std::string const& user,
                     std::string const& password, std::string const& database)
    -> Task<MysqlResult<void>> {
    auto packet = std::vector<uint8_t>{};
    // Client capabilities.
    auto caps
      = static_cast<uint32_t>(capability::long_password)
        | static_cast<uint32_t>(capability::protocol_41)
        | static_cast<uint32_t>(capability::secure_connection)
        | static_cast<uint32_t>(capability::plugin_auth)
        | static_cast<uint32_t>(capability::plugin_auth_lenenc_client_data);
    if (not database.empty()) {
      caps |= static_cast<uint32_t>(capability::connect_with_db);
    }
    write_int_le<uint32_t>(packet, caps);
    // Max packet size.
    write_int_le<uint32_t>(packet, 16777215);
    // Character set (utf8mb4 = 255).
    packet.push_back(255);
    // Reserved (23 bytes).
    packet.insert(packet.end(), 23, 0);
    // Username (null-terminated).
    write_null_string(packet, user);
    // Auth response.
    auto auth_response = std::vector<uint8_t>{};
    if (handshake.auth_plugin_name == "mysql_native_password") {
      auth_response
        = compute_native_password(password, handshake.auth_plugin_data);
    } else if (handshake.auth_plugin_name == "caching_sha2_password") {
      auth_response
        = compute_caching_sha2_password(password, handshake.auth_plugin_data);
    }
    write_lenenc_int(packet, auth_response.size());
    packet.insert(packet.end(), auth_response.begin(), auth_response.end());
    // Database (if specified).
    if (not database.empty()) {
      write_null_string(packet, database);
    }
    // Auth plugin name.
    write_null_string(packet, handshake.auth_plugin_name);
    // Send packet.
    auto write_result = co_await write_packet(packet, 1);
    if (write_result.is_err()) {
      co_return Err{std::move(write_result).unwrap_err()};
    }
    // Read response.
    auto response = co_await read_packet();
    if (response.is_err()) {
      co_return Err{std::move(response).unwrap_err()};
    }
    auto resp_data = std::move(response).value();
    if (resp_data.empty()) {
      co_return Err{mysql_error::protocol("empty auth response")};
    }
    // OK packet - authentication successful.
    if (resp_data[0] == 0x00) {
      co_return {};
    }
    // Error packet.
    if (resp_data[0] == 0xff) {
      co_return Err{parse_error_from_packet(resp_data)};
    }
    // Auth switch request.
    if (resp_data[0] == 0xfe) {
      co_return Err{
        mysql_error::protocol("auth switch request not implemented")};
    }
    // caching_sha2_password fast auth success.
    if (resp_data[0] == 0x01 and resp_data.size() > 1
        and resp_data[1] == 0x04) {
      auto final_response = co_await read_packet();
      if (final_response.is_err()) {
        co_return Err{std::move(final_response).unwrap_err()};
      }
      if (final_response.value()[0] == 0x00) {
        co_return {};
      }
      co_return Err{
        mysql_error::protocol("unexpected response after fast auth")};
    }
    co_return Err{mysql_error::protocol(
      fmt::format("unexpected auth response: 0x{:02x}", resp_data[0]))};
  }

private:
  explicit async_connection(folly::coro::Transport transport)
    : transport_{
        std::make_unique<folly::coro::Transport>(std::move(transport))} {
  }
  std::unique_ptr<folly::coro::Transport> transport_;
  std::chrono::milliseconds read_timeout_{std::chrono::seconds{30}};
  uint8_t sequence_id_ = 0;
};

// -- Section 5: Protocol Commands --------------------------------------------

/// MySQL protocol command codes.
namespace cmd {

inline constexpr auto query = uint8_t{0x03};

} // namespace cmd

/// MySQL protocol packet markers.
namespace marker {

inline constexpr auto eof = uint8_t{0xfe};
inline constexpr auto error = uint8_t{0xff};

} // namespace marker

/// Build a COM_QUERY packet.
auto make_query_packet(std::string_view sql) -> std::vector<uint8_t> {
  auto packet = std::vector<uint8_t>{};
  packet.reserve(1 + sql.size());
  packet.push_back(cmd::query);
  packet.insert(packet.end(), sql.begin(), sql.end());
  return packet;
}

/// Check if packet indicates end-of-result-set.
auto is_eof_packet(std::span<uint8_t const> pkt) -> bool {
  return not pkt.empty() and pkt[0] == marker::eof and pkt.size() < 9;
}

/// Check if packet is an error packet. Returns the error if so.
auto check_error_packet(std::span<uint8_t const> data)
  -> std::optional<mysql_error> {
  if (not data.empty() and data[0] == marker::error) {
    return parse_error_from_packet(data);
  }
  return std::nullopt;
}

/// Result set metadata after reading column definitions.
struct result_set_meta {
  std::vector<column_info> columns;
};

/// Read result set metadata (column count + column definitions).
auto read_result_set_meta(async_connection& conn)
  -> Task<MysqlResult<result_set_meta>> {
  auto response = co_await conn.read_packet();
  if (response.is_err()) {
    co_return Err{std::move(response).unwrap_err()};
  }
  if (auto err = check_error_packet(response.value())) {
    co_return Err{std::move(*err)};
  }
  auto count_result = read_lenenc_int(response.value());
  if (count_result.is_err()) {
    co_return Err{std::move(count_result).unwrap_err()};
  }
  auto [column_count, _] = count_result.value();
  auto meta = result_set_meta{};
  meta.columns.reserve(column_count);
  for (auto i = uint64_t{0}; i < column_count; ++i) {
    auto col_packet = co_await conn.read_packet();
    if (col_packet.is_err()) {
      co_return Err{std::move(col_packet).unwrap_err()};
    }
    auto col = parse_column_definition(col_packet.value());
    if (col.is_err()) {
      co_return Err{std::move(col).unwrap_err()};
    }
    meta.columns.push_back(std::move(col).value());
  }
  // Skip EOF packet after column definitions.
  (void)co_await conn.read_packet();
  co_return meta;
}

// -- Section 6: Async Client -------------------------------------------------

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
void parse_row_into_builder(std::span<uint8_t const> data,
                            std::vector<column_info> const& cols,
                            series_builder& builder) {
  auto event = builder.record();
  auto offset = size_t{0};
  for (auto const& col : cols) {
    auto field = event.field(col.name);
    if (offset >= data.size()) {
      field.null();
      continue;
    }
    // NULL value (0xfb).
    if (data[offset] == 0xfb) {
      field.null();
      offset += 1;
      continue;
    }
    auto str_result = read_lenenc_string(data.subspan(offset));
    if (str_result.is_err()) {
      field.null();
      break;
    }
    auto [str, consumed] = std::move(str_result).value();
    offset += consumed;
    // Convert based on column type.
    switch (col.type) {
      case mysql_type::tiny:
      case mysql_type::short_:
      case mysql_type::long_:
      case mysql_type::longlong:
      case mysql_type::int24:
      case mysql_type::year:
        try {
          if (is_unsigned(col)) {
            field.data(static_cast<uint64_t>(std::stoull(str)));
          } else {
            field.data(static_cast<int64_t>(std::stoll(str)));
          }
        } catch (...) {
          field.data(std::move(str));
        }
        break;
      case mysql_type::float_:
      case mysql_type::double_:
      case mysql_type::decimal:
      case mysql_type::newdecimal:
        try {
          field.data(std::stod(str));
        } catch (...) {
          field.data(std::move(str));
        }
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
    -> Task<MysqlResult<std::unique_ptr<async_client>>> {
    TENZIR_ERROR("from_mysql: async_client::make() connecting to {}:{}",
                 config.host, config.port);
    auto conn = async_connection{};
    try {
      conn = co_await async_connection::connect(evb, config.host, config.port);
    } catch (folly::AsyncSocketException const& e) {
      co_return Err{
        mysql_error{0, fmt::format("connect failed: {}", e.what())}};
    }
    TENZIR_ERROR("from_mysql: async_client::make() TCP connected, handshaking");
    auto handshake = co_await conn.perform_handshake();
    if (handshake.is_err()) {
      co_return Err{std::move(handshake).unwrap_err()};
    }
    TENZIR_ERROR("from_mysql: async_client::make() handshake done, "
                 "authenticating as '{}' db='{}'",
                 config.user, config.database);
    auto auth = co_await conn.send_auth_response(
      handshake.value(), config.user, config.password, config.database);
    if (auth.is_err()) {
      co_return Err{std::move(auth).unwrap_err()};
    }
    TENZIR_ERROR("from_mysql: async_client::make() authenticated successfully");
    co_return std::unique_ptr<async_client>{new async_client{std::move(conn)}};
  }

  /// Execute query and stream results as table_slices.
  auto query(query_config cfg) -> AsyncGenerator<MysqlResult<table_slice>> {
    TENZIR_ERROR("from_mysql: query() sending COM_QUERY: {}", cfg.sql);
    // Send COM_QUERY.
    auto packet = make_query_packet(cfg.sql);
    auto write_result = co_await conn_.write_packet(packet, 0);
    if (write_result.is_err()) {
      TENZIR_ERROR("from_mysql: query() write_packet failed");
      co_yield Err{std::move(write_result).unwrap_err()};
      co_return;
    }
    TENZIR_ERROR("from_mysql: query() COM_QUERY sent, reading metadata");
    // Read result set metadata.
    auto meta = co_await read_result_set_meta(conn_);
    if (meta.is_err()) {
      TENZIR_ERROR("from_mysql: query() read_result_set_meta failed");
      co_yield Err{std::move(meta).unwrap_err()};
      co_return;
    }
    auto columns = std::move(meta).value().columns;
    TENZIR_ERROR("from_mysql: query() got {} columns, reading rows",
                 columns.size());
    // Stream rows, building table_slices.
    auto builder = series_builder{};
    auto row_count = int64_t{0};
    while (true) {
      auto row_packet = co_await conn_.read_packet();
      if (row_packet.is_err()) {
        TENZIR_ERROR("from_mysql: query() row read failed");
        co_yield Err{std::move(row_packet).unwrap_err()};
        co_return;
      }
      if (is_eof_packet(row_packet.value())) {
        TENZIR_ERROR("from_mysql: query() got EOF after {} rows", row_count);
        break;
      }
      if (auto err = check_error_packet(row_packet.value())) {
        TENZIR_ERROR("from_mysql: query() got error packet");
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
      TENZIR_ERROR("from_mysql: query() yielding final slice with {} rows",
                   row_count);
      co_yield builder.finish_assert_one_slice(cfg.schema_name);
    }
  }

private:
  explicit async_client(async_connection conn) : conn_{std::move(conn)} {
  }

  async_connection conn_;
};

// -- Section 7: Operator Implementation --------------------------------------

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
    TENZIR_ERROR("from_mysql: start() begin");
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
    if (args_.table) {
      query_ = fmt::format("SELECT * FROM {}", args_.table->inner);
      schema_name_ = fmt::format("mysql.{}", args_.table->inner);
    } else if (args_.sql) {
      query_ = args_.sql->inner;
      schema_name_ = "mysql.query";
    } else if (args_.show) {
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
        query_ = fmt::format("SHOW COLUMNS FROM {}", args_.table->inner);
        schema_name_ = fmt::format("mysql.columns.{}", args_.table->inner);
      } else {
        diagnostic::error("invalid show mode `{}`", args_.show->inner)
          .primary(args_.show->source)
          .hint("expected `tables` or `columns`")
          .emit(ctx);
        done_ = true;
        co_return;
      }
    } else {
      diagnostic::error("no query specified")
        .hint("specify `table`, `sql`, or `show`")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    TENZIR_ERROR("from_mysql: start() query={}, connecting to {}:{}", query_,
                 config.host, config.port);
    // Get EventBase from global IO executor.
    auto* evb = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ERROR("from_mysql: start() got EventBase={}", fmt::ptr(evb));
    // Connect asynchronously.
    auto result = co_await async_client::make(evb, std::move(config));
    if (result.is_err()) {
      auto err = std::move(result).unwrap_err();
      if (err.code != 0) {
        diagnostic::error("MySQL error {}: {}", err.code, err.message).emit(ctx);
      } else {
        diagnostic::error("MySQL connection failed: {}", err.message).emit(ctx);
      }
      done_ = true;
      co_return;
    }
    client_ = std::move(result).value();
    TENZIR_ERROR("from_mysql: start() connected successfully");
  }

  auto await_task() const -> Task<Any> override {
    TENZIR_ERROR("from_mysql: await_task() done_={}", done_);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return Any{query_};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ERROR("from_mysql: process_task() begin, done_={}", done_);
    if (done_) {
      co_return;
    }
    auto query = std::move(result).as<std::string>();
    TENZIR_ERROR("from_mysql: process_task() sending query: {}", query);
    auto slice_stream = client_->query({
      .sql = query,
      .schema_name = schema_name_,
    });
    TENZIR_ERROR("from_mysql: process_task() query stream created, iterating");
    while (auto slice_result = co_await slice_stream.next()) {
      TENZIR_ERROR("from_mysql: process_task() got slice result");
      if (slice_result->is_err()) {
        auto err = std::move(*slice_result).unwrap_err();
        if (err.code != 0) {
          diagnostic::error("MySQL error {}: {}", err.code, err.message)
            .emit(ctx);
        } else {
          diagnostic::error("MySQL error: {}", err.message).emit(ctx);
        }
        break;
      }
      TENZIR_ERROR("from_mysql: process_task() pushing slice");
      co_await push(std::move(*slice_result).value());
    }
    TENZIR_ERROR("from_mysql: process_task() done");
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
    serde("query", query_);
    serde("schema_name", schema_name_);
  }

private:
  FromMySQLArgs args_;
  std::unique_ptr<async_client> client_;
  std::string query_;
  std::string schema_name_;
  bool done_ = false;
};

// -- Section 8: Plugin Registration ------------------------------------------

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
