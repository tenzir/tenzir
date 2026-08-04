//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/string.hpp"

#include <fmt/format.h>

#include <bit>
#include <chrono>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <tuple>

#include "detail.hpp"

namespace tenzir::netflow::detail {

namespace {

constexpr auto ixia_elements = std::array{
  EnterpriseElement{110, "ixia_l7_application_id",
                    InformationElementType::unsigned32},
  EnterpriseElement{111, "ixia_l7_application_name",
                    InformationElementType::string},
  EnterpriseElement{120, "ixia_source_ip_country_code",
                    InformationElementType::string},
  EnterpriseElement{121, "ixia_source_ip_country_name",
                    InformationElementType::string},
  EnterpriseElement{122, "ixia_source_ip_region_code",
                    InformationElementType::string},
  EnterpriseElement{123, "ixia_source_ip_region_name",
                    InformationElementType::string},
  EnterpriseElement{125, "ixia_source_ip_city_name",
                    InformationElementType::string},
  EnterpriseElement{126, "ixia_source_ip_city_latitude",
                    InformationElementType::float32},
  EnterpriseElement{127, "ixia_source_ip_city_longitude",
                    InformationElementType::float32},
  EnterpriseElement{140, "ixia_destination_ip_country_code",
                    InformationElementType::string},
  EnterpriseElement{141, "ixia_destination_ip_country_name",
                    InformationElementType::string},
  EnterpriseElement{142, "ixia_destination_ip_region_code",
                    InformationElementType::string},
  EnterpriseElement{143, "ixia_destination_ip_region_name",
                    InformationElementType::string},
  EnterpriseElement{145, "ixia_destination_ip_city_name",
                    InformationElementType::string},
  EnterpriseElement{146, "ixia_destination_ip_city_latitude",
                    InformationElementType::float32},
  EnterpriseElement{147, "ixia_destination_ip_city_longitude",
                    InformationElementType::float32},
  EnterpriseElement{160, "ixia_os_device_id",
                    InformationElementType::unsigned8},
  EnterpriseElement{161, "ixia_os_device_name", InformationElementType::string},
  EnterpriseElement{162, "ixia_browser_id", InformationElementType::unsigned8},
  EnterpriseElement{163, "ixia_browser_name", InformationElementType::string},
  EnterpriseElement{176, "ixia_reverse_octet_delta_count",
                    InformationElementType::unsigned64},
  EnterpriseElement{177, "ixia_reverse_packet_delta_count",
                    InformationElementType::unsigned64},
  EnterpriseElement{178, "ixia_ssl_connection_encryption_type",
                    InformationElementType::string},
  EnterpriseElement{179, "ixia_ssl_encryption_cipher_name",
                    InformationElementType::string},
  EnterpriseElement{180, "ixia_ssl_encryption_key_length",
                    InformationElementType::unsigned16},
  EnterpriseElement{182, "ixia_user_agent", InformationElementType::string},
  EnterpriseElement{183, "ixia_host_name", InformationElementType::string},
  EnterpriseElement{184, "ixia_uri", InformationElementType::string},
  EnterpriseElement{185, "ixia_dns_text", InformationElementType::string},
  EnterpriseElement{186, "ixia_source_as_name", InformationElementType::string},
  EnterpriseElement{187, "ixia_destination_as_name",
                    InformationElementType::string},
  EnterpriseElement{188, "ixia_transaction_latency",
                    InformationElementType::unsigned32},
  EnterpriseElement{189, "ixia_dns_query_hostname",
                    InformationElementType::string},
  EnterpriseElement{190, "ixia_dns_response_hostname",
                    InformationElementType::string},
  EnterpriseElement{191, "ixia_dns_classes", InformationElementType::string},
  EnterpriseElement{192, "ixia_threat_type", InformationElementType::string},
  EnterpriseElement{193, "ixia_threat_ipv4",
                    InformationElementType::ipv4_address},
  EnterpriseElement{194, "ixia_threat_ipv6",
                    InformationElementType::ipv6_address},
  EnterpriseElement{195, "ixia_http_session",
                    InformationElementType::sub_template_list},
  EnterpriseElement{196, "ixia_request_time",
                    InformationElementType::unsigned32},
  EnterpriseElement{197, "ixia_dns_record",
                    InformationElementType::sub_template_list},
  EnterpriseElement{198, "ixia_dns_name", InformationElementType::string},
  EnterpriseElement{199, "ixia_dns_ipv4_address",
                    InformationElementType::ipv4_address},
  EnterpriseElement{200, "ixia_dns_ipv6_address",
                    InformationElementType::ipv6_address},
  EnterpriseElement{201, "ixia_sni", InformationElementType::string},
  EnterpriseElement{457, "ixia_http_status_code",
                    InformationElementType::unsigned16},
  EnterpriseElement{459, "ixia_http_request_method",
                    InformationElementType::string},
  EnterpriseElement{462, "ixia_http_message_version",
                    InformationElementType::string},
};

auto as_blob(std::span<const std::byte> bytes) -> data {
  return blob{bytes.begin(), bytes.end()};
}

auto unsigned_value(std::span<const std::byte> bytes) -> Option<uint64_t> {
  auto cursor = Cursor{bytes};
  auto result = uint64_t{0};
  if (bytes.empty() or bytes.size() > 8
      or not cursor.read_u64(result, bytes.size())) {
    return None{};
  }
  return result;
}

auto signed_value(std::span<const std::byte> bytes) -> Option<int64_t> {
  auto value = unsigned_value(bytes);
  if (not value) {
    return None{};
  }
  if (bytes.size() < 8
      and (std::to_integer<uint8_t>(bytes.front()) & uint8_t{0x80}) != 0) {
    *value |= std::numeric_limits<uint64_t>::max() << (bytes.size() * 8);
  }
  return std::bit_cast<int64_t>(*value);
}

template <class Unit>
auto decode_unix_time(std::span<const std::byte> bytes, size_t max_length)
  -> Option<time> {
  auto value = unsigned_value(bytes);
  if (not value or bytes.size() > max_length) {
    return None{};
  }
  auto const max_count
    = std::chrono::duration_cast<Unit>(duration::max()).count();
  if (*value > static_cast<uint64_t>(max_count)) {
    return None{};
  }
  return time{} + Unit{static_cast<int64_t>(*value)};
}

auto decode_unix_seconds(std::span<const std::byte> bytes,
                         int64_t reference_unix_seconds) -> Option<time> {
  auto value = unsigned_value(bytes);
  if (not value or bytes.size() > 4) {
    return None{};
  }
  return make_time(
    unfold_seconds(static_cast<uint32_t>(*value), reference_unix_seconds));
}

auto decode_time_ntp(std::span<const std::byte> bytes,
                     bool microsecond_precision, int64_t reference_unix_seconds)
  -> Option<time> {
  if (bytes.size() != 8) {
    return None{};
  }
  auto cursor = Cursor{bytes};
  auto seconds = uint32_t{0};
  auto fraction = uint32_t{0};
  if (not cursor.read_u32(seconds) or not cursor.read_u32(fraction)) {
    return None{};
  }
  if (microsecond_precision) {
    fraction &= ~uint32_t{0x7ff};
  }
  auto fractional_nanoseconds
    = (uint64_t{fraction} * uint64_t{1'000'000'000}) >> 32;
  if (microsecond_precision) {
    fractional_nanoseconds = fractional_nanoseconds / 1000 * 1000;
  }
  auto const reference_ntp_seconds
    = reference_unix_seconds + ntp_to_unix_seconds;
  auto const unfolded_seconds = unfold_seconds(seconds, reference_ntp_seconds);
  return make_time(unfolded_seconds - ntp_to_unix_seconds,
                   static_cast<int64_t>(fractional_nanoseconds));
}

} // namespace

auto standard_element(uint16_t id) -> InformationElement const* {
  auto position = std::ranges::lower_bound(information_elements, id, {},
                                           &InformationElement::id);
  if (position == information_elements.end() or position->id != id) {
    return nullptr;
  }
  return &*position;
}

auto ixia_element(uint16_t id) -> EnterpriseElement const* {
  auto position = std::ranges::find(ixia_elements, id, &EnterpriseElement::id);
  return position == ixia_elements.end() ? nullptr : &*position;
}

auto decode_value(InformationElementType type, std::span<const std::byte> bytes,
                  int64_t reference_unix_seconds) -> data {
  switch (type) {
    case InformationElementType::octet_array:
    case InformationElementType::basic_list:
    case InformationElementType::sub_template_list:
    case InformationElementType::sub_template_multi_list:
      return as_blob(bytes);
    case InformationElementType::unsigned8:
    case InformationElementType::unsigned16:
    case InformationElementType::unsigned32:
    case InformationElementType::unsigned64:
      if (auto value = unsigned_value(bytes)) {
        return *value;
      }
      return as_blob(bytes);
    case InformationElementType::signed8:
    case InformationElementType::signed16:
    case InformationElementType::signed32:
    case InformationElementType::signed64:
      if (auto value = signed_value(bytes)) {
        return *value;
      }
      return as_blob(bytes);
    case InformationElementType::float32:
      if (bytes.size() == 4) {
        auto cursor = Cursor{bytes};
        auto bits = uint32_t{0};
        std::ignore = cursor.read_u32(bits);
        return double{std::bit_cast<float>(bits)};
      }
      return as_blob(bytes);
    case InformationElementType::float64:
      if (bytes.size() == 8) {
        auto cursor = Cursor{bytes};
        auto bits = uint64_t{0};
        std::ignore = cursor.read_u64(bits, 8);
        return std::bit_cast<double>(bits);
      }
      return as_blob(bytes);
    case InformationElementType::boolean:
      if (bytes.size() == 1) {
        auto const value = std::to_integer<uint8_t>(bytes.front());
        if (value == 1) {
          return true;
        }
        if (value == 2) {
          return false;
        }
      }
      return as_blob(bytes);
    case InformationElementType::mac_address:
      if (bytes.size() == 6) {
        return fmt::format("{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                           std::to_integer<uint8_t>(bytes[0]),
                           std::to_integer<uint8_t>(bytes[1]),
                           std::to_integer<uint8_t>(bytes[2]),
                           std::to_integer<uint8_t>(bytes[3]),
                           std::to_integer<uint8_t>(bytes[4]),
                           std::to_integer<uint8_t>(bytes[5]));
      }
      return as_blob(bytes);
    case InformationElementType::string: {
      auto const original = bytes;
      while (not bytes.empty() and bytes.back() == std::byte{0}) {
        bytes = bytes.first(bytes.size() - 1);
      }
      if (bytes.empty()) {
        return std::string{};
      }
      auto const value = std::string_view{
        reinterpret_cast<char const*>(bytes.data()), bytes.size()};
      return tenzir::detail::is_valid_utf8(value) ? data{std::string{value}}
                                                  : as_blob(original);
    }
    case InformationElementType::date_time_seconds:
      if (auto value = decode_unix_seconds(bytes, reference_unix_seconds)) {
        return *value;
      }
      return as_blob(bytes);
    case InformationElementType::date_time_milliseconds:
      if (auto value = decode_unix_time<std::chrono::milliseconds>(bytes, 8)) {
        return *value;
      }
      return as_blob(bytes);
    case InformationElementType::date_time_microseconds:
      if (auto value = decode_time_ntp(bytes, true, reference_unix_seconds)) {
        return *value;
      }
      return as_blob(bytes);
    case InformationElementType::date_time_nanoseconds:
      if (auto value = decode_time_ntp(bytes, false, reference_unix_seconds)) {
        return *value;
      }
      return as_blob(bytes);
    case InformationElementType::ipv4_address:
      if (bytes.size() == 4) {
        return ip::v4(bytes.first<4>());
      }
      return as_blob(bytes);
    case InformationElementType::ipv6_address:
      if (bytes.size() == 16) {
        return ip::v6(bytes.first<16>());
      }
      return as_blob(bytes);
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir::netflow::detail
