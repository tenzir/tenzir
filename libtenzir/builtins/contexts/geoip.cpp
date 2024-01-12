//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/fbs/geoip.hpp>
#include <tenzir/fbs/utils.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <maxminddb.h>
#include <memory>
#include <string>

namespace tenzir::plugins::geoip {

namespace {

auto constexpr path_key = "db-path";

#if MMDB_UINT128_IS_BYTE_ARRAY
auto cast_128_bit_unsigned_to_64_bit(uint8_t uint128[16]) -> uint64_t {
  auto low = uint64_t{};
  auto high = uint64_t{};
  std::memcpy(&low, uint128, 8);
  std::memcpy(&high, uint128 + 8, 8);
  if (high != 0) {
    TENZIR_WARN("casting MDDB 128-bit to 64-bit unsigned will be lossy for "
                "value [{},{}]",
                high, low);
  }
  return low;
}
#else
auto cast_128_bit_unsigned_to_64_bit(mmdb_uint128_t uint128) -> uint64_t {
  auto high = static_cast<uint64_t>(uint128 >> 64);
  auto low = static_cast<uint64_t>(uint128);
  if (high != 0) {
    TENZIR_WARN("casting MDDB 128-bit to 64-bit unsigned will be lossy for "
                "value [{},{}]",
                high, low);
  }
  return low;
}
#endif

class ctx final : public virtual context {
public:
  ctx() noexcept = default;

  explicit ctx(context::parameter_map parameters) noexcept {
    update(std::move(parameters));
  }

  ~ctx() override {
    if (mmdb_) {
      MMDB_close(&*mmdb_);
    }
  }

  auto entry_data_list_to_list(MMDB_entry_data_list_s* entry_data_list,
                               int* status, list& l) const
    -> MMDB_entry_data_list_s* {
    switch (entry_data_list->entry_data.type) {
      case MMDB_DATA_TYPE_MAP: {
        auto size = entry_data_list->entry_data.data_size;
        auto sub_r = record{};
        for (entry_data_list = entry_data_list->next; size && entry_data_list;
             size--) {
          if (MMDB_DATA_TYPE_UTF8_STRING != entry_data_list->entry_data.type) {
            *status = MMDB_INVALID_DATA_ERROR;
            return entry_data_list;
          }
          auto sub_record_key
            = std::string{entry_data_list->entry_data.utf8_string,
                          entry_data_list->entry_data.data_size};

          entry_data_list = entry_data_list->next;
          entry_data_list = entry_data_list_to_record(entry_data_list, status,
                                                      sub_r, sub_record_key);
          if (*status != MMDB_SUCCESS) {
            return entry_data_list;
          }
        }
        l.emplace_back(sub_r);
        break;
      }
      case MMDB_DATA_TYPE_ARRAY: {
        auto sub_l = list{};
        auto size = entry_data_list->entry_data.data_size;
        for (entry_data_list = entry_data_list->next; size && entry_data_list;
             size--) {
          entry_data_list
            = entry_data_list_to_list(entry_data_list, status, sub_l);
          if (*status != MMDB_SUCCESS) {
            return entry_data_list;
          }
        }
        l.emplace_back(sub_l);
        break;
      }
      case MMDB_DATA_TYPE_UTF8_STRING: {
        auto str = std::string{entry_data_list->entry_data.utf8_string,
                               entry_data_list->entry_data.data_size};
        l.emplace_back(str);
        entry_data_list = entry_data_list->next;
        break;
      }
      case MMDB_DATA_TYPE_BYTES: {
        auto bytes = blob{
          reinterpret_cast<const std::byte*>(entry_data_list->entry_data.bytes),
          entry_data_list->entry_data.data_size};
        l.emplace_back(std::move(bytes));
        entry_data_list = entry_data_list->next;
        break;
      }
      case MMDB_DATA_TYPE_DOUBLE:
        l.emplace_back(entry_data_list->entry_data.double_value);
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_FLOAT:
        l.emplace_back(entry_data_list->entry_data.float_value);
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT16:
        l.emplace_back(entry_data_list->entry_data.uint16);
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT32:
        l.emplace_back(entry_data_list->entry_data.uint32);
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_BOOLEAN:
        l.emplace_back(entry_data_list->entry_data.boolean);
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT64:
        l.emplace_back(entry_data_list->entry_data.uint64);
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT128:
        l.emplace_back(
          cast_128_bit_unsigned_to_64_bit(entry_data_list->entry_data.uint128));
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_INT32:
        l.emplace_back(int64_t{entry_data_list->entry_data.int32});
        entry_data_list = entry_data_list->next;
        break;
      default:
        *status = MMDB_INVALID_DATA_ERROR;
        return entry_data_list;
    }
    *status = MMDB_SUCCESS;
    return entry_data_list;
  }

  auto
  entry_data_list_to_record(MMDB_entry_data_list_s* entry_data_list,
                            int* status, record& r, std::string key = {}) const
    -> MMDB_entry_data_list_s* {
    switch (entry_data_list->entry_data.type) {
      case MMDB_DATA_TYPE_MAP: {
        auto size = entry_data_list->entry_data.data_size;

        for (entry_data_list = entry_data_list->next; size && entry_data_list;
             size--) {
          if (MMDB_DATA_TYPE_UTF8_STRING != entry_data_list->entry_data.type) {
            *status = MMDB_INVALID_DATA_ERROR;
            return entry_data_list;
          }
          auto sub_record_key
            = std::string{entry_data_list->entry_data.utf8_string,
                          entry_data_list->entry_data.data_size};
          auto sub_r = record{};
          entry_data_list = entry_data_list->next;
          entry_data_list = entry_data_list_to_record(entry_data_list, status,
                                                      sub_r, sub_record_key);
          if (*status != MMDB_SUCCESS) {
            return entry_data_list;
          }
          if (sub_r.size() == 1) {
            // Fuse values of sub-records that belong to the parent record with
            // the parent record. MMDB recursive map iteration idiosyncracy.
            r[sub_record_key] = sub_r.begin()->second;
          } else if (not sub_r.empty()) {
            r[sub_record_key] = sub_r;
          }
        }
        break;
      }
      case MMDB_DATA_TYPE_ARRAY: {
        auto l = list{};
        auto size = entry_data_list->entry_data.data_size;
        auto sub_r = record{};
        for (entry_data_list = entry_data_list->next; size && entry_data_list;
             size--) {
          entry_data_list = entry_data_list_to_list(entry_data_list, status, l);
          if (*status != MMDB_SUCCESS) {
            return entry_data_list;
          }
        }
        r[key] = l;
        break;
      }
      case MMDB_DATA_TYPE_UTF8_STRING: {
        auto str = std::string{entry_data_list->entry_data.utf8_string,
                               entry_data_list->entry_data.data_size};
        r[key] = str;
        entry_data_list = entry_data_list->next;
        break;
      }
      case MMDB_DATA_TYPE_BYTES: {
        auto bytes = blob{
          reinterpret_cast<const std::byte*>(entry_data_list->entry_data.bytes),
          entry_data_list->entry_data.data_size};
        r[key] = std::move(bytes);
        entry_data_list = entry_data_list->next;
        break;
      }
      case MMDB_DATA_TYPE_DOUBLE:
        r[key] = entry_data_list->entry_data.double_value;
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_FLOAT:
        r[key] = entry_data_list->entry_data.float_value;
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT16:
        r[key] = entry_data_list->entry_data.uint16;
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT32:
        r[key] = entry_data_list->entry_data.uint32;
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_BOOLEAN:
        r[key] = entry_data_list->entry_data.boolean;
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT64:
        r[key] = entry_data_list->entry_data.uint64;
        entry_data_list = entry_data_list->next;
        break;
      case MMDB_DATA_TYPE_UINT128:
        r[key] = cast_128_bit_unsigned_to_64_bit(
          entry_data_list->entry_data.uint128);
        break;
      case MMDB_DATA_TYPE_INT32:
        r[key] = int64_t{entry_data_list->entry_data.int32};
        entry_data_list = entry_data_list->next;
        break;
      default:
        *status = MMDB_INVALID_DATA_ERROR;
        return entry_data_list;
    }
    *status = MMDB_SUCCESS;
    return entry_data_list;
  }

  /// Emits context information for every event in `slice` in order.
  auto apply(table_slice slice, context::parameter_map parameters) const
    -> caf::expected<std::vector<typed_array>> override {
    auto status = 0;
    MMDB_entry_data_list_s* entry_data_list = nullptr;
    auto resolved_slice = resolve_enumerations(slice);
    auto field_name = std::optional<std::string>{};
    for (const auto& [key, value] : parameters) {
      if (key == "field") {
        if (not value) {
          return caf::make_error(ec::invalid_argument,
                                 "invalid argument type for `field`: expected "
                                 "a string");
        }
        field_name = *value;
        continue;
      }
      return caf::make_error(ec::invalid_argument,
                             fmt::format("invalid argument `{}`", key));
    }
    if (not field_name) {
      return caf::make_error(ec::invalid_argument, "missing argument `field`");
    }
    auto field_builder = series_builder{};
    auto column_offset = slice.schema().resolve_key_or_concept(*field_name);
    if (not column_offset) {
      for (auto i = size_t{0}; i < slice.rows(); ++i) {
        field_builder.null();
      }
      return field_builder.finish();
    }
    auto [slice_type, slice_array] = column_offset->get(resolved_slice);
    if (slice_type != type{ip_type{}} and slice_type != type{string_type{}}) {
      // No ip type = no enrichment.
      field_builder.null();
      return field_builder.finish();
    }
    for (const auto& value : values(slice_type, *slice_array)) {
      auto address_info_error = 0;
      auto ip_string = fmt::to_string(value);
      if (slice_type == type{string_type{}}) {
        // Unquote IP strings.
        ip_string.erase(0, 1);
        ip_string.erase(ip_string.size() - 1);
      }
      auto result = MMDB_lookup_string(&*mmdb_, ip_string.data(),
                                       &address_info_error, &status);
      if (address_info_error != MMDB_SUCCESS) {
        return caf::make_error(
          ec::lookup_error,
          fmt::format("error looking up IP address '{}' in "
                      "GeoIP database: {}",
                      ip_string, gai_strerror(address_info_error)));
      }
      if (status != MMDB_SUCCESS) {
        return caf::make_error(
          ec::lookup_error, fmt::format("error looking up IP address '{}' in "
                                        "GeoIP database: {}",
                                        ip_string, MMDB_strerror(status)));
      }
      if (result.found_entry) {
        status = MMDB_get_entry_data_list(&result.entry, &entry_data_list);
        auto free_entry_data_list = caf::detail::make_scope_guard([&] {
          if (entry_data_list) {
            MMDB_free_entry_data_list(entry_data_list);
          }
        });
        if (status != MMDB_SUCCESS) {
          return caf::make_error(
            ec::lookup_error, fmt::format("error looking up IP address '{}' in "
                                          "GeoIP database: {}",
                                          ip_string, MMDB_strerror(status)));
        }
        auto* entry_data_list_it = entry_data_list;
        auto output = record{};
        entry_data_list_it
          = entry_data_list_to_record(entry_data_list_it, &status, output);
        if (status != MMDB_SUCCESS) {
          return caf::make_error(
            ec::lookup_error, fmt::format("error looking up IP address '{}' in "
                                          "GeoIP database: {}",
                                          ip_string, MMDB_strerror(status)));
        }
        auto r = field_builder.record();
        r.field("address", value);
        r.field("context", output);
        r.field("timestamp", std::chrono::system_clock::now());
      } else {
        field_builder.null();
      }
    }
    return field_builder.finish();
  }

  /// Inspects the context.
  auto show() const -> record override {
    return record{{path_key, db_path_}};
  }

  /// Updates the context.
  auto update(table_slice, context::parameter_map)
    -> caf::expected<update_result> override {
    return caf::make_error(ec::unimplemented,
                           "geoip context can not be updated with events");
  }

  auto update(chunk_ptr, context::parameter_map)
    -> caf::expected<update_result> override {
    return caf::make_error(ec::unimplemented, "geoip context can not be "
                                              "updated with bytes");
  }

  auto update(context::parameter_map parameters)
    -> caf::expected<update_result> override {
    if (parameters.contains(path_key) and parameters.at(path_key)) {
      db_path_ = *parameters[path_key];
    }
    if (mmdb_) {
      MMDB_close(&*mmdb_);
    } else {
      mmdb_ = MMDB_s{};
    }
    auto status = MMDB_open(db_path_.c_str(), MMDB_MODE_MMAP, &*mmdb_);
    if (status != MMDB_SUCCESS) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("error opening IP database at path "
                                         "'{}': {}",
                                         db_path_, MMDB_strerror(status)));
    }
    return update_result{.update_info = show(), .make_query = {}};
  }

  auto snapshot(parameter_map) const -> caf::expected<expression> override {
    return caf::make_error(ec::unimplemented,
                           "geoip context does not support snapshots");
  }

  auto save() const -> caf::expected<chunk_ptr> override {
    auto builder = flatbuffers::FlatBufferBuilder{};
    auto path = builder.CreateString(db_path_);
    fbs::context::geoip::GeoIPDataBuilder geoip_builder(builder);
    geoip_builder.add_url(path);
    auto geoip_data = geoip_builder.Finish();
    fbs::context::geoip::FinishGeoIPDataBuffer(builder, geoip_data);
    return tenzir::fbs::release(builder);
  }

private:
  std::string db_path_;
  record r_;
  std::optional<MMDB_s> mmdb_;
};

class plugin : public virtual context_plugin {
  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "geoip";
  }

  auto make_context(context::parameter_map parameters) const
    -> caf::expected<std::unique_ptr<context>> override {
    return std::make_unique<ctx>(std::move(parameters));
  }

  auto load_context(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> override {
    const auto* serialized_data
      = fbs::context::geoip::GetGeoIPData(serialized->data());
    if (not serialized_data) {
      return caf::make_error(ec::serialization_error,
                             fmt::format("failed to deserialize geoip "
                                         "context: invalid file content"));
    }
    const auto* serialized_string = serialized_data->url();
    if (not serialized_string) {
      return caf::make_error(ec::serialization_error,
                             "failed to deserialize geoip "
                             "context: invalid type or value for "
                             "DB path entry");
    }
    context::parameter_map params;
    params[path_key] = serialized_string->str();
    return std::make_unique<ctx>(std::move(params));
  }
};

} // namespace

} // namespace tenzir::plugins::geoip

TENZIR_REGISTER_PLUGIN(tenzir::plugins::geoip::plugin)
