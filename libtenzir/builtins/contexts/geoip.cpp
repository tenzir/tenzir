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
#include <tenzir/fbs/data.hpp>
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

auto constexpr path_key = "db_path";

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
    auto mmdb = MMDB_s{};
    auto status = MMDB_open(db_path_.c_str(), MMDB_MODE_MMAP, &mmdb);
    if (status != MMDB_SUCCESS) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("error opening IP database at path "
                                         "'{}': {}",
                                         db_path_, MMDB_strerror(status)));
    }
    MMDB_entry_data_list_s* entry_data_list = nullptr;
    auto close_mmdb = caf::detail::make_scope_guard([&] {
      if (entry_data_list) {
        MMDB_free_entry_data_list(entry_data_list);
      }
      MMDB_close(&mmdb);
    });
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
    auto column_offset
      = caf::get<record_type>(slice.schema()).resolve_key(*field_name);
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
      auto result = MMDB_lookup_string(&mmdb, ip_string.data(),
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
        r.field("key", value);
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
    -> caf::expected<record> override {
    return caf::make_error(ec::unimplemented,
                           "geoip context can not be updated with events");
  }

  auto update(chunk_ptr, context::parameter_map)
    -> caf::expected<record> override {
    return caf::make_error(ec::unimplemented, "geoip context can not be "
                                              "updated with bytes");
  }

  auto update(context::parameter_map parameters)
    -> caf::expected<record> override {
    if (parameters.contains(path_key) and parameters.at(path_key)) {
      db_path_ = *parameters[path_key];
    }
    return show();
  }

  auto save() const -> caf::expected<chunk_ptr> override {
    // We save the context by formatting into a record of this format:
    // {path: string}
    auto builder = flatbuffers::FlatBufferBuilder{};
    const auto key_key_offset = builder.CreateSharedString(path_key);
    const auto key_value_offset = pack(builder, data{db_path_});
    auto field_offsets
      = std::vector<flatbuffers::Offset<fbs::data::RecordField>>{};
    field_offsets.reserve(1);
    const auto record_offset
      = fbs::data::CreateRecordField(builder, key_key_offset, key_value_offset);
    field_offsets.emplace_back(record_offset);
    const auto value_offset
      = fbs::data::CreateRecordDirect(builder, &field_offsets);
    const auto data_offset
      = fbs::CreateData(builder, fbs::data::Data::record, value_offset.Union());
    fbs::FinishDataBuffer(builder, data_offset);
    return chunk::make(builder.Release());
  }

private:
  std::string db_path_;
  record r;
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
    auto fb = flatbuffer<fbs::Data>::make(std::move(serialized));
    if (not fb) {
      return caf::make_error(ec::serialization_error,
                             fmt::format("failed to deserialize geoip "
                                         "context: {}",
                                         fb.error()));
    }
    const auto* record = fb.value()->data_as_record();
    if (not record) {
      return caf::make_error(ec::serialization_error,
                             "failed to deserialize geoip "
                             "context: invalid type for "
                             "context entry, entry must be a record");
    }
    if (not record->fields() or record->fields()->size() != 1) {
      return caf::make_error(ec::serialization_error,
                             "failed to deserialize geoip "
                             "context: invalid or missing value for "
                             "context entry, entry must be a record {key, "
                             "value}");
    }
    data value;
    context::parameter_map params;
    if (record->fields()->Get(0)->name()->str() != path_key) {
      return caf::make_error(
        ec::serialization_error,
        fmt::format("failed to deserialize geoip context"
                    ": invalid key '{}' ",
                    record->fields()->Get(0)->name()->str()));
    }
    auto err = unpack(*record->fields()->Get(0)->data(), value);
    if (err) {
      return caf::make_error(ec::serialization_error,
                             fmt::format("failed to deserialize geoip"
                                         "context: invalid value for key '{}': "
                                         "{}",
                                         path_key, err));
    }
    params[path_key] = caf::get<std::string>(value);
    return std::make_unique<ctx>(std::move(params));
  }
};

} // namespace

} // namespace tenzir::plugins::geoip

TENZIR_REGISTER_PLUGIN(tenzir::plugins::geoip::plugin)
