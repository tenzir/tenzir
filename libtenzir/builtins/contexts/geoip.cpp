//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/error.hpp>
#include <tenzir/fbs/geoip.hpp>
#include <tenzir/fbs/utils.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/type.hpp>
#include <tenzir/uuid.hpp>
#include <tenzir/view.hpp>

#include <fmt/format.h>

#include <cstdint>
#include <maxminddb.h>
#include <memory>
#include <string>
#include <utility>

namespace tenzir::plugins::geoip {

namespace {

auto constexpr path_key = "db-path";

struct mmdb_deleter final {
  auto operator()(MMDB_s* ptr) noexcept -> void {
    if (ptr) {
      MMDB_close(ptr);
      // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
      delete ptr;
    }
  }
};

using mmdb_ptr = std::unique_ptr<MMDB_s, mmdb_deleter>;
using deleter_type = detail::unique_function<void() noexcept>;

auto make_mmdb(const std::string& path) -> caf::expected<mmdb_ptr> {
  if (!std::filesystem::exists(path)) {
    return diagnostic::error("")
      .note("failed to find path `{}`", path)
      .to_error();
  }
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
  auto* ptr = new MMDB_s;
  const auto status = MMDB_open(path.c_str(), MMDB_MODE_MMAP, ptr);
  if (status != MMDB_SUCCESS) {
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    delete ptr;
    return diagnostic::error("{}", MMDB_strerror(status))
      .note("failed to open MaxMind database at `{}`", path)
      .to_error();
  }
  return mmdb_ptr{ptr};
};

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

struct current_dump {
  std::set<uint64_t> visited = {};
  int status = MMDB_SUCCESS;
  series_builder builder;
};

class ctx final : public virtual context {
public:
  ctx() noexcept = default;

  explicit ctx(mmdb_ptr mmdb, chunk_ptr mapped_mmdb_)
    : mapped_mmdb_{std::move(mapped_mmdb_)}, mmdb_{std::move(mmdb)} {
  }

  auto context_type() const -> std::string override {
    return "geoip";
  }

  auto entry_data_list_to_list(MMDB_entry_data_list_s* entry_data_list,
                               int* status, list& l) const
    -> MMDB_entry_data_list_s* {
    switch (entry_data_list->entry_data.type) {
      case MMDB_DATA_TYPE_MAP: {
        auto size = entry_data_list->entry_data.data_size;
        auto sub_r = record{};
        for (entry_data_list = entry_data_list->next;
             size > 0 && entry_data_list; size--) {
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
        for (entry_data_list = entry_data_list->next;
             size > 0 && entry_data_list; size--) {
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

  auto entry_data_list_to_record(MMDB_entry_data_list_s* entry_data_list,
                                 int* status, record& r,
                                 const std::string& key = {}) const
    -> MMDB_entry_data_list_s* {
    switch (entry_data_list->entry_data.type) {
      case MMDB_DATA_TYPE_MAP: {
        auto size = entry_data_list->entry_data.data_size;

        for (entry_data_list = entry_data_list->next;
             size > 0 && entry_data_list; size--) {
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
        for (entry_data_list = entry_data_list->next;
             size > 0 && entry_data_list; size--) {
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
  auto apply(series array, bool replace)
    -> caf::expected<std::vector<series>> override {
    if (!mmdb_) {
      return caf::make_error(ec::lookup_error,
                             fmt::format("no GeoIP data currently exists for "
                                         "this context"));
    }
    auto status = 0;
    MMDB_entry_data_list_s* entry_data_list = nullptr;
    auto builder = series_builder{};
    if (not caf::holds_alternative<ip_type>(array.type)
        and not caf::holds_alternative<string_type>(array.type)) {
      return caf::make_error(ec::lookup_error,
                             fmt::format("error looking up IP address in "
                                         "GeoIP database: invalid column "
                                         "type, only IP or string types are "
                                         "allowed"));
    }
    const auto is_ip = caf::holds_alternative<ip_type>(array.type);
    for (const auto& value : array.values()) {
      if (caf::holds_alternative<caf::none_t>(value)) {
        builder.null();
        continue;
      }
      auto address_info_error = 0;
      const auto ip_string = is_ip
                               ? fmt::to_string(value)
                               : materialize(caf::get<std::string_view>(value));
      auto result = MMDB_lookup_string(mmdb_.get(), ip_string.data(),
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
      if (not result.found_entry) {
        if (replace and not caf::holds_alternative<caf::none_t>(value)) {
          builder.data(value);
        } else {
          builder.null();
        }
        continue;
      }
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
      builder.data(output);
    }
    return builder.finish();
  }

  /// Inspects the context.
  auto show() const -> record override {
    return {};
  }

  auto dump_recurse(uint64_t node_number, uint8_t type, MMDB_entry_s* entry,
                    current_dump* current_dump) -> generator<table_slice> {
    if (current_dump->visited.contains(node_number)) {
      co_return;
    }
    current_dump->visited.emplace(node_number);
    switch (type) {
      case MMDB_RECORD_TYPE_SEARCH_NODE: {
        MMDB_search_node_s search_node{};
        current_dump->status
          = MMDB_read_node(mmdb_.get(), node_number, &search_node);
        if (current_dump->status != MMDB_SUCCESS) {
          break;
        }
        for (auto&& x :
             dump_recurse(search_node.left_record, search_node.left_record_type,
                          &search_node.left_record_entry, current_dump)) {
          if (current_dump->status != MMDB_SUCCESS) {
            break;
          }
          co_yield x;
        }
        for (auto&& x : dump_recurse(
               search_node.right_record, search_node.right_record_type,
               &search_node.right_record_entry, current_dump)) {
          if (current_dump->status != MMDB_SUCCESS) {
            break;
          }
          co_yield x;
        }
        break;
      }
      case MMDB_RECORD_TYPE_EMPTY: {
        // Stop search
        break;
      }
      case MMDB_RECORD_TYPE_DATA: {
        TENZIR_ASSERT(entry != nullptr);
        MMDB_entry_data_list_s* entry_data_list = nullptr;
        current_dump->status
          = MMDB_get_entry_data_list(entry, &entry_data_list);
        if (current_dump->status != MMDB_SUCCESS) {
          break;
        }
        auto free_entry_data_list = caf::detail::make_scope_guard([&] {
          if (entry_data_list) {
            MMDB_free_entry_data_list(entry_data_list);
          }
        });
        auto output = list{};
        entry_data_list_to_list(entry_data_list, &current_dump->status, output);
        if (current_dump->status != MMDB_SUCCESS) {
          break;
        }
        for (auto& x : output) {
          current_dump->builder.data(x);
          if (current_dump->builder.length()
              >= context::dump_batch_size_limit) {
            for (auto&& slice : current_dump->builder.finish_as_table_slice(
                   fmt::format("tenzir.{}.info", context_type()))) {
              co_yield std::move(slice);
            }
          }
        }
        break;
      }
      case MMDB_RECORD_TYPE_INVALID: {
        current_dump->status = MMDB_INVALID_DATA_ERROR;
        break;
      }
    }
  }

  auto dump() -> generator<table_slice> override {
    TENZIR_ASSERT(mmdb_);
    current_dump current_dump;
    for (auto&& slice : dump_recurse(0, MMDB_RECORD_TYPE_SEARCH_NODE, nullptr,
                                     &current_dump)) {
      co_yield slice;
    }
    // Dump all remaining entries that did not reach the size limit.
    for (auto&& slice : current_dump.builder.finish_as_table_slice(
           fmt::format("tenzir.{}.info", context_type()))) {
      co_yield std::move(slice);
    }
    if (current_dump.status != MMDB_SUCCESS) {
      TENZIR_ERROR("dump of GeoIP context ended prematurely: {}",
                   MMDB_strerror(current_dump.status));
    }
  }

  /// Updates the context.
  auto update(table_slice, context::parameter_map)
    -> caf::expected<update_result> override {
    return caf::make_error(ec::unimplemented,
                           "geoip context can not be updated with events");
  }

  auto make_query() -> make_query_type override {
    return {};
  }

  auto reset() -> caf::expected<void> override {
    return {};
  }

  auto save() const -> caf::expected<save_result> override {
    if (!mapped_mmdb_) {
      return caf::make_error(ec::lookup_error,
                             fmt::format("no GeoIP data currently exists for "
                                         "this context"));
    }
    return save_result{.data = mapped_mmdb_, .version = latest_version};
  }

private:
  chunk_ptr mapped_mmdb_;
  mmdb_ptr mmdb_;
  int latest_version = 2;
};

struct v1_loader : public context_loader {
  auto version() const -> int {
    return 1;
  }

  auto load(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> {
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
    const auto* plugin = plugins::find<context_plugin>("geoip");
    TENZIR_ASSERT(plugin);
    return plugin->make_context({{path_key, serialized_string->str()}});
  }
};

struct v2_loader : public context_loader {
  explicit v2_loader(record global_config)
    : global_config_{std::move(global_config)} {
  }

  auto version() const -> int {
    return 2;
  }

  auto load(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> {
    const auto* cache_dir
      = get_if<std::string>(&global_config_, "tenzir.cache-directory");
    TENZIR_ASSERT(cache_dir);
    auto dir_identifier = *cache_dir + "/plugins/geoip=";
    std::error_code ec{};
    std::filesystem::create_directories(dir_identifier, ec);
    if (ec.value() != 0) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to make a tmp directory on "
                                         "data load: {}",
                                         ec.value()));
    }
    std::string temp_file_name
      = dir_identifier + fmt::to_string(uuid::random());
    auto temp_file = std::fstream(temp_file_name, std::ios_base::out);
    if (!temp_file) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to open temp file on "
                                         "data load: {}",
                                         detail::describe_errno()));
    }
    temp_file.write(reinterpret_cast<const char*>(serialized->data()),
                    static_cast<std::streamsize>(serialized->size()));
    temp_file.flush();
    if (!temp_file) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed write the temp file "
                                         "on data load: {}",
                                         detail::describe_errno()));
    }
    auto mmdb = make_mmdb(temp_file_name);
    if (not mmdb) {
      return mmdb.error();
    }
    auto mapped_mmdb = chunk::mmap(temp_file_name);
    temp_file.close();
    if (!temp_file) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed close the temp file: {}",
                                         detail::describe_errno()));
    }
    std::filesystem::remove(temp_file_name);
    return std::make_unique<ctx>(std::move(*mmdb),
                                 std::move(mapped_mmdb.value()));
  }

private:
  const record global_config_;
};

class plugin : public virtual context_plugin {
  auto initialize(const record&, const record& global_config)
    -> caf::error override {
    register_loader(std::make_unique<v1_loader>());
    register_loader(std::make_unique<v2_loader>(global_config));
    return caf::none;
  }

  auto name() const -> std::string override {
    return "geoip";
  }

  auto make_context(context::parameter_map parameters) const
    -> caf::expected<std::unique_ptr<context>> override {
    auto db_path = std::string{};
    for (const auto& [key, value] : parameters) {
      if (key == path_key) {
        if (not value) {
          return diagnostic::error("missing value for option `{}`", key)
            .usage("context create <name> geoip --db-path <path>")
            .to_error();
        }
        db_path = *value;
        continue;
      }
      return diagnostic::error("unsupported option `{}`", key)
        .usage("context create <name> geoip --db-path <path>")
        .to_error();
    }
    if (db_path.empty()) {
      return std::make_unique<ctx>(nullptr, nullptr);
    }
    auto mmdb = make_mmdb(db_path);
    auto mapped_mmdb = chunk::mmap(db_path);
    if (!mapped_mmdb) {
      return diagnostic::error("unable to retrieve file contents into memory")
        .to_error();
    }
    if (not mmdb) {
      return mmdb.error();
    }
    return std::make_unique<ctx>(std::move(*mmdb),
                                 std::move(mapped_mmdb.value()));
  }
};

} // namespace

} // namespace tenzir::plugins::geoip

TENZIR_REGISTER_PLUGIN(tenzir::plugins::geoip::plugin)
