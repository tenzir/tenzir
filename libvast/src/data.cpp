//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/data.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/filter_dir.hpp"
#include "vast/detail/load_contents.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/fbs/data.hpp"
#include "vast/logger.hpp"
#include "vast/operator.hpp"
#include "vast/type.hpp"

#include <caf/config_value.hpp>
#include <fmt/format.h>

#include <iterator>
#include <optional>
#include <simdjson.h>
#include <stdexcept>

#include <yaml-cpp/yaml.h>

namespace vast {

bool operator==(const data& lhs, const data& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator<(const data& lhs, const data& rhs) {
  return lhs.data_ < rhs.data_;
}

flatbuffers::Offset<fbs::Data>
pack(flatbuffers::FlatBufferBuilder& builder, const data& value) {
  auto f = detail::overload{
    [&](caf::none_t) -> flatbuffers::Offset<fbs::Data> {
      return fbs::CreateData(builder);
    },
    [&](const bool& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset = builder.CreateStruct(fbs::data::Boolean{value});
      return fbs::CreateData(builder, fbs::data::Data::boolean,
                             value_offset.Union());
    },
    [&](const integer& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset
        = builder.CreateStruct(fbs::data::Integer{value.value});
      return fbs::CreateData(builder, fbs::data::Data::integer,
                             value_offset.Union());
    },
    [&](const count& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset = builder.CreateStruct(fbs::data::Count{value});
      return fbs::CreateData(builder, fbs::data::Data::count,
                             value_offset.Union());
    },
    [&](const real& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset = builder.CreateStruct(fbs::data::Real{value});
      return fbs::CreateData(builder, fbs::data::Data::real,
                             value_offset.Union());
    },
    [&](const duration& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset
        = builder.CreateStruct(fbs::data::Duration{value.count()});
      return fbs::CreateData(builder, fbs::data::Data::duration,
                             value_offset.Union());
    },
    [&](const time& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset = builder.CreateStruct(
        fbs::data::Time{fbs::data::Duration{value.time_since_epoch().count()}});
      return fbs::CreateData(builder, fbs::data::Data::time,
                             value_offset.Union());
    },
    [&](const std::string& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset
        = fbs::data::CreateString(builder, builder.CreateString(value));
      return fbs::CreateData(builder, fbs::data::Data::string,
                             value_offset.Union());
    },
    [&](const pattern& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset = fbs::data::CreatePattern(
        builder, builder.CreateString(value.string()));
      return fbs::CreateData(builder, fbs::data::Data::pattern,
                             value_offset.Union());
    },
    [&](const address& value) -> flatbuffers::Offset<fbs::Data> {
      auto address_buffer = fbs::data::Address{};
      std::memcpy(address_buffer.mutable_bytes()->data(),
                  as_bytes(value).data(), 16);
      const auto value_offset = builder.CreateStruct(address_buffer);
      return fbs::CreateData(builder, fbs::data::Data::address,
                             value_offset.Union());
    },
    [&](const subnet& value) -> flatbuffers::Offset<fbs::Data> {
      auto subnet_buffer
        = fbs::data::Subnet{fbs::data::Address{}, value.length()};
      std::memcpy(subnet_buffer.mutable_address().mutable_bytes()->data(),
                  as_bytes(value.network()).data(), 16);
      const auto value_offset = builder.CreateStruct(subnet_buffer);
      return fbs::CreateData(builder, fbs::data::Data::subnet,
                             value_offset.Union());
    },
    [&](const enumeration& value) -> flatbuffers::Offset<fbs::Data> {
      const auto value_offset
        = builder.CreateStruct(fbs::data::Enumeration{value});
      return fbs::CreateData(builder, fbs::data::Data::enumeration,
                             value_offset.Union());
    },
    [&](const list& values) -> flatbuffers::Offset<fbs::Data> {
      auto value_offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
      value_offsets.reserve(values.size());
      for (const auto& value : values)
        value_offsets.emplace_back(pack(builder, value));
      const auto value_offset
        = fbs::data::CreateListDirect(builder, &value_offsets);
      return fbs::CreateData(builder, fbs::data::Data::list,
                             value_offset.Union());
    },
    [&](const map& entries) -> flatbuffers::Offset<fbs::Data> {
      auto entry_offsets
        = std::vector<flatbuffers::Offset<fbs::data::MapEntry>>{};
      entry_offsets.reserve(entries.size());
      for (const auto& [key, value] : entries) {
        const auto key_offset = pack(builder, key);
        const auto value_offset = pack(builder, value);
        entry_offsets.emplace_back(
          fbs::data::CreateMapEntry(builder, key_offset, value_offset));
      }
      const auto value_offset
        = fbs::data::CreateMapDirect(builder, &entry_offsets);
      return fbs::CreateData(builder, fbs::data::Data::map,
                             value_offset.Union());
    },
    [&](const record& fields) -> flatbuffers::Offset<fbs::Data> {
      auto field_offsets
        = std::vector<flatbuffers::Offset<fbs::data::RecordField>>{};
      field_offsets.reserve(fields.size());
      for (const auto& [name, value] : fields) {
        const auto key_offset = builder.CreateSharedString(name);
        const auto value_offset = pack(builder, value);
        field_offsets.emplace_back(
          fbs::data::CreateRecordField(builder, key_offset, value_offset));
      }
      const auto value_offset
        = fbs::data::CreateRecordDirect(builder, &field_offsets);
      return fbs::CreateData(builder, fbs::data::Data::record,
                             value_offset.Union());
    },
  };
  return caf::visit(f, value);
}

caf::error unpack(const fbs::Data& from, data& to) {
  switch (from.data_type()) {
    case fbs::data::Data::NONE: {
      to = data{};
      return caf::none;
    }
    case fbs::data::Data::boolean: {
      to = bool{from.data_as_boolean()->value()};
      return caf::none;
    }
    case fbs::data::Data::integer: {
      to = integer{from.data_as_integer()->value()};
      return caf::none;
    }
    case fbs::data::Data::count: {
      to = count{from.data_as_count()->value()};
      return caf::none;
    }
    case fbs::data::Data::real: {
      to = real{from.data_as_real()->value()};
      return caf::none;
    }
    case fbs::data::Data::duration: {
      to = duration{from.data_as_duration()->ns()};
      return caf::none;
    }
    case fbs::data::Data::time: {
      to = time{} + duration{from.data_as_time()->time_since_epoch().ns()};
      return caf::none;
    }
    case fbs::data::Data::string: {
      to = from.data_as_string()->value()->str();
      return caf::none;
    }
    case fbs::data::Data::pattern: {
      to = pattern{from.data_as_pattern()->value()->str()};
      return caf::none;
    }
    case fbs::data::Data::address: {
      auto address_buffer = address{};
      static_assert(sizeof(address)
                    == sizeof(*from.data_as_address()->bytes()));
      std::memcpy(&address_buffer, from.data_as_address()->bytes()->data(),
                  sizeof(address));
      to = address_buffer;
      return caf::none;
    }
    case fbs::data::Data::subnet: {
      auto address_buffer = address{};
      static_assert(sizeof(address)
                    == sizeof(*from.data_as_subnet()->address().bytes()));
      std::memcpy(&address_buffer,
                  from.data_as_subnet()->address().bytes()->data(),
                  sizeof(address));
      to = subnet{address_buffer, from.data_as_subnet()->length()};
      return caf::none;
    }
    case fbs::data::Data::enumeration: {
      to
        = detail::narrow_cast<enumeration>(from.data_as_enumeration()->value());
      return caf::none;
    }
    case fbs::data::Data::list: {
      auto list_buffer = list{};
      list_buffer.reserve(from.data_as_list()->values()->size());
      for (const auto* value : *from.data_as_list()->values()) {
        VAST_ASSERT(value);
        auto element_buffer = data{};
        if (auto err = unpack(*value, element_buffer))
          return err;
        list_buffer.emplace_back(std::move(element_buffer));
      }
      to = std::move(list_buffer);
      return caf::none;
    }
    case fbs::data::Data::map: {
      auto map_buffer = map::vector_type{};
      map_buffer.reserve(from.data_as_map()->entries()->size());
      for (const auto* entry : *from.data_as_map()->entries()) {
        VAST_ASSERT(entry);
        auto key_buffer = data{};
        if (auto err = unpack(*entry->key(), key_buffer))
          return err;
        auto value_buffer = data{};
        if (auto err = unpack(*entry->value(), value_buffer))
          return err;
        map_buffer.emplace_back(std::move(key_buffer), std::move(value_buffer));
      }
      to = map{map::make_unsafe(std::move(map_buffer))};
      return caf::none;
    }
    case fbs::data::Data::record: {
      auto record_buffer = record::vector_type{};
      record_buffer.reserve(from.data_as_record()->fields()->size());
      for (const auto* field : *from.data_as_record()->fields()) {
        VAST_ASSERT(field);
        auto data_buffer = data{};
        if (auto err = unpack(*field->data(), data_buffer))
          return err;
        record_buffer.emplace_back(field->name()->str(),
                                   std::move(data_buffer));
      }
      to = record{record::make_unsafe(std::move(record_buffer))};
      return caf::none;
    }
  }
  __builtin_unreachable();
}

bool evaluate(const data& lhs, relational_operator op, const data& rhs) {
  auto eval_string_and_pattern = [](const auto& x, const auto& y) {
    return caf::visit(
      detail::overload{
        [](const auto&, const auto&) -> std::optional<bool> {
          return {};
        },
        [](const std::string& lhs, const pattern& rhs) -> std::optional<bool> {
          return rhs.match(lhs);
        },
        [](const pattern& lhs, const std::string& rhs) -> std::optional<bool> {
          return lhs.match(rhs);
        },
      },
      x, y);
  };
  auto eval_in = [](const auto& x, const auto& y) {
    return caf::visit(detail::overload{
                        [](const auto&, const auto&) {
                          return false;
                        },
                        [](const std::string& lhs, const std::string& rhs) {
                          return rhs.find(lhs) != std::string::npos;
                        },
                        [](const std::string& lhs, const pattern& rhs) {
                          return rhs.search(lhs);
                        },
                        [](const address& lhs, const subnet& rhs) {
                          return rhs.contains(lhs);
                        },
                        [](const subnet& lhs, const subnet& rhs) {
                          return rhs.contains(lhs);
                        },
                        [](const auto& lhs, const list& rhs) {
                          return std::find(rhs.begin(), rhs.end(), lhs)
                                 != rhs.end();
                        },
                      },
                      x, y);
  };
  switch (op) {
    default:
      VAST_ASSERT(!"missing case");
      return false;
    case relational_operator::in:
      return eval_in(lhs, rhs);
    case relational_operator::not_in:
      return !eval_in(lhs, rhs);
    case relational_operator::ni:
      return eval_in(rhs, lhs);
    case relational_operator::not_ni:
      return !eval_in(rhs, lhs);
    case relational_operator::equal:
      if (auto x = eval_string_and_pattern(lhs, rhs))
        return *x;
      return lhs == rhs;
    case relational_operator::not_equal:
      if (auto x = eval_string_and_pattern(lhs, rhs))
        return !*x;
      return lhs != rhs;
    case relational_operator::less:
      return lhs < rhs;
    case relational_operator::less_equal:
      return lhs <= rhs;
    case relational_operator::greater:
      return lhs > rhs;
    case relational_operator::greater_equal:
      return lhs >= rhs;
  }
}

bool is_basic(const data& x) {
  return caf::visit(detail::overload{
                      [](const auto&) {
                        return true;
                      },
                      [](const list&) {
                        return false;
                      },
                      [](const map&) {
                        return false;
                      },
                      [](const record&) {
                        return false;
                      },
                    },
                    x);
}

bool is_complex(const data& x) {
  return !is_basic(x);
}

bool is_recursive(const data& x) {
  return caf::visit(detail::overload{
                      [](const auto&) {
                        return false;
                      },
                      [](const list&) {
                        return true;
                      },
                      [](const map&) {
                        return true;
                      },
                      [](const record&) {
                        return true;
                      },
                    },
                    x);
}

bool is_container(const data& x) {
  // TODO: should a record be considered as a container?
  return is_recursive(x);
}

size_t depth(const record& r) {
  size_t result = 0;
  if (r.empty())
    return result;
  // Do a DFS, using (begin, end, depth) tuples for the state.
  std::vector<std::tuple<record::const_iterator, record::const_iterator, size_t>>
    stack;
  stack.emplace_back(r.begin(), r.end(), 1u);
  while (!stack.empty()) {
    auto [begin, end, depth] = stack.back();
    stack.pop_back();
    result = std::max(result, depth);
    while (begin != end) {
      const auto& x = (begin++)->second;
      if (const auto* nested = caf::get_if<record>(&x))
        stack.emplace_back(nested->begin(), nested->end(), depth + 1);
    }
  }
  return result;
}

namespace {

record flatten(const record& r, size_t max_recursion) {
  record result;
  if (max_recursion == 0) {
    VAST_WARN("partially discarding record: recursion limit of {} exceeded",
              defaults::max_recursion);
    return result;
  }
  for (const auto& [k, v] : r) {
    if (const auto* nested = caf::get_if<record>(&v))
      for (auto& [nk, nv] : flatten(*nested, --max_recursion))
        result.emplace(fmt::format("{}.{}", k, nk), std::move(nv));
    else
      result.emplace(k, v);
  }
  return result;
}

std::optional<record>
flatten(const record& r, const record_type& rt, size_t max_recursion) {
  record result;
  if (max_recursion == 0) {
    VAST_WARN("partially discarding record: recursion limit of {} exceeded",
              defaults::max_recursion);
    return result;
  }
  for (const auto& [k, v] : r) {
    if (const auto* ir = caf::get_if<record>(&v)) {
      // Look for a matching field of type record.
      const auto offset = rt.resolve_key(k);
      if (!offset.has_value())
        return {};
      auto field = rt.field(*offset);
      const auto* irt = caf::get_if<record_type>(&field.type);
      if (!irt)
        return {};
      // Recurse.
      auto nested = flatten(*ir, *irt, --max_recursion);
      if (!nested)
        return {};
      // Hoist nested record into parent scope by prefixing field names.
      for (auto& [nk, nv] : *nested)
        result.emplace(fmt::format("{}.{}", k, nk), std::move(nv));
    } else {
      result.emplace(k, v);
    }
  }
  return result;
}

std::optional<data>
flatten(const data& x, const type& t, size_t max_recursion) {
  if (max_recursion == 0) {
    VAST_WARN("partially discarding record: recursion limit of {} exceeded",
              defaults::max_recursion);
    return caf::none;
  }
  const auto* xs = caf::get_if<record>(&x);
  const auto* rt = caf::get_if<record_type>(&t);
  if (xs && rt)
    return flatten(*xs, *rt, --max_recursion);
  return caf::none;
}

} // namespace

std::optional<data> flatten(const data& x, const type& t) {
  return flatten(x, t, defaults::max_recursion);
}

std::optional<record> flatten(const record& r, const record_type& rt) {
  return flatten(r, rt, defaults::max_recursion);
}

record flatten(const record& r) {
  return flatten(r, defaults::max_recursion);
}

namespace {

void merge(const record& src, record& dst, enum policy::merge_lists merge_lists,
           const size_t max_recursion) {
  if (max_recursion == 0) {
    VAST_WARN("partially discarding record: recursion limit of {} exceeded",
              defaults::max_recursion);
    return;
  }
  for (const auto& [k, v] : src) {
    if (const auto* src_rec = caf::get_if<record>(&v)) {
      auto* dst_rec = caf::get_if<record>(&dst[k]);
      if (!dst_rec) {
        // Overwrite key with empty record on type mismatch.
        dst[k] = record{};
        dst_rec = caf::get_if<record>(&dst[k]);
      }
      merge(*src_rec, *dst_rec, merge_lists, max_recursion - 1);
    } else if (merge_lists == policy::merge_lists::yes
               && caf::holds_alternative<list>(v)) {
      const auto& src_list = caf::get<list>(v);
      if (auto* dst_list = caf::get_if<list>(&dst[k])) {
        dst_list->insert(dst_list->end(), src_list.begin(), src_list.end());
      } else if (auto it = dst.find(k); it != dst.end()) {
        auto dst_list = list{};
        if (!caf::holds_alternative<caf::none_t>(it->second)) {
          dst_list.reserve(src_list.size() + 1);
          dst_list.push_back(std::move(it->second));
        } else {
          dst_list.reserve(src_list.size());
        }
        dst_list.insert(dst_list.end(), src_list.begin(), src_list.end());
        it->second = std::move(dst_list);
      } else {
        dst[k] = src_list;
      }
    } else {
      dst[k] = v;
    }
  }
}

} // namespace

void merge(const record& src, record& dst,
           enum policy::merge_lists merge_lists) {
  merge(src, dst, merge_lists, defaults::max_recursion);
}

caf::error convert(const map& xs, caf::dictionary<caf::config_value>& ys) {
  for (const auto& [k, v] : xs) {
    caf::config_value x;
    if (auto err = convert(v, x))
      return err;
    ys[to_string(k)] = std::move(x);
  }
  return caf::none;
}

caf::error convert(const record& xs, caf::dictionary<caf::config_value>& ys) {
  for (const auto& [k, v] : xs) {
    caf::config_value x;
    if (auto err = convert(v, x))
      return err;
    ys[k] = std::move(x);
  }
  return caf::none;
}

caf::error convert(const record& xs, caf::config_value& cv) {
  caf::config_value::dictionary result;
  if (auto err = convert(xs, result))
    return err;
  cv = std::move(result);
  return caf::none;
}

caf::error convert(const data& d, caf::config_value& cv) {
  auto f = detail::overload{
    [&](const auto& x) -> caf::error {
      using value_type = std::decay_t<decltype(x)>;
      if constexpr (detail::is_any_v<value_type, bool, count, real, duration,
                                     std::string>)
        cv = x;
      else if constexpr (std::is_same_v<value_type, integer>)
        cv = x.value;
      else
        cv = to_string(x);
      return caf::none;
    },
    [&](caf::none_t) -> caf::error {
      // A caf::config_value has no notion of "null" value. Converting it to a
      // default-constructed config_value would be wrong, because that's just
      // an integer with value 0. As such, the conversion is a partial function
      // and we must fail at this point. If you trigger this error when
      // converting a record, you can first flatten the record and then delete
      // all null keys. Then this branch will not be triggered.
      return caf::make_error(ec::type_clash, "cannot convert null to "
                                             "config_value");
    },
    [&](const list& xs) -> caf::error {
      caf::config_value::list result;
      result.reserve(xs.size());
      for (const auto& x : xs) {
        caf::config_value y;
        if (auto err = convert(x, y))
          return err;
        result.push_back(std::move(y));
      }
      cv = std::move(result);
      return caf::none;
    },
    [&](const map& xs) -> caf::error {
      // We treat maps like records.
      caf::dictionary<caf::config_value> result;
      if (auto err = convert(xs, result))
        return err;
      cv = std::move(result);
      return caf::none;
    },
    [&](const record& xs) -> caf::error {
      caf::dictionary<caf::config_value> result;
      if (auto err = convert(xs, result))
        return err;
      cv = std::move(result);
      return caf::none;
    },
  };
  return caf::visit(f, d);
}

bool convert(const caf::dictionary<caf::config_value>& xs, record& ys) {
  for (const auto& [k, v] : xs) {
    data y;
    if (!convert(v, y))
      return false;
    ys.emplace(k, std::move(y));
  }
  return true;
}

bool convert(const caf::dictionary<caf::config_value>& xs, data& y) {
  record result;
  if (!convert(xs, result))
    return false;
  y = std::move(result);
  return true;
}

bool convert(const caf::config_value& x, data& y) {
  auto f = detail::overload{
    [&](const auto& value) -> bool {
      y = value;
      return true;
    },
    [&](const caf::config_value::integer& value) -> bool {
      y = integer{value};
      return true;
    },
    [&](const caf::uri& value) -> bool {
      y = to_string(value);
      return true;
    },
    [&](const caf::config_value::list& xs) -> bool {
      list result;
      result.reserve(xs.size());
      for (const auto& x : xs) {
        data element;
        if (!convert(x, element)) {
          return false;
        }
        result.push_back(std::move(element));
      }
      y = std::move(result);
      return true;
    },
    [&](const caf::config_value::dictionary& xs) -> bool {
      record result;
      if (!convert(xs, result))
        return false;
      y = std::move(result);
      return true;
    },
  };
  return caf::visit(f, x.get_data());
}

caf::expected<record> strip(const record& xs, size_t max_recursion) {
  if (max_recursion == 0)
    return ec::recursion_limit_reached;
  record result;
  for (const auto& [k, v] : xs) {
    if (caf::holds_alternative<caf::none_t>(v))
      continue;
    if (const auto* vr = caf::get_if<record>(&v)) {
      if (auto nested = strip(*vr, max_recursion - 1)) {
        if (!nested->empty())
          result.emplace(k, std::move(*nested));
      } else {
        return nested.error();
      }
    } else {
      result.emplace(k, v);
    }
  }
  return result;
}

namespace {

caf::error convert(const simdjson::dom::element& json, data& result,
                   size_t max_recursion) {
  switch (json.type()) {
    case simdjson::dom::element_type::ARRAY: {
      auto array = json.get_array();
      list xs;
      xs.reserve(array.size());
      for (auto element : array) {
        data x;
        if (auto err = convert(element, x, max_recursion - 1))
          return err;
        xs.push_back(std::move(x));
      }
      result = std::move(xs);
      break;
    }
    case simdjson::dom::element_type::OBJECT: {
      auto object = json.get_object();
      record xs;
      xs.reserve(object.size());
      for (auto [k, v] : object) {
        data value;
        if (auto err = convert(v, value, max_recursion - 1))
          return err;
        xs.emplace(k, std::move(value));
      }
      result = std::move(xs);
      break;
    }
    case simdjson::dom::element_type::INT64:
      result = integer{json.get_int64().value()};
      break;
    case simdjson::dom::element_type::UINT64:
      result = count{json.get_uint64().value()};
      break;
    case simdjson::dom::element_type::DOUBLE:
      result = real{json.get_double().value()};
      break;
    case simdjson::dom::element_type::STRING: {
      const auto& str = json.get_string().value();
      time t;
      duration d;
      if (auto x = to<subnet>(str))
        result = *x;
      else if (auto x = to<address>(str))
        result = *x;
      else if (parsers::ymdhms(str, t))
        result = t;
      else if (parsers::duration(str, d))
        result = d;
      else
        result = std::string{json.get_string().value()};
      break;
    }
    case simdjson::dom::element_type::BOOL:
      result = json.get_bool().value();
      break;
    case simdjson::dom::element_type::NULL_VALUE:
      break;
  }
  return caf::none;
}

} // namespace

caf::expected<data> from_json(std::string_view str, size_t max_recursion) {
  simdjson::dom::parser json_parser;
  auto parse_result = json_parser.parse(str);
  if (parse_result.error() != simdjson::error_code::SUCCESS)
    return caf::make_error(ec::parse_error, "failed to parse JSON value");
  data result;
  if (auto err = convert(parse_result.value(), result, max_recursion))
    return err;
  return result;
}

caf::expected<std::string> to_json(const data& x) {
  std::string str;
  auto out = std::back_inserter(str);
  if (json_printer<policy::tree, policy::human_readable_durations,
                   policy::include_nulls, 2>{}
        .print(out, x))
    return str;
  return caf::make_error(ec::parse_error, "cannot convert to json");
}

namespace {

data parse(const YAML::Node& node) {
  switch (node.Type()) {
    case YAML::NodeType::Undefined:
    case YAML::NodeType::Null:
      return data{};
    case YAML::NodeType::Scalar: {
      auto str = node.as<std::string>();
      data result;
      // Attempt some type inference.
      if (parsers::boolean(str, result))
        return result;
      // Attempt maximum type inference.
      if (parsers::data(str, result))
        return result;
      // Take the input as-is if nothing worked.
      return str;
    }
    case YAML::NodeType::Sequence: {
      list xs;
      xs.reserve(node.size());
      for (const auto& element : node)
        xs.push_back(parse(element));
      return xs;
    }
    case YAML::NodeType::Map: {
      record xs;
      xs.reserve(node.size());
      for (const auto& pair : node)
        xs.emplace(pair.first.as<std::string>(), parse(pair.second));
      return xs;
    }
  }
  die("unhandled YAML node type in switch statement");
}

} // namespace

caf::expected<data> from_yaml(std::string_view str) {
  try {
    // Maybe one glory day in the future it will be possible to perform a
    // single pass over the input without incurring a copy.
    auto node = YAML::Load(std::string{str});
    return parse(node);
  } catch (const YAML::Exception& e) {
    return caf::make_error(ec::parse_error,
                           "failed to parse YAML at line/col/pos", e.mark.line,
                           e.mark.column, e.mark.pos);
  } catch (const std::logic_error& e) {
    return caf::make_error(ec::logic_error, e.what());
  }
}

caf::expected<data> load_yaml(const std::filesystem::path& file) {
  const auto contents = detail::load_contents(file);
  if (!contents)
    return contents.error();
  if (auto yaml = from_yaml(*contents))
    return yaml;
  else
    return caf::make_error(ec::parse_error, "failed to load YAML file",
                           file.string(), yaml.error().context());
}

caf::expected<std::vector<std::pair<std::filesystem::path, data>>>
load_yaml_dir(const std::filesystem::path& dir, size_t max_recursion) {
  if (max_recursion == 0)
    return ec::recursion_limit_reached;
  std::vector<std::pair<std::filesystem::path, data>> result;
  auto filter = [](const std::filesystem::path& f) {
    const auto& extension = f.extension();
    return extension == ".yaml" || extension == ".yml";
  };
  auto yaml_files = detail::filter_dir(dir, std::move(filter), max_recursion);
  if (!yaml_files)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to filter YAML dir at {}: {}",
                                       dir, yaml_files.error()));
  for (auto& file : *yaml_files)
    if (auto yaml = load_yaml(file))
      result.emplace_back(std::move(file), std::move(*yaml));
    else
      return yaml.error();
  return result;
}

namespace {

void print(YAML::Emitter& out, const data& x) {
  auto f = detail::overload{
    [&out](caf::none_t) {
      out << YAML::Null;
    },
    [&out](bool x) {
      out << (x ? "true" : "false");
    },
    [&out](integer x) {
      out << x.value;
    },
    [&out](count x) {
      out << x;
    },
    [&out](real x) {
      out << to_string(x);
    },
    [&out](duration x) {
      out << to_string(x);
    },
    [&out](time x) {
      out << to_string(x);
    },
    [&out](const std::string& x) {
      out << x;
    },
    [&out](const pattern& x) {
      out << to_string(x);
    },
    [&out](const address& x) {
      out << to_string(x);
    },
    [&out](const subnet& x) {
      out << to_string(x);
    },
    [&out](const enumeration& x) {
      out << to_string(x);
    },
    [&out](const list& xs) {
      out << YAML::BeginSeq;
      for (const auto& x : xs)
        print(out, x);
      out << YAML::EndSeq;
    },
    // We treat maps like records.
    [&out](const map& xs) {
      out << YAML::BeginMap;
      for (const auto& [k, v] : xs) {
        out << YAML::Key;
        print(out, k);
        out << YAML::Value;
        print(out, v);
      }
      out << YAML::EndMap;
    },
    [&out](const record& xs) {
      out << YAML::BeginMap;
      for (const auto& [k, v] : xs) {
        out << YAML::Key << k << YAML::Value;
        print(out, v);
      }
      out << YAML::EndMap;
    },
  };
  caf::visit(f, x);
}

} // namespace

caf::expected<std::string> to_yaml(const data& x) {
  YAML::Emitter out;
  out.SetOutputCharset(YAML::EscapeNonAscii); // restrict to ASCII output
  out.SetIndent(2);
  print(out, x);
  if (out.good())
    return std::string{out.c_str(), out.size()};
  return caf::make_error(ec::parse_error, out.GetLastError());
}

} // namespace vast
