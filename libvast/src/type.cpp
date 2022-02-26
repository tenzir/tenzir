//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/type.hpp"

#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/fbs/type.hpp"
#include "vast/legacy_type.hpp"
#include "vast/schema.hpp"

#include <caf/sum_type.hpp>
#include <fmt/format.h>

// -- utility functions -------------------------------------------------------

namespace vast {

namespace {

std::span<const std::byte> none_type_representation() {
  // This helper function solely exists because ADL will not find the as_bytes
  // overload for none_type from within the as_bytes overload for type.
  return as_bytes(none_type{});
}

constexpr size_t reserved_string_size(std::string_view str) {
  // This helper function calculates the length of a string in a FlatBuffers
  // table. It adds an extra byte because strings in FlatBuffers tables are
  // always zero-terminated, and then rounds up to a full four bytes because of
  // the included padding.
  return str.empty() ? 0 : (((str.size() + 1 + 3) / 4) * 4);
}

const fbs::Type*
resolve_transparent(const fbs::Type* root, enum type::transparent transparent
                                           = type::transparent::yes) {
  VAST_ASSERT(root);
  while (transparent == type::transparent::yes) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type:
      case fbs::type::Type::record_type:
        transparent = type::transparent::no;
        break;
      case fbs::type::Type::enriched_type:
        root = root->type_as_enriched_type()->type_nested_root();
        VAST_ASSERT(root);
        break;
    }
  }
  return root;
}

template <complex_type T>
std::span<const std::byte> as_bytes_complex(const T& ct) {
  const auto& t
    = static_cast<const type&>(static_cast<const stateful_type_base&>(ct));
  const auto* root = &t.table(type::transparent::no);
  auto result = as_bytes(t);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type:
      case fbs::type::Type::record_type:
        return result;
      case fbs::type::Type::enriched_type: {
        const auto* enriched = root->type_as_enriched_type();
        VAST_ASSERT(enriched);
        root = enriched->type_nested_root();
        VAST_ASSERT(root);
        result = as_bytes(*enriched->type());
        break;
      }
    }
  }
  __builtin_unreachable();
}

template <class T>
  requires(std::is_same_v<T, struct enumeration_type::field> //
           || std::is_same_v<T, enumeration_type::field_view>)
void construct_enumeration_type(stateful_type_base& self, const T* begin,
                                const T* end) {
  VAST_ASSERT(begin != end, "An enumeration type must not have zero "
                            "fields");
  // Unlike for other concrete types, we do not calculate the exact amount of
  // bytes we need to allocate beforehand. This is because the individual
  // fields are stored in a flat hash map, whose size cannot trivially be
  // determined.
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto field_offsets
    = std::vector<flatbuffers::Offset<fbs::type::detail::EnumerationField>>{};
  field_offsets.reserve(end - begin);
  uint32_t next_key = 0;
  for (const auto* it = begin; it != end; ++it) {
    const auto key
      = it->key != std::numeric_limits<uint32_t>::max() ? it->key : next_key;
    next_key = key + 1;
    const auto name_offset = builder.CreateString(it->name);
    field_offsets.emplace_back(
      fbs::type::detail::CreateEnumerationField(builder, key, name_offset));
  }
  const auto fields_offset = builder.CreateVectorOfSortedTables(&field_offsets);
  const auto enumeration_type_offset
    = fbs::type::CreateEnumerationType(builder, fields_offset);
  const auto type_offset
    = fbs::CreateType(builder, fbs::type::Type::enumeration_type,
                      enumeration_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  auto chunk = chunk::make(std::move(result));
  self = type{std::move(chunk)};
}

template <class T>
void construct_record_type(stateful_type_base& self, const T& begin,
                           const T& end) {
  VAST_ASSERT(begin != end, "A record type must not have zero fields.");
  const auto reserved_size = [&]() noexcept {
    // By default the builder allocates 1024 bytes, which is much more than
    // what we require, and since we can easily calculate the exact amount we
    // should do that. The total length is made up from the following terms:
    // - 52 bytes FlatBuffers table framing
    // - 24 bytes for each contained field.
    // - All contained string lengths, rounded up to four each.
    // - All contained nested type FlatBuffers.
    size_t size = 52;
    for (auto it = begin; it != end; ++it) {
      const auto& type_bytes = as_bytes(it->type);
      size += 24;
      VAST_ASSERT(!it->name.empty(), "Record field names must not be empty.");
      size += reserved_string_size(it->name);
      size += type_bytes.size();
    }
    return size;
  }();
  auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
  auto field_offsets
    = std::vector<flatbuffers::Offset<fbs::type::detail::RecordField>>{};
  field_offsets.reserve(end - begin);
  for (auto it = begin; it != end; ++it) {
    const auto type_bytes = as_bytes(it->type);
    const auto name_offset = builder.CreateString(it->name);
    const auto type_offset = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(type_bytes.data()), type_bytes.size());
    field_offsets.emplace_back(
      fbs::type::detail::CreateRecordField(builder, name_offset, type_offset));
  }
  const auto fields_offset = builder.CreateVector(field_offsets);
  const auto record_type_offset
    = fbs::type::CreateRecordType(builder, fields_offset);
  const auto type_offset = fbs::CreateType(
    builder, fbs::type::Type::record_type, record_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  VAST_ASSERT(result.size() == reserved_size);
  auto chunk = chunk::make(std::move(result));
  self = type{std::move(chunk)};
}

/// Returns whether a type holds a custom name. Use with caution, as whether a
/// type is an "alias" or not should not be of concern to developers using the
/// type system, and is abstracted away by design.
/// Note that a type with an attribute and no custom name is not an alias, which
/// is why it's best not to think in terms of aliases when using the type API,
/// and also why this function is not part of the public API.
/// @related type::transparent
bool is_alias(const type& type) noexcept {
  const auto* root = &type.table(type::transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type:
      case fbs::type::Type::record_type:
        return false;
      case fbs::type::Type::enriched_type: {
        const auto* enriched_type = root->type_as_enriched_type();
        if (enriched_type->name())
          return true;
        root = enriched_type->type_nested_root();
        VAST_ASSERT(root);
        break;
      }
    }
  }
  __builtin_unreachable();
}

} // namespace

// -- type --------------------------------------------------------------------

type::type() noexcept = default;

type::type(const type& other) noexcept = default;

type& type::operator=(const type& rhs) noexcept = default;

type::type(type&& other) noexcept = default;

type& type::operator=(type&& other) noexcept = default;

type::~type() noexcept = default;

type::type(chunk_ptr&& table) noexcept {
#if VAST_ENABLE_ASSERTIONS
  VAST_ASSERT(table);
  VAST_ASSERT(table->size() > 0);
  const auto* const data = reinterpret_cast<const uint8_t*>(table->data());
  auto verifier = flatbuffers::Verifier{data, table->size()};
  VAST_ASSERT(fbs::GetType(data)->Verify(verifier),
              "Encountered invalid vast.fbs.Type FlatBuffers table.");
#  if defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
  VAST_ASSERT(verifier.GetComputedSize() == table->size(),
              "Encountered unexpected excess bytes in vast.fbs.Type "
              "FlatBuffers table.");
#  endif // defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
#endif   // VAST_ENABLE_ASSERTIONS
  table_ = std::move(table);
}

type::type(std::string_view name, const type& nested,
           const std::vector<struct attribute>& attributes) noexcept {
  if (name.empty() && attributes.empty()) {
    // This special case fbs::type::Type::exists for easier conversion of legacy
    // types, which did not require an legacy alias type wrapping to have a name.
    *this = nested;
  } else {
    const auto nested_bytes = as_bytes(nested);
    const auto reserved_size = [&]() noexcept {
      // The total length is made up from the following terms:
      // - 52 bytes FlatBuffers table framing
      // - Nested type FlatBuffers table size
      // - All contained string lengths, rounded up to four each
      // Note that this cannot account for attributes, since they are stored in
      // hash map which makes calculating the space requirements non-trivial.
      size_t size = 52;
      size += nested_bytes.size();
      size += reserved_string_size(name);
      return size;
    };
    auto builder = attributes.empty()
                     ? flatbuffers::FlatBufferBuilder{reserved_size()}
                     : flatbuffers::FlatBufferBuilder{};
    const auto nested_type_offset = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(nested_bytes.data()),
      nested_bytes.size());
    const auto name_offset = name.empty() ? 0 : builder.CreateString(name);
    const auto attributes_offset = [&]() noexcept
      -> flatbuffers::Offset<
        flatbuffers::Vector<flatbuffers::Offset<fbs::type::detail::Attribute>>> {
      if (attributes.empty())
        return 0;
      auto attributes_offsets
        = std::vector<flatbuffers::Offset<fbs::type::detail::Attribute>>{};
      attributes_offsets.reserve(attributes.size());
      for (const auto& attribute : attributes) {
        const auto key_offset = builder.CreateString(attribute.key);
        const auto value_offset
          = attribute.value ? builder.CreateString(*attribute.value) : 0;
        attributes_offsets.emplace_back(fbs::type::detail::CreateAttribute(
          builder, key_offset, value_offset));
      }
      return builder.CreateVectorOfSortedTables(&attributes_offsets);
    }();
    const auto enriched_type_offset = fbs::type::detail::CreateEnrichedType(
      builder, nested_type_offset, name_offset, attributes_offset);
    const auto type_offset = fbs::CreateType(
      builder, fbs::type::Type::enriched_type, enriched_type_offset.Union());
    builder.Finish(type_offset);
    auto result = builder.Release();
    table_ = chunk::make(std::move(result));
  }
}

type::type(std::string_view name, const type& nested) noexcept
  : type(name, nested, {}) {
  // nop
}

type::type(const type& nested,
           const std::vector<struct attribute>& attributes) noexcept
  : type("", nested, attributes) {
  // nop
}

type type::infer(const data& value) noexcept {
  auto f = detail::overload{
    [](caf::none_t) noexcept -> type {
      return {};
    },
    [](const bool&) noexcept -> type {
      return type{bool_type{}};
    },
    [](const integer&) noexcept -> type {
      return type{integer_type{}};
    },
    [](const count&) noexcept -> type {
      return type{count_type{}};
    },
    [](const real&) noexcept -> type {
      return type{real_type{}};
    },
    [](const duration&) noexcept -> type {
      return type{duration_type{}};
    },
    [](const time&) noexcept -> type {
      return type{time_type{}};
    },
    [](const std::string&) noexcept -> type {
      return type{string_type{}};
    },
    [](const pattern&) noexcept -> type {
      return type{pattern_type{}};
    },
    [](const address&) noexcept -> type {
      return type{address_type{}};
    },
    [](const subnet&) noexcept -> type {
      return type{subnet_type{}};
    },
    [](const enumeration&) noexcept -> type {
      // Enumeration types cannot be inferred.
      return {};
    },
    [](const list& list) noexcept -> type {
      // List types cannot be inferred from empty lists.
      if (list.empty())
        return type{list_type{none_type{}}};
      // Technically lists can contain heterogenous data, but for optimization
      // purposes we only check the first element when assertions are disabled.
      auto value_type = infer(*list.begin());
      VAST_ASSERT(std::all_of(list.begin() + 1, list.end(),
                              [&](const auto& elem) noexcept {
                                return value_type.type_index()
                                       == infer(elem).type_index();
                              }),
                  "expected a homogenous list");
      return type{list_type{value_type}};
    },
    [](const map& map) noexcept -> type {
      // Map types cannot be inferred from empty maps.
      if (map.empty())
        return type{map_type{none_type{}, none_type{}}};
      // Technically maps can contain heterogenous data, but for optimization
      // purposes we only check the first element when assertions are disabled.
      auto key_type = infer(map.begin()->first);
      auto value_type = infer(map.begin()->second);
      VAST_ASSERT(std::all_of(map.begin() + 1, map.end(),
                              [&](const auto& elem) noexcept {
                                return key_type.type_index()
                                         == infer(elem.first).type_index()
                                       && value_type.type_index()
                                            == infer(elem.second).type_index();
                              }),
                  "expected a homogenous map");
      return type{map_type{key_type, value_type}};
    },
    [](const record& record) noexcept -> type {
      // Record types cannot be inferred from empty records.
      if (record.empty())
        return {};
      auto fields = std::vector<record_type::field_view>{};
      fields.reserve(record.size());
      for (const auto& field : record)
        fields.push_back({
          field.first,
          infer(field.second),
        });
      return type{record_type{fields}};
    },
  };
  return caf::visit(f, value);
}

type type::from_legacy_type(const legacy_type& other) noexcept {
  const auto attributes = [&] {
    auto result = std::vector<struct attribute>{};
    const auto& attributes = other.attributes();
    result.reserve(attributes.size());
    for (const auto& attribute : other.attributes())
      result.push_back({
        attribute.key,
        attribute.value ? *attribute.value : std::optional<std::string>{},
      });
    return result;
  }();
  auto f = detail::overload{
    [&](const legacy_none_type&) {
      return type{other.name(), none_type{}, attributes};
    },
    [&](const legacy_bool_type&) {
      return type{other.name(), bool_type{}, attributes};
    },
    [&](const legacy_integer_type&) {
      return type{other.name(), integer_type{}, attributes};
    },
    [&](const legacy_count_type&) {
      return type{other.name(), count_type{}, attributes};
    },
    [&](const legacy_real_type&) {
      return type{other.name(), real_type{}, attributes};
    },
    [&](const legacy_duration_type&) {
      return type{other.name(), duration_type{}, attributes};
    },
    [&](const legacy_time_type&) {
      return type{other.name(), time_type{}, attributes};
    },
    [&](const legacy_string_type&) {
      return type{other.name(), string_type{}, attributes};
    },
    [&](const legacy_pattern_type&) {
      return type{other.name(), pattern_type{}, attributes};
    },
    [&](const legacy_address_type&) {
      return type{other.name(), address_type{}, attributes};
    },
    [&](const legacy_subnet_type&) {
      return type{other.name(), subnet_type{}, attributes};
    },
    [&](const legacy_enumeration_type& enumeration) {
      auto fields = std::vector<struct enumeration_type::field>{};
      fields.reserve(enumeration.fields.size());
      for (const auto& field : enumeration.fields)
        fields.push_back({field});
      return type{other.name(), enumeration_type{fields}, attributes};
    },
    [&](const legacy_list_type& list) {
      return type{other.name(), list_type{from_legacy_type(list.value_type)},
                  attributes};
    },
    [&](const legacy_map_type& map) {
      return type{other.name(),
                  map_type{from_legacy_type(map.key_type),
                           from_legacy_type(map.value_type)},
                  attributes};
    },
    [&](const legacy_alias_type& alias) {
      return type{other.name(), from_legacy_type(alias.value_type), attributes};
    },
    [&](const legacy_record_type& record) {
      auto fields = std::vector<struct record_type::field_view>{};
      fields.reserve(record.fields.size());
      for (const auto& field : record.fields)
        fields.push_back({field.name, from_legacy_type(field.type)});
      return type{other.name(), record_type{fields}, attributes};
    },
  };
  return caf::visit(f, other);
}

legacy_type type::to_legacy_type() const noexcept {
  auto f = detail::overload{
    [&](const none_type&) -> legacy_type {
      return legacy_none_type{};
    },
    [&](const bool_type&) -> legacy_type {
      return legacy_bool_type{};
    },
    [&](const integer_type&) -> legacy_type {
      return legacy_integer_type{};
    },
    [&](const count_type&) -> legacy_type {
      return legacy_count_type{};
    },
    [&](const real_type&) -> legacy_type {
      return legacy_real_type{};
    },
    [&](const duration_type&) -> legacy_type {
      return legacy_duration_type{};
    },
    [&](const time_type&) -> legacy_type {
      return legacy_time_type{};
    },
    [&](const string_type&) -> legacy_type {
      return legacy_string_type{};
    },
    [&](const pattern_type&) -> legacy_type {
      return legacy_pattern_type{};
    },
    [&](const address_type&) -> legacy_type {
      return legacy_address_type{};
    },
    [&](const subnet_type&) -> legacy_type {
      return legacy_subnet_type{};
    },
    [&](const enumeration_type& enumeration) -> legacy_type {
      auto result = legacy_enumeration_type{};
      for (uint32_t i = 0; const auto& field : enumeration.fields()) {
        VAST_ASSERT(i++ == field.key, "failed to convert enumeration type to "
                                      "legacy enumeration type");
        result.fields.emplace_back(std::string{field.name});
      }
      return result;
    },
    [&](const list_type& list) -> legacy_type {
      return legacy_list_type{list.value_type().to_legacy_type()};
    },
    [&](const map_type& map) -> legacy_type {
      return legacy_map_type{
        map.key_type().to_legacy_type(),
        map.value_type().to_legacy_type(),
      };
    },
    [&](const record_type& record) -> legacy_type {
      auto result = legacy_record_type{};
      for (const auto& field : record.fields())
        result.fields.push_back({
          std::string{field.name},
          field.type.to_legacy_type(),
        });
      return result;
    },
  };
  auto result = caf::visit(f, *this);
  if (is_alias(*this))
    result = legacy_alias_type{std::move(result)}.name(std::string{name()});
  for (const auto& attribute : attributes()) {
    if (attribute.value.empty())
      result.update_attributes({{std::string{attribute.key}}});
    else
      result.update_attributes({{
        std::string{attribute.key},
        std::string{attribute.value},
      }});
  }
  return result;
}

const fbs::Type& type::table(enum transparent transparent) const noexcept {
  const auto repr = as_bytes(*this);
  const auto* table = fbs::GetType(repr.data());
  VAST_ASSERT(table);
  const auto* resolved = resolve_transparent(table, transparent);
  VAST_ASSERT(resolved);
  return *resolved;
}

type::operator bool() const noexcept {
  return table(transparent::yes).type_type() != fbs::type::Type::NONE;
}

bool operator==(const type& lhs, const type& rhs) noexcept {
  const auto lhs_bytes = as_bytes(lhs);
  const auto rhs_bytes = as_bytes(rhs);
  return std::equal(lhs_bytes.begin(), lhs_bytes.end(), rhs_bytes.begin(),
                    rhs_bytes.end());
}

std::strong_ordering operator<=>(const type& lhs, const type& rhs) noexcept {
  auto lhs_bytes = as_bytes(lhs);
  auto rhs_bytes = as_bytes(rhs);
  // TODO: Replace implementation with `std::lexicographical_compare_three_way`
  // once that is implemented for all compilers we need to support. This does
  // the same thing essentially, just a lot less generic.
  if (lhs_bytes.data() == rhs_bytes.data()
      && lhs_bytes.size() == rhs_bytes.size())
    return std::strong_ordering::equal;
  while (!lhs_bytes.empty() && !rhs_bytes.empty()) {
    if (lhs_bytes[0] < rhs_bytes[0])
      return std::strong_ordering::less;
    if (lhs_bytes[0] > rhs_bytes[0])
      return std::strong_ordering::greater;
    lhs_bytes = lhs_bytes.subspan(1);
    rhs_bytes = rhs_bytes.subspan(1);
  }
  return !lhs_bytes.empty()   ? std::strong_ordering::greater
         : !rhs_bytes.empty() ? std::strong_ordering::less
                              : std::strong_ordering::equivalent;
}

uint8_t type::type_index() const noexcept {
  static_assert(
    std::is_same_v<uint8_t, std::underlying_type_t<vast::fbs::type::Type>>);
  return static_cast<uint8_t>(table(transparent::yes).type_type());
}

std::span<const std::byte> as_bytes(const type& x) noexcept {
  return x.table_ ? as_bytes(*x.table_) : none_type_representation();
}

data type::construct() const noexcept {
  auto f = []<concrete_type T>(const T& x) noexcept -> data {
    return x.construct();
  };
  return caf::visit(f, *this);
}

void inspect(caf::detail::stringification_inspector& f, type& x) {
  static_assert(
    std::is_same_v<caf::detail::stringification_inspector::result_type, void>);
  static_assert(caf::detail::stringification_inspector::reads_state);
  auto str = fmt::to_string(x);
  f(str);
}

void type::assign_metadata(const type& other) noexcept {
  if (!is_alias(other) && !other.has_attributes())
    return;
  const auto nested_bytes = as_bytes(table_);
  const auto reserved_size = [&]() noexcept {
    // The total length is made up from the following terms:
    // - 52 bytes FlatBuffers table framing
    // - Nested type FlatBuffers table size
    // - All contained string lengths, rounded up to four each
    // Note that this cannot account for attributes, since they are stored in
    // hash map which makes calculating the space requirements non-trivial.
    size_t size = 52;
    size += nested_bytes.size();
    size += reserved_string_size(other.name());
    return size;
  };
  auto builder = other.has_attributes()
                   ? flatbuffers::FlatBufferBuilder{reserved_size()}
                   : flatbuffers::FlatBufferBuilder{};
  const auto nested_type_offset = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(nested_bytes.data()), nested_bytes.size());
  const auto name_offset
    = is_alias(other) ? builder.CreateString(other.name()) : 0;
  const auto attributes_offset = [&]() noexcept
    -> flatbuffers::Offset<
      flatbuffers::Vector<flatbuffers::Offset<fbs::type::detail::Attribute>>> {
    if (!other.has_attributes())
      return 0;
    auto attributes_offsets
      = std::vector<flatbuffers::Offset<fbs::type::detail::Attribute>>{};
    for (const auto& attribute : other.attributes()) {
      const auto key_offset = builder.CreateString(attribute.key);
      const auto value_offset
        = attribute.value.empty() ? 0 : builder.CreateString(attribute.value);
      attributes_offsets.emplace_back(
        fbs::type::detail::CreateAttribute(builder, key_offset, value_offset));
    }
    return builder.CreateVectorOfSortedTables(&attributes_offsets);
  }();
  const auto enriched_type_offset = fbs::type::detail::CreateEnrichedType(
    builder, nested_type_offset, name_offset, attributes_offset);
  const auto type_offset = fbs::CreateType(
    builder, fbs::type::Type::enriched_type, enriched_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  table_ = chunk::make(std::move(result));
}

std::string_view type::name() const& noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
        return none_type::kind;
      case fbs::type::Type::bool_type:
        return bool_type::kind;
      case fbs::type::Type::integer_type:
        return integer_type::kind;
      case fbs::type::Type::count_type:
        return count_type::kind;
      case fbs::type::Type::real_type:
        return real_type::kind;
      case fbs::type::Type::duration_type:
        return duration_type::kind;
      case fbs::type::Type::time_type:
        return time_type::kind;
      case fbs::type::Type::string_type:
        return string_type::kind;
      case fbs::type::Type::pattern_type:
        return pattern_type::kind;
      case fbs::type::Type::address_type:
        return address_type::kind;
      case fbs::type::Type::subnet_type:
        return subnet_type::kind;
      case fbs::type::Type::enumeration_type:
        return enumeration_type::kind;
      case fbs::type::Type::list_type:
        return list_type::kind;
      case fbs::type::Type::map_type:
        return map_type::kind;
      case fbs::type::Type::record_type:
        return record_type::kind;
      case fbs::type::Type::enriched_type: {
        const auto* enriched_type = root->type_as_enriched_type();
        if (const auto* name = enriched_type->name())
          return name->string_view();
        root = enriched_type->type_nested_root();
        VAST_ASSERT(root);
        break;
      }
    }
  }
  __builtin_unreachable();
}

detail::generator<std::string_view> type::names() const& noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
        co_yield std::string_view{none_type::kind};
        co_return;
      case fbs::type::Type::bool_type:
        co_yield std::string_view{bool_type::kind};
        co_return;
      case fbs::type::Type::integer_type:
        co_yield std::string_view{integer_type::kind};
        co_return;
      case fbs::type::Type::count_type:
        co_yield std::string_view{count_type::kind};
        co_return;
      case fbs::type::Type::real_type:
        co_yield std::string_view{real_type::kind};
        co_return;
      case fbs::type::Type::duration_type:
        co_yield std::string_view{duration_type::kind};
        co_return;
      case fbs::type::Type::time_type:
        co_yield std::string_view{time_type::kind};
        co_return;
      case fbs::type::Type::string_type:
        co_yield std::string_view{string_type::kind};
        co_return;
      case fbs::type::Type::pattern_type:
        co_yield std::string_view{pattern_type::kind};
        co_return;
      case fbs::type::Type::address_type:
        co_yield std::string_view{address_type::kind};
        co_return;
      case fbs::type::Type::subnet_type:
        co_yield std::string_view{subnet_type::kind};
        co_return;
      case fbs::type::Type::enumeration_type:
        co_yield std::string_view{enumeration_type::kind};
        co_return;
      case fbs::type::Type::list_type:
        co_yield std::string_view{list_type::kind};
        co_return;
      case fbs::type::Type::map_type:
        co_yield std::string_view{map_type::kind};
        co_return;
      case fbs::type::Type::record_type:
        co_yield std::string_view{record_type::kind};
        co_return;
      case fbs::type::Type::enriched_type: {
        const auto* enriched_type = root->type_as_enriched_type();
        if (const auto* name = enriched_type->name())
          co_yield name->string_view();
        root = enriched_type->type_nested_root();
        VAST_ASSERT(root);
        break;
      }
    }
  }
  __builtin_unreachable();
}

std::optional<std::string_view>
type::attribute(const char* key) const& noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type:
      case fbs::type::Type::record_type:
        return std::nullopt;
      case fbs::type::Type::enriched_type: {
        const auto* enriched_type = root->type_as_enriched_type();
        if (const auto* attributes = enriched_type->attributes()) {
          if (const auto* attribute = attributes->LookupByKey(key)) {
            if (const auto* value = attribute->value())
              return value->string_view();
            return "";
          }
        }
        root = enriched_type->type_nested_root();
        VAST_ASSERT(root);
        break;
      }
    }
  }
  __builtin_unreachable();
}

bool type::has_attributes() const noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type:
      case fbs::type::Type::record_type:
        return false;
      case fbs::type::Type::enriched_type: {
        const auto* enriched_type = root->type_as_enriched_type();
        if (const auto* attributes = enriched_type->attributes()) {
          if (attributes->begin() != attributes->end())
            return true;
        }
        root = enriched_type->type_nested_root();
        VAST_ASSERT(root);
        break;
      }
    }
  }
  __builtin_unreachable();
}

detail::generator<std::pair<std::string_view, std::vector<type::attribute_view>>>
type::names_and_attributes() const& noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
        co_yield {none_type::kind, {}};
        co_return;
      case fbs::type::Type::bool_type:
        co_yield {bool_type::kind, {}};
        co_return;
      case fbs::type::Type::integer_type:
        co_yield {integer_type::kind, {}};
        co_return;
      case fbs::type::Type::count_type:
        co_yield {count_type::kind, {}};
        co_return;
      case fbs::type::Type::real_type:
        co_yield {real_type::kind, {}};
        co_return;
      case fbs::type::Type::duration_type:
        co_yield {duration_type::kind, {}};
        co_return;
      case fbs::type::Type::time_type:
        co_yield {time_type::kind, {}};
        co_return;
      case fbs::type::Type::string_type:
        co_yield {string_type::kind, {}};
        co_return;
      case fbs::type::Type::pattern_type:
        co_yield {pattern_type::kind, {}};
        co_return;
      case fbs::type::Type::address_type:
        co_yield {address_type::kind, {}};
        co_return;
      case fbs::type::Type::subnet_type:
        co_yield {subnet_type::kind, {}};
        co_return;
      case fbs::type::Type::enumeration_type:
        co_yield {enumeration_type::kind, {}};
        co_return;
      case fbs::type::Type::list_type:
        co_yield {list_type::kind, {}};
        co_return;
      case fbs::type::Type::map_type:
        co_yield {map_type::kind, {}};
        co_return;
      case fbs::type::Type::record_type:
        co_yield {record_type::kind, {}};
        co_return;
      case fbs::type::Type::enriched_type: {
        const auto* enriched_type = root->type_as_enriched_type();
        std::vector<attribute_view> attrs{};
        if (const auto* attributes = enriched_type->attributes()) {
          for (const auto& attribute : *attributes) {
            if (attribute->value() != nullptr
                && attribute->value()->begin() != attribute->value()->end())
              attrs.push_back({attribute->key()->string_view(),
                               attribute->value()->string_view()});
            else
              attrs.push_back({attribute->key()->string_view(), ""});
          }
        }
        if (enriched_type->name())
          co_yield {enriched_type->name()->string_view(), std::move(attrs)};
        else
          co_yield {"", std::move(attrs)};
        root = enriched_type->type_nested_root();
        VAST_ASSERT(root);
        break;
      }
    }
  }
  __builtin_unreachable();
}

detail::generator<type::attribute_view>
type::attributes(enum transparent transparent) const& noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type:
      case fbs::type::Type::record_type:
        co_return;
      case fbs::type::Type::enriched_type: {
        const auto* enriched_type = root->type_as_enriched_type();
        if (const auto* attributes = enriched_type->attributes()) {
          for (const auto& attribute : *attributes) {
            if (attribute->value() != nullptr
                && attribute->value()->begin() != attribute->value()->end())
              co_yield {attribute->key()->string_view(),
                        attribute->value()->string_view()};
            else
              co_yield {attribute->key()->string_view(), ""};
          }
        }
        if (transparent == transparent::no)
          if (enriched_type->name())
            co_return;
        root = enriched_type->type_nested_root();
        VAST_ASSERT(root);
        break;
      }
    }
  }
  __builtin_unreachable();
}

bool is_container(const type& type) noexcept {
  const auto& root = type.table(type::transparent::yes);
  switch (root.type_type()) {
    case fbs::type::Type::NONE:
    case fbs::type::Type::bool_type:
    case fbs::type::Type::integer_type:
    case fbs::type::Type::count_type:
    case fbs::type::Type::real_type:
    case fbs::type::Type::duration_type:
    case fbs::type::Type::time_type:
    case fbs::type::Type::string_type:
    case fbs::type::Type::pattern_type:
    case fbs::type::Type::address_type:
    case fbs::type::Type::subnet_type:
    case fbs::type::Type::enumeration_type:
      return false;
    case fbs::type::Type::list_type:
    case fbs::type::Type::map_type:
    case fbs::type::Type::record_type:
      return true;
    case fbs::type::Type::enriched_type:
      __builtin_unreachable();
  }
  __builtin_unreachable();
}

type flatten(const type& t) noexcept {
  if (const auto* rt = caf::get_if<record_type>(&t)) {
    auto result = type{flatten(*rt)};
    result.assign_metadata(t);
    return result;
  }
  return t;
}

bool congruent(const type& x, const type& y) noexcept {
  auto f = detail::overload{
    [](const enumeration_type& x, const enumeration_type& y) noexcept {
      const auto xf = x.fields();
      const auto yf = y.fields();
      if (xf.size() != yf.size())
        return false;
      for (size_t i = 0; i < xf.size(); ++i)
        if (xf[i].key != yf[i].key)
          return false;
      return true;
    },
    [](const list_type& x, const list_type& y) noexcept {
      return congruent(x.value_type(), y.value_type());
    },
    [](const map_type& x, const map_type& y) noexcept {
      return congruent(x.key_type(), y.key_type())
             && congruent(x.value_type(), y.value_type());
    },
    [](const record_type& x, const record_type& y) noexcept {
      if (x.num_fields() != y.num_fields())
        return false;
      for (size_t i = 0; i < x.num_fields(); ++i)
        if (!congruent(x.field(i).type, y.field(i).type))
          return false;
      return true;
    },
    []<complex_type T>(const T&, const T&) noexcept {
      static_assert(detail::always_false_v<T>, "missing congruency check for "
                                               "complex type");
    },
    []<concrete_type T, concrete_type U>(const T&, const U&) noexcept {
      return std::is_same_v<T, U>;
    },
  };
  return caf::visit(f, x, y);
}

bool congruent(const type& x, const data& y) noexcept {
  auto f = detail::overload{
    [](const auto&, const auto&) noexcept {
      return false;
    },
    [](const none_type&, caf::none_t) noexcept {
      return true;
    },
    [](const bool_type&, bool) noexcept {
      return true;
    },
    [](const integer_type&, integer) noexcept {
      return true;
    },
    [](const count_type&, count) noexcept {
      return true;
    },
    [](const real_type&, real) noexcept {
      return true;
    },
    [](const duration_type&, duration) noexcept {
      return true;
    },
    [](const time_type&, time) noexcept {
      return true;
    },
    [](const string_type&, const std::string&) noexcept {
      return true;
    },
    [](const pattern_type&, const pattern&) noexcept {
      return true;
    },
    [](const address_type&, const address&) noexcept {
      return true;
    },
    [](const subnet_type&, const subnet&) noexcept {
      return true;
    },
    [](const enumeration_type& x, const std::string& y) noexcept {
      return x.resolve(y).has_value();
    },
    [](const list_type&, const list&) noexcept {
      return true;
    },
    [](const map_type&, const map&) noexcept {
      return true;
    },
    [](const record_type& x, const list& y) noexcept {
      if (x.num_fields() != y.size())
        return false;
      for (size_t i = 0; i < x.num_fields(); ++i)
        if (!congruent(x.field(i).type, y[i]))
          return false;
      return true;
    },
    [](const record_type& x, const record& y) noexcept {
      if (x.num_fields() != y.size())
        return false;
      for (const auto& field : x.fields()) {
        if (auto it = y.find(field.name); it != y.end()) {
          if (!congruent(field.type, it->second))
            return false;
        } else {
          return false;
        }
      }
      return true;
    },
  };
  return caf::visit(f, x, y);
}

bool congruent(const data& x, const type& y) noexcept {
  return congruent(y, x);
}

bool compatible(const type& lhs, relational_operator op,
                const type& rhs) noexcept {
  auto string_and_pattern = [](auto& x, auto& y) {
    return (caf::holds_alternative<string_type>(x)
            && caf::holds_alternative<pattern_type>(y))
           || (caf::holds_alternative<pattern_type>(x)
               && caf::holds_alternative<string_type>(y));
  };
  switch (op) {
    case relational_operator::match:
    case relational_operator::not_match:
      return string_and_pattern(lhs, rhs);
    case relational_operator::equal:
    case relational_operator::not_equal:
      return !lhs || !rhs || string_and_pattern(lhs, rhs)
             || congruent(lhs, rhs);
    case relational_operator::less:
    case relational_operator::less_equal:
    case relational_operator::greater:
    case relational_operator::greater_equal:
      return congruent(lhs, rhs);
    case relational_operator::in:
    case relational_operator::not_in:
      if (caf::holds_alternative<string_type>(lhs))
        return caf::holds_alternative<string_type>(rhs) || is_container(rhs);
      else if (caf::holds_alternative<address_type>(lhs)
               || caf::holds_alternative<subnet_type>(lhs))
        return caf::holds_alternative<subnet_type>(rhs) || is_container(rhs);
      else
        return is_container(rhs);
    case relational_operator::ni:
      return compatible(rhs, relational_operator::in, lhs);
    case relational_operator::not_ni:
      return compatible(rhs, relational_operator::not_in, lhs);
  }
  __builtin_unreachable();
}

bool compatible(const type& lhs, relational_operator op,
                const data& rhs) noexcept {
  auto string_and_pattern = [](auto& x, auto& y) {
    return (caf::holds_alternative<string_type>(x)
            && caf::holds_alternative<pattern>(y))
           || (caf::holds_alternative<pattern_type>(x)
               && caf::holds_alternative<std::string>(y));
  };
  switch (op) {
    case relational_operator::match:
    case relational_operator::not_match:
      return string_and_pattern(lhs, rhs);
    case relational_operator::equal:
    case relational_operator::not_equal:
      return !lhs || caf::holds_alternative<caf::none_t>(rhs)
             || string_and_pattern(lhs, rhs) || congruent(lhs, rhs);
    case relational_operator::less:
    case relational_operator::less_equal:
    case relational_operator::greater:
    case relational_operator::greater_equal:
      return congruent(lhs, rhs);
    case relational_operator::in:
    case relational_operator::not_in:
      if (caf::holds_alternative<string_type>(lhs))
        return caf::holds_alternative<std::string>(rhs) || is_container(rhs);
      else if (caf::holds_alternative<address_type>(lhs)
               || caf::holds_alternative<subnet_type>(lhs))
        return caf::holds_alternative<subnet>(rhs) || is_container(rhs);
      else
        return is_container(rhs);
    case relational_operator::ni:
    case relational_operator::not_ni:
      if (caf::holds_alternative<std::string>(rhs))
        return caf::holds_alternative<string_type>(lhs) || is_container(lhs);
      else if (caf::holds_alternative<address>(rhs)
               || caf::holds_alternative<subnet>(rhs))
        return caf::holds_alternative<subnet_type>(lhs) || is_container(lhs);
      else
        return is_container(lhs);
  }
  __builtin_unreachable();
}

bool compatible(const data& lhs, relational_operator op,
                const type& rhs) noexcept {
  return compatible(rhs, flip(op), lhs);
}

bool is_subset(const type& x, const type& y) noexcept {
  const auto* sub = caf::get_if<record_type>(&x);
  const auto* super = caf::get_if<record_type>(&y);
  // If either of the types is not a record type, check if they are
  // congruent instead.
  if (!sub || !super)
    return congruent(x, y);
  // Check whether all fields of the subset exist in the superset.
  for (const auto& sub_field : sub->fields()) {
    bool exists_in_superset = false;
    for (const auto& super_field : super->fields()) {
      if (sub_field.name == super_field.name) {
        // Perform the check recursively to support nested record types.
        if (!is_subset(sub_field.type, super_field.type))
          return false;
        exists_in_superset = true;
      }
    }
    // Not all fields of the subset exist in the superset; exit early.
    if (!exists_in_superset)
      return false;
  }
  return true;
}

// WARNING: making changes to the logic of this function requires adapting the
// companion overload in view.cpp.
bool type_check(const type& x, const data& y) noexcept {
  auto f = detail::overload{
    [&](const none_type&, const auto&) {
      // Cannot determine data type since data may always be
      // null.
      return true;
    },
    [&](const auto&, const caf::none_t&) {
      // Every type can be assigned nil.
      return true;
    },
    [&](const none_type&, const caf::none_t&) {
      return true;
    },
    [&](const enumeration_type& t, const enumeration& u) {
      return !t.field(u).empty();
    },
    [&](const list_type& t, const list& u) {
      if (u.empty())
        return true;
      const auto vt = t.value_type();
      auto it = u.begin();
      const auto check = [&](const auto& d) noexcept {
        return type_check(vt, d);
      };
      if (check(*it)) {
        // Technically lists can contain heterogenous data,
        // but for optimization purposes we only check the
        // first element when assertions are disabled.
        VAST_ASSERT(std::all_of(it + 1, u.end(), check), //
                    "expected a homogenous list");
        return true;
      }
      return false;
    },
    [&](const map_type& t, const map& u) {
      if (u.empty())
        return true;
      const auto kt = t.key_type();
      const auto vt = t.value_type();
      auto it = u.begin();
      const auto check = [&](const auto& d) noexcept {
        return type_check(kt, d.first) && type_check(vt, d.second);
      };
      if (check(*it)) {
        // Technically maps can contain heterogenous data,
        // but for optimization purposes we only check the
        // first element when assertions are disabled.
        VAST_ASSERT(std::all_of(it + 1, u.end(), check), //
                    "expected a homogenous map");
        return true;
      }
      return false;
    },
    [&](const record_type& t, const record& u) {
      if (u.size() != t.num_fields())
        return false;
      for (size_t i = 0; i < u.size(); ++i) {
        const auto field = t.field(i);
        const auto& [k, v] = as_vector(u)[i];
        if (field.name != k || type_check(field.type, v))
          return false;
      }
      return true;
    },
    [&]<basic_type T, class U>(const T&, const U&) {
      // For basic types we can solely rely on the result of
      // construct.
      return std::is_same_v<type_to_data_t<T>, U>;
    },
    [&]<complex_type T, class U>(const T&, const U&) {
      // We don't have a matching overload.
      static_assert(!std::is_same_v<type_to_data_t<T>, U>, //
                    "missing type check overload");
      return false;
    },
  };
  return caf::visit(f, x, y);
}

caf::error
replace_if_congruent(std::initializer_list<type*> xs, const schema& with) {
  for (auto* x : xs)
    if (const auto* t = with.find(x->name())) {
      if (!congruent(*x, *t))
        return caf::make_error(ec::type_clash,
                               fmt::format("incongruent type {}", x->name()));
      *x = *t;
    }
  return caf::none;
}

// -- none_type ---------------------------------------------------------------

static_assert(none_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::NONE));

std::span<const std::byte> as_bytes(const none_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 12;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto type = fbs::CreateType(builder);
    builder.Finish(type);
    auto result = builder.Release();
    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

caf::none_t none_type::construct() noexcept {
  return {};
}

// -- bool_type ---------------------------------------------------------------

static_assert(bool_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::bool_type));

std::span<const std::byte> as_bytes(const bool_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto bool_type = fbs::type::CreateBoolType(builder);
    const auto type
      = fbs::CreateType(builder, fbs::type::Type::bool_type, bool_type.Union());
    builder.Finish(type);
    auto result = builder.Release();
    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

bool bool_type::construct() noexcept {
  return {};
}

// -- integer_type ------------------------------------------------------------

static_assert(integer_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::integer_type));

std::span<const std::byte> as_bytes(const integer_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto integer_type = fbs::type::CreateIntegerType(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::integer_type,
                                      integer_type.Union());
    builder.Finish(type);
    auto result = builder.Release();
    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

integer integer_type::construct() noexcept {
  return {};
}

// -- count_type --------------------------------------------------------------

static_assert(count_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::count_type));

std::span<const std::byte> as_bytes(const count_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto count_type = fbs::type::CreateCountType(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::count_type,
                                      count_type.Union());
    builder.Finish(type);
    auto result = builder.Release();
    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

count count_type::construct() noexcept {
  return {};
}

// -- real_type ---------------------------------------------------------------

static_assert(real_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::real_type));

std::span<const std::byte> as_bytes(const real_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto real_type = fbs::type::CreateRealType(builder);
    const auto type
      = fbs::CreateType(builder, fbs::type::Type::real_type, real_type.Union());
    builder.Finish(type);
    auto result = builder.Release();

    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

real real_type::construct() noexcept {
  return {};
}

// -- duration_type -----------------------------------------------------------

static_assert(duration_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::duration_type));

std::span<const std::byte> as_bytes(const duration_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto duration_type = fbs::type::CreateDurationType(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::duration_type,
                                      duration_type.Union());
    builder.Finish(type);
    auto result = builder.Release();

    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

duration duration_type::construct() noexcept {
  return {};
}

// -- time_type ---------------------------------------------------------------

static_assert(time_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::time_type));

std::span<const std::byte> as_bytes(const time_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto time_type = fbs::type::CreateDurationType(builder);
    const auto type
      = fbs::CreateType(builder, fbs::type::Type::time_type, time_type.Union());
    builder.Finish(type);
    auto result = builder.Release();

    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

time time_type::construct() noexcept {
  return {};
}

// -- string_type --------------------------------------------------------------

static_assert(string_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::string_type));

std::span<const std::byte> as_bytes(const string_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto string_type = fbs::type::CreateStringType(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::string_type,
                                      string_type.Union());
    builder.Finish(type);
    auto result = builder.Release();

    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

std::string string_type::construct() noexcept {
  return {};
}

// -- pattern_type ------------------------------------------------------------

static_assert(pattern_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::pattern_type));

std::span<const std::byte> as_bytes(const pattern_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto pattern_type = fbs::type::CreatePatternType(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::pattern_type,
                                      pattern_type.Union());
    builder.Finish(type);
    auto result = builder.Release();

    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

pattern pattern_type::construct() noexcept {
  return {};
}

// -- address_type ------------------------------------------------------------

static_assert(address_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::address_type));

std::span<const std::byte> as_bytes(const address_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto address_type = fbs::type::CreateAddressType(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::address_type,
                                      address_type.Union());
    builder.Finish(type);
    auto result = builder.Release();

    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

address address_type::construct() noexcept {
  return {};
}

// -- subnet_type -------------------------------------------------------------

static_assert(subnet_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::subnet_type));

std::span<const std::byte> as_bytes(const subnet_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto subnet_type = fbs::type::CreateSubnetType(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::subnet_type,
                                      subnet_type.Union());
    builder.Finish(type);
    auto result = builder.Release();

    VAST_ASSERT(result.size() == reserved_size);
    return result;
  }();
  return as_bytes(buffer);
}

subnet subnet_type::construct() noexcept {
  return {};
}

// -- enumeration_type --------------------------------------------------------

enumeration_type::enumeration_type(
  const enumeration_type& other) noexcept = default;

enumeration_type&
enumeration_type::operator=(const enumeration_type& rhs) noexcept = default;

enumeration_type::enumeration_type(enumeration_type&& other) noexcept = default;

enumeration_type&
enumeration_type::operator=(enumeration_type&& other) noexcept = default;

enumeration_type::~enumeration_type() noexcept = default;

enumeration_type::enumeration_type(
  const std::vector<field_view>& fields) noexcept {
  construct_enumeration_type(*this, fields.data(),
                             fields.data() + fields.size());
}

enumeration_type::enumeration_type(
  std::initializer_list<field_view> fields) noexcept
  : enumeration_type{std::vector<field_view>{fields}} {
  // nop
}

enumeration_type::enumeration_type(
  const std::vector<struct field>& fields) noexcept {
  construct_enumeration_type(*this, fields.data(),
                             fields.data() + fields.size());
}

const fbs::Type& enumeration_type::table() const noexcept {
  const auto repr = as_bytes(*this);
  const auto* table = fbs::GetType(repr.data());
  VAST_ASSERT(table);
  VAST_ASSERT(table == resolve_transparent(table));
  VAST_ASSERT(table->type_type() == fbs::type::Type::enumeration_type);
  return *table;
}

static_assert(enumeration_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::enumeration_type));

std::span<const std::byte> as_bytes(const enumeration_type& x) noexcept {
  return as_bytes_complex(x);
}

enumeration enumeration_type::construct() const noexcept {
  const auto* fields = table().type_as_enumeration_type()->fields();
  VAST_ASSERT(fields);
  VAST_ASSERT(fields->size() > 0);
  const auto value = fields->Get(0)->key();
  // TODO: Currently, enumeration can not holds keys that don't fit a uint8_t;
  // when switching to a strong typedef for enumeration we should change that.
  // An example use case fbs::type::Type::is NetFlow, where many enumeration
  // values require usage of a uint16_t, which for now we would need to model as
  // strings in schemas.
  VAST_ASSERT(value <= std::numeric_limits<enumeration>::max());
  return static_cast<enumeration>(value);
}

std::string_view enumeration_type::field(uint32_t key) const& noexcept {
  const auto* fields = table().type_as_enumeration_type()->fields();
  VAST_ASSERT(fields);
  if (const auto* field = fields->LookupByKey(key))
    return field->name()->string_view();
  return "";
}

std::vector<enumeration_type::field_view>
enumeration_type::fields() const& noexcept {
  const auto* fields = table().type_as_enumeration_type()->fields();
  VAST_ASSERT(fields);
  auto result = std::vector<field_view>{};
  result.reserve(fields->size());
  for (const auto& field : *fields)
    result.push_back({field->name()->string_view(), field->key()});
  return result;
}

std::optional<uint32_t>
enumeration_type::resolve(std::string_view key) const noexcept {
  const auto* fields = table().type_as_enumeration_type()->fields();
  VAST_ASSERT(fields);
  for (const auto& field : *fields)
    if (field->name()->string_view() == key)
      return field->key();
  return std::nullopt;
}

// -- list_type ---------------------------------------------------------------

list_type::list_type(const list_type& other) noexcept = default;

list_type& list_type::operator=(const list_type& rhs) noexcept = default;

list_type::list_type(list_type&& other) noexcept = default;

list_type& list_type::operator=(list_type&& other) noexcept = default;

list_type::~list_type() noexcept = default;

list_type::list_type(const type& value_type) noexcept {
  const auto value_type_bytes = as_bytes(value_type);
  const auto reserved_size = 44 + value_type_bytes.size();
  auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
  const auto list_type_offset = fbs::type::CreateListType(
    builder, builder.CreateVector(
               reinterpret_cast<const uint8_t*>(value_type_bytes.data()),
               value_type_bytes.size()));
  const auto type_offset = fbs::CreateType(builder, fbs::type::Type::list_type,
                                           list_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  VAST_ASSERT(result.size() == reserved_size);
  table_ = chunk::make(std::move(result));
}

const fbs::Type& list_type::table() const noexcept {
  const auto repr = as_bytes(*this);
  const auto* table = fbs::GetType(repr.data());
  VAST_ASSERT(table);
  VAST_ASSERT(table == resolve_transparent(table));
  VAST_ASSERT(table->type_type() == fbs::type::Type::list_type);
  return *table;
}

static_assert(list_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::list_type));

std::span<const std::byte> as_bytes(const list_type& x) noexcept {
  return as_bytes_complex(x);
}

list list_type::construct() noexcept {
  return {};
}

type list_type::value_type() const noexcept {
  const auto* view = table().type_as_list_type()->type();
  VAST_ASSERT(view);
  return type{table_->slice(as_bytes(*view))};
}

// -- map_type ----------------------------------------------------------------

map_type::map_type(const map_type& other) noexcept = default;

map_type& map_type::operator=(const map_type& rhs) noexcept = default;

map_type::map_type(map_type&& other) noexcept = default;

map_type& map_type::operator=(map_type&& other) noexcept = default;

map_type::~map_type() noexcept = default;

map_type::map_type(const type& key_type, const type& value_type) noexcept {
  const auto key_type_bytes = as_bytes(key_type);
  const auto value_type_bytes = as_bytes(value_type);
  const auto reserved_size
    = 52 + key_type_bytes.size() + value_type_bytes.size();
  auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
  const auto key_type_offset = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(key_type_bytes.data()),
    key_type_bytes.size());
  const auto value_type_offset = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(value_type_bytes.data()),
    value_type_bytes.size());
  const auto map_type_offset
    = fbs::type::CreateMapType(builder, key_type_offset, value_type_offset);
  const auto type_offset = fbs::CreateType(builder, fbs::type::Type::map_type,
                                           map_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  VAST_ASSERT(result.size() == reserved_size);
  table_ = chunk::make(std::move(result));
}

const fbs::Type& map_type::table() const noexcept {
  const auto repr = as_bytes(*this);
  const auto* table = fbs::GetType(repr.data());
  VAST_ASSERT(table);
  VAST_ASSERT(table == resolve_transparent(table));
  VAST_ASSERT(table->type_type() == fbs::type::Type::map_type);
  return *table;
}

static_assert(map_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::map_type));

std::span<const std::byte> as_bytes(const map_type& x) noexcept {
  return as_bytes_complex(x);
}

map map_type::construct() noexcept {
  return {};
}

type map_type::key_type() const noexcept {
  const auto* view = table().type_as_map_type()->key_type();
  VAST_ASSERT(view);
  return type{table_->slice(as_bytes(*view))};
}

type map_type::value_type() const noexcept {
  const auto* view = table().type_as_map_type()->value_type();
  VAST_ASSERT(view);
  return type{table_->slice(as_bytes(*view))};
}

// -- record_type -------------------------------------------------------------

record_type::record_type(const record_type& other) noexcept = default;

record_type& record_type::operator=(const record_type& rhs) noexcept = default;

record_type::record_type(record_type&& other) noexcept = default;

record_type& record_type::operator=(record_type&& other) noexcept = default;

record_type::~record_type() noexcept = default;

record_type::record_type(const std::vector<field_view>& fields) noexcept {
  construct_record_type(*this, fields.data(), fields.data() + fields.size());
}

record_type::record_type(std::initializer_list<field_view> fields) noexcept
  : record_type{std::vector<field_view>{fields}} {
  // nop
}

record_type::record_type(const std::vector<struct field>& fields) noexcept {
  construct_record_type(*this, fields.data(), fields.data() + fields.size());
}

const fbs::Type& record_type::table() const noexcept {
  const auto repr = as_bytes(*this);
  const auto* table = fbs::GetType(repr.data());
  VAST_ASSERT(table);
  VAST_ASSERT(table == resolve_transparent(table));
  VAST_ASSERT(table->type_type() == fbs::type::Type::record_type);
  return *table;
}

static_assert(record_type::type_index
              == static_cast<uint8_t>(fbs::type::Type::record_type));

std::span<const std::byte> as_bytes(const record_type& x) noexcept {
  return as_bytes_complex(x);
}

record record_type::construct() const noexcept {
  // A record is a stable map under the hood, and we construct its underlying
  // vector directly here as that is slightly more efficient, and as an added
  // benefit(?) allows for creating records with duplicate fields, so if this
  // record type happens to break its contract we can still create a fitting
  // record from it. Known occurences of such record types are:
  // - test.full blueprint record type for the test generator.
  // - Combined layout of the partition v0.
  // We should consider getting rid of vector_map::make_unsafe in the future.
  auto result = record::vector_type{};
  result.reserve(num_fields());
  for (const auto& field : fields())
    result.emplace_back(field.name, field.type.construct());
  return record::make_unsafe(std::move(result));
}

detail::generator<record_type::field_view>
record_type::fields() const noexcept {
  const auto* record = table().type_as_record_type();
  VAST_ASSERT(record);
  const auto* fields = record->fields();
  VAST_ASSERT(fields);
  for (const auto* field : *fields) {
    co_yield {
      field->name()->string_view(),
      type{table_->slice(as_bytes(*field->type()))},
    };
  }
  co_return;
}

detail::generator<record_type::leaf_view> record_type::leaves() const noexcept {
  auto index = offset{0};
  auto history = detail::stack_vector<const fbs::type::RecordType*, 64>{
    table().type_as_record_type()};
  while (!index.empty()) {
    const auto* record = history.back();
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // This is our exit condition: If we arrived at the end of a record, we need
    // to step out one layer. We must also reset the target key at this point.
    if (index.back() >= fields->size()) {
      history.pop_back();
      index.pop_back();
      if (!index.empty())
        ++index.back();
      continue;
    }
    const auto* field = record->fields()->Get(index.back());
    VAST_ASSERT(field);
    const auto* field_type = resolve_transparent(field->type_nested_root());
    VAST_ASSERT(field_type);
    switch (field_type->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type: {
        auto leaf = leaf_view{
          {
            field->name()->string_view(),
            type{table_->slice(as_bytes(*field->type()))},
          },
          index,
        };
        co_yield std::move(leaf);
        ++index.back();
        break;
      }
      case fbs::type::Type::record_type: {
        history.emplace_back(field_type->type_as_record_type());
        index.push_back(0);
        break;
      }
      case fbs::type::Type::enriched_type:
        __builtin_unreachable();
        break;
    }
  }
  co_return;
}

size_t record_type::num_fields() const noexcept {
  const auto* record = table().type_as_record_type();
  VAST_ASSERT(record);
  return record->fields()->size();
}

size_t record_type::num_leaves() const noexcept {
  auto num_leaves = size_t{0};
  auto index = offset{0};
  auto history = detail::stack_vector<const fbs::type::RecordType*, 64>{
    table().type_as_record_type()};
  while (!index.empty()) {
    const auto* record = history.back();
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // This is our exit condition: If we arrived at the end of a record, we need
    // to step out one layer. We must also reset the target key at this point.
    if (index.back() >= fields->size()) {
      history.pop_back();
      index.pop_back();
      if (!index.empty())
        ++index.back();
      continue;
    }
    const auto* field = record->fields()->Get(index.back());
    VAST_ASSERT(field);
    const auto* field_type = resolve_transparent(field->type_nested_root());
    VAST_ASSERT(field_type);
    switch (field_type->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type: {
        ++index.back();
        ++num_leaves;
        break;
      }
      case fbs::type::Type::record_type: {
        history.emplace_back(field_type->type_as_record_type());
        index.push_back(0);
        break;
      }
      case fbs::type::Type::enriched_type:
        __builtin_unreachable();
        break;
    }
  }
  return num_leaves;
}

offset record_type::resolve_flat_index(size_t flat_index) const noexcept {
  size_t current_flat_index = 0;
  auto index = offset{0};
  auto history = detail::stack_vector<const fbs::type::RecordType*, 64>{
    table().type_as_record_type()};
  while (!index.empty()) {
    const auto* record = history.back();
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // This is our exit condition: If we arrived at the end of a record, we need
    // to step out one layer. We must also reset the target key at this point.
    if (index.back() >= fields->size()) {
      history.pop_back();
      index.pop_back();
      if (!index.empty())
        ++index.back();
      continue;
    }
    const auto* field = record->fields()->Get(index.back());
    VAST_ASSERT(field);
    const auto* field_type = resolve_transparent(field->type_nested_root());
    VAST_ASSERT(field_type);
    switch (field_type->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type: {
        if (current_flat_index == flat_index)
          return index;
        ++current_flat_index;
        ++index.back();
        break;
      }
      case fbs::type::Type::record_type: {
        history.emplace_back(field_type->type_as_record_type());
        index.push_back(0);
        break;
      }
      case fbs::type::Type::enriched_type:
        __builtin_unreachable();
        break;
    }
  }
  die("index out of bounds");
}

std::optional<offset>
record_type::resolve_key(std::string_view key) const noexcept {
  auto index = offset{0};
  auto history = std::vector{std::pair{
    table().type_as_record_type(),
    key,
  }};
  while (!index.empty()) {
    const auto& [record, remaining_key] = history.back();
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // This is our exit condition: If we arrived at the end of a record, we need
    // to step out one layer. We must also reset the target key at this point.
    if (index.back() >= fields->size() || remaining_key.empty()) {
      history.pop_back();
      index.pop_back();
      if (!index.empty())
        ++index.back();
      continue;
    }
    const auto* field = record->fields()->Get(index.back());
    VAST_ASSERT(field);
    const auto* field_name = field->name();
    VAST_ASSERT(field_name);
    const auto* field_type = resolve_transparent(field->type_nested_root());
    VAST_ASSERT(field_type);
    switch (field_type->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type: {
        if (remaining_key == field_name->string_view())
          return index;
        ++index.back();
        break;
      }
      case fbs::type::Type::record_type: {
        auto [remaining_key_mismatch, field_name_mismatch]
          = std::mismatch(remaining_key.begin(), remaining_key.end(),
                          field_name->begin(), field_name->end());
        if (field_name_mismatch == field_name->end()
            && remaining_key_mismatch == remaining_key.end())
          return index;
        if (field_name_mismatch == field_name->end()
            && remaining_key_mismatch != remaining_key.end()
            && *remaining_key_mismatch == '.') {
          history.emplace_back(field_type->type_as_record_type(),
                               remaining_key.substr(1 + remaining_key_mismatch
                                                    - remaining_key.begin()));
          index.push_back(0);
        } else {
          ++index.back();
        }
        break;
      }
      case fbs::type::Type::enriched_type:
        __builtin_unreachable();
        break;
    }
  }
  return {};
}

detail::generator<offset>
record_type::resolve_key_suffix(std::string_view key,
                                std::string_view prefix) const noexcept {
  if (key.empty())
    co_return;
  auto index = offset{0};
  auto history = std::vector{
    std::pair{
      table().type_as_record_type(),
      std::vector{key},
    },
  };
  const auto* prefix_begin = prefix.begin();
  while (prefix_begin != prefix.end()) {
    const auto [prefix_mismatch, key_mismatch]
      = std::mismatch(prefix_begin, prefix.end(), key.begin(), key.end());
    if (prefix_mismatch == prefix.end() && key_mismatch != key.end()
        && *key_mismatch == '.')
      history[0].second.push_back(key.substr(1 + key_mismatch - key.begin()));
    prefix_begin = std::find(prefix_begin, prefix.end(), '.');
    if (prefix_begin == prefix.end())
      break;
    ++prefix_begin;
  }
  while (!index.empty()) {
    auto& [record, remaining_keys] = history.back();
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // This is our exit condition: If we arrived at the end of a record, we
    // need to step out one layer. We must also reset the target key at this
    // point.
    if (index.back() >= fields->size()) {
      history.pop_back();
      index.pop_back();
      if (!index.empty())
        ++index.back();
      continue;
    }
    const auto* field = record->fields()->Get(index.back());
    VAST_ASSERT(field);
    const auto* field_name = field->name();
    VAST_ASSERT(field_name);
    const auto* field_type = resolve_transparent(field->type_nested_root());
    VAST_ASSERT(field_type);
    switch (field_type->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type: {
        for (const auto& remaining_key : remaining_keys) {
          // TODO: Once we no longer support flattening types, we can switch to
          // an equality comparison between field_name and remaining_key here.
          const auto [field_name_mismatch, remaining_key_mismatch]
            = std::mismatch(field_name->rbegin(), field_name->rend(),
                            remaining_key.rbegin(), remaining_key.rend());
          if (remaining_key_mismatch == remaining_key.rend()
              && (field_name_mismatch == field_name->rend()
                  || *field_name_mismatch == '.')) {
            co_yield index;
            break;
          }
        }
        ++index.back();
        break;
      }
      case fbs::type::Type::record_type: {
        using history_entry = decltype(history)::value_type;
        auto next = history_entry{
          field_type->type_as_record_type(),
          history[0].second,
        };
        for (const auto& remaining_key : remaining_keys) {
          auto [remaining_key_mismatch, field_name_mismatch]
            = std::mismatch(remaining_key.begin(), remaining_key.end(),
                            field_name->begin(), field_name->end());
          if (field_name_mismatch == field_name->end()
              && remaining_key_mismatch != remaining_key.end()
              && *remaining_key_mismatch == '.') {
            next.second.emplace_back(remaining_key.substr(
              1 + remaining_key_mismatch - remaining_key.begin()));
          }
        }
        history.push_back(std::move(next));
        index.push_back(0);
        break;
      }
      case fbs::type::Type::enriched_type:
        __builtin_unreachable();
        break;
    }
  }
  co_return;
}

std::string_view record_type::key(size_t index) const& noexcept {
  const auto* record = table().type_as_record_type();
  VAST_ASSERT(record);
  VAST_ASSERT(index < record->fields()->size(), "index out of bounds");
  const auto* field = record->fields()->Get(index);
  VAST_ASSERT(field);
  return field->name()->string_view();
}

std::string record_type::key(const offset& index) const noexcept {
  auto result = std::string{};
  const auto* record = table().type_as_record_type();
  VAST_ASSERT(record);
  for (size_t i = 0; i < index.size() - 1; ++i) {
    VAST_ASSERT(index[i] < record->fields()->size());
    const auto* field = record->fields()->Get(index[i]);
    VAST_ASSERT(field);
    fmt::format_to(std::back_inserter(result), "{}.",
                   field->name()->string_view());
    record
      = resolve_transparent(field->type_nested_root())->type_as_record_type();
    VAST_ASSERT(record);
  }
  VAST_ASSERT(index.back() < record->fields()->size());
  const auto* field = record->fields()->Get(index.back());
  VAST_ASSERT(field);
  fmt::format_to(std::back_inserter(result), "{}",
                 field->name()->string_view());
  return result;
}

record_type::field_view record_type::field(size_t index) const noexcept {
  const auto* record = table().type_as_record_type();
  VAST_ASSERT(record);
  VAST_ASSERT(index < record->fields()->size(), "index out of bounds");
  const auto* field = record->fields()->Get(index);
  VAST_ASSERT(field);
  VAST_ASSERT(field->type());
  return {
    field->name()->string_view(),
    type{table_->slice(as_bytes(*field->type()))},
  };
}

record_type::field_view record_type::field(const offset& index) const noexcept {
  VAST_ASSERT(!index.empty(), "offset must not be empty");
  const auto* record = table().type_as_record_type();
  VAST_ASSERT(record);
  for (size_t i = 0; i < index.size() - 1; ++i) {
    VAST_ASSERT(index[i] < record->fields()->size(), "index out of bounds");
    record
      = resolve_transparent(record->fields()->Get(index[i])->type_nested_root())
          ->type_as_record_type();
    VAST_ASSERT(record, "offset contains excess indices");
  }
  VAST_ASSERT(index.back() < record->fields()->size(), "index out of bounds");
  const auto* field = record->fields()->Get(index.back());
  VAST_ASSERT(field);
  return {
    field->name()->string_view(),
    type{table_->slice(as_bytes(*field->type()))},
  };
}

size_t record_type::flat_index(const offset& index) const noexcept {
  VAST_ASSERT(!index.empty(), "index must not be empty");
  auto flat_index = size_t{0};
  auto current_index = offset{0};
  auto history = detail::stack_vector<const fbs::type::RecordType*, 64>{
    table().type_as_record_type()};
  while (true) {
    VAST_ASSERT(current_index <= index, "index out of bounds");
    const auto* record = history.back();
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // This is our exit condition: If we arrived at the end of a record, we need
    // to step out one layer.
    if (current_index.back() >= fields->size()) {
      history.pop_back();
      current_index.pop_back();
      VAST_ASSERT(!current_index.empty());
      ++current_index.back();
      continue;
    }
    const auto* field = record->fields()->Get(current_index.back());
    VAST_ASSERT(field);
    const auto* field_type = resolve_transparent(field->type_nested_root());
    VAST_ASSERT(field_type);
    switch (field_type->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type: {
        if (index == current_index)
          return flat_index;
        ++current_index.back();
        ++flat_index;
        break;
      }
      case fbs::type::Type::record_type: {
        VAST_ASSERT(index != current_index);
        history.emplace_back(field_type->type_as_record_type());
        current_index.push_back(0);
        break;
      }
      case fbs::type::Type::enriched_type:
        __builtin_unreachable();
        break;
    }
  }
}

record_type::transformation::function_type record_type::drop() noexcept {
  return [](const field_view&) noexcept -> std::vector<struct field> {
    return {};
  };
}

record_type::transformation::function_type
record_type::assign(std::vector<struct field> fields) noexcept {
  return [fields = std::move(fields)](
           const field_view&) noexcept -> std::vector<struct field> {
    return fields;
  };
}

record_type::transformation::function_type
record_type::insert_before(std::vector<struct field> fields) noexcept {
  return [fields = std::move(fields)](const field_view& field) mutable noexcept
         -> std::vector<struct field> {
    fields.reserve(fields.size() + 1);
    fields.push_back({std::string{field.name}, field.type});
    return fields;
  };
}

record_type::transformation::function_type
record_type::insert_after(std::vector<struct field> fields) noexcept {
  return [fields = std::move(fields)](const field_view& field) mutable noexcept
         -> std::vector<struct field> {
    fields.reserve(fields.size() + 1);
    fields.insert(fields.begin(), {std::string{field.name}, field.type});
    return fields;
  };
}

std::optional<record_type> record_type::transform(
  std::vector<transformation> transformations) const noexcept {
  // This function recursively calls do_transform while iterating over all the
  // leaves of the record type backwards.
  //
  // Given this record type:
  //
  //   type x = record {
  //     a: record {
  //       b: integer,
  //       c: integer,
  //     },
  //     d: record {
  //       e: record {
  //         f: integer,
  //       },
  //     },
  //   }
  //
  // A transformation of `d.e` will cause `f` and `a` to be untouched and
  // simply copied to the new record type, while all the other fields need to
  // be re-created. This is essentially an optimization over the naive approach
  // that recursively converts the record type into a list of record fields,
  // and then modifies that. As an additional benefit this function allows for
  // applying multiple transformations at the same time.
  //
  // This algorithm works by walking over the transformations in reverse order
  // (by offset), and unwrapping the record type into fields for all fields
  // whose offset is a prefix of the transformations target offset.
  // Transformations are applied when unwrapping if the target offset matches
  // the field offset exactly. After having walked over a record type, the
  // fields are joined back together at the end of the recursive lambda call.
  const auto do_transform
    = [](const auto& do_transform, const record_type& self, offset index,
         auto& current, const auto end) noexcept -> std::optional<record_type> {
    if (current == end)
      return self;
    auto new_fields = std::vector<struct field>{};
    new_fields.reserve(self.num_fields());
    index.emplace_back(self.num_fields());
    while (index.back() > 0 && current != end) {
      const auto& old_field = self.field(--index.back());
      // Compare the offsets of the next target with our current offset.
      const auto [index_mismatch, current_index_mismatch]
        = std::mismatch(index.begin(), index.end(), current->index.begin(),
                        current->index.end());
      if (index_mismatch == index.end()
          && current_index_mismatch == current->index.end()) {
        // The offset matches exactly, so we apply the transformation.
        do {
          auto replacements = std::invoke(std::move(current->fun), old_field);
          std::move(replacements.rbegin(), replacements.rend(),
                    std::back_inserter(new_fields));
          ++current;
        } while (current != end && current->index == index);
      } else if (index_mismatch == index.end()) {
        // The index is a prefix of the target offset for the next
        // transformation, so we recurse one level deeper.
        VAST_ASSERT(caf::holds_alternative<record_type>(old_field.type));
        if (auto sub_result
            = do_transform(do_transform, caf::get<record_type>(old_field.type),
                           index, current, end))
          new_fields.push_back({
            std::string{old_field.name},
            std::move(*sub_result),
          });
        // Check for invalid arguments on the way in.
        VAST_ASSERT(current == end || index != current->index,
                    "cannot apply transformations to both a nested record type "
                    "and its children at the same time.");
      } else {
        // Check for invalid arguments on the way out.
        VAST_ASSERT(current_index_mismatch != current->index.end(),
                    "cannot apply transformations to both a nested record type "
                    "and its children at the same time.");
        // We don't have a match and we also don't have a transformation, so
        // we just leave the field untouched.
        new_fields.push_back({
          std::string{old_field.name},
          old_field.type,
        });
      }
    }
    // In case fbs::type::Type::we exited the loop earlier, we still have to add
    // all the remaining fields back to the modified record (untouched).
    while (index.back() > 0) {
      const auto& old_field = self.field(--index.back());
      new_fields.push_back({
        std::string{old_field.name},
        old_field.type,
      });
    }
    if (new_fields.empty())
      return std::nullopt;
    type result{};
    construct_record_type(result, new_fields.rbegin(), new_fields.rend());
    return caf::get<record_type>(result);
  };
  // Verify that transformations are sorted in order.
  VAST_ASSERT(std::is_sorted(transformations.begin(), transformations.end(),
                             [](const auto& lhs, const auto& rhs) noexcept {
                               return lhs.index <= rhs.index;
                             }));
  auto current = transformations.rbegin();
  auto result
    = do_transform(do_transform, *this, {}, current, transformations.rend());
  VAST_ASSERT(current == transformations.rend(), "index out of bounds");
  return result;
}

caf::expected<record_type>
merge(const record_type& lhs, const record_type& rhs,
      enum record_type::merge_conflict merge_conflict) noexcept {
  auto do_merge = [&](const record_type::field_view& lfield,
                      const record_type::field_view& rfield) noexcept {
    return detail::overload{
      [&](const record_type& lhs,
          const record_type& rhs) noexcept -> caf::expected<type> {
        if (auto result = merge(lhs, rhs, merge_conflict))
          return type{*result};
        else
          return result.error();
      },
      [&]<concrete_type T, concrete_type U>(
        const T& lhs, const U& rhs) noexcept -> caf::expected<type> {
        switch (merge_conflict) {
          case record_type::merge_conflict::fail: {
            if (congruent(type{lhs}, type{rhs})) {
              if (lfield.type.name() != rfield.type.name())
                return caf::make_error(
                  ec::logic_error,
                  fmt::format("conflicting alias types {} and {} for "
                              "field {}; failed to merge {} and {}",
                              lfield.type.name(), rfield.type.name(),
                              rfield.name, lhs, rhs));
              auto to_vector
                = [](detail::generator<type::attribute_view>&& rng) {
                    auto result = std::vector<type::attribute_view>{};
                    for (auto&& elem : std::move(rng))
                      result.push_back(std::move(elem));
                    return result;
                  };
              const auto lhs_attributes = to_vector(lfield.type.attributes());
              const auto rhs_attributes = to_vector(rfield.type.attributes());
              const auto conflicting_attribute = std::any_of(
                lhs_attributes.begin(), lhs_attributes.end(),
                [&](const auto& lhs_attribute) noexcept {
                  return rfield.type.attribute(lhs_attribute.key.data())
                         != lhs_attribute.value;
                });
              if (conflicting_attribute)
                return caf::make_error(ec::logic_error,
                                       fmt::format("conflicting attributes for "
                                                   "field {}; failed to "
                                                   "merge {} and {}",
                                                   rfield.name, lhs, rhs));
              auto attributes = std::vector<struct type::attribute>{};
              for (const auto& attribute : lhs_attributes)
                attributes.push_back(
                  {std::string{attribute.key},
                   std::optional{std::string{attribute.value}}});
              for (const auto& attribute : rhs_attributes)
                attributes.push_back(
                  {std::string{attribute.key},
                   std::optional{std::string{attribute.value}}});
              return type{lfield.type.name(), lfield.type, attributes};
            }
            return caf::make_error(ec::logic_error,
                                   fmt::format("conflicting field {}; "
                                               "failed to "
                                               "merge {} and {}",
                                               rfield.name, lhs, rhs));
          }
          case record_type::merge_conflict::prefer_left:
            return lfield.type;
          case record_type::merge_conflict::prefer_right:
            return rfield.type;
        }
        __builtin_unreachable();
      },
    };
  };
  auto transformations = std::vector<record_type::transformation>{};
  auto additions = std::vector<struct record_type::field>{};
  transformations.reserve(rhs.num_fields());
  auto err = caf::error{};
  for (auto rfield : rhs.fields()) {
    if (const auto& lindex = lhs.resolve_key(rfield.name)) {
      transformations.push_back({
        *lindex,
        ([&, rfield = std::move(rfield)](
           const record_type::field_view& lfield) mutable noexcept
         -> std::vector<struct record_type::field> {
          if (auto result = caf::visit(do_merge(lfield, rfield), lfield.type,
                                       rfield.type)) {
            return {{
              std::string{rfield.name},
              *result,
            }};
          } else {
            err = std::move(result.error());
            return {};
          }
        }),
      });
    } else {
      additions.push_back({std::string{rfield.name}, rfield.type});
    }
  }
  auto result = lhs.transform(std::move(transformations));
  if (err)
    return err;
  VAST_ASSERT(result);
  result = result->transform({{
    {result->num_fields() - 1},
    record_type::insert_after(std::move(additions)),
  }});
  VAST_ASSERT(result);
  return std::move(*result);
}

record_type flatten(const record_type& type) noexcept {
  auto fields = std::vector<struct record_type::field>{};
  for (const auto& [field, offset] : type.leaves())
    fields.push_back({
      type.key(offset),
      field.type,
    });
  return record_type{fields};
}

} // namespace vast

// -- sum_type_access ---------------------------------------------------------

namespace caf {

uint8_t
sum_type_access<vast::type>::index_from_type(const vast::type& x) noexcept {
  static const auto table = []<vast::concrete_type... Ts, uint8_t... Indices>(
    caf::detail::type_list<Ts...>,
    std::integer_sequence<uint8_t, Indices...>) noexcept {
    std::array<uint8_t, std::numeric_limits<uint8_t>::max()> tbl{};
    tbl.fill(std::numeric_limits<uint8_t>::max());
    (static_cast<void>(tbl[Ts::type_index] = Indices), ...);
    return tbl;
  }
  (types{},
   std::make_integer_sequence<uint8_t, caf::detail::tl_size<types>::value>());
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
  auto result = table[x.type_index()];
  VAST_ASSERT(result != std::numeric_limits<uint8_t>::max());
  return result;
}

} // namespace caf
