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
#include "vast/logger.hpp"
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
      case fbs::type::Type::bool_type_v0:
      case fbs::type::Type::integer_type_v0:
      case fbs::type::Type::count_type_v0:
      case fbs::type::Type::real_type_v0:
      case fbs::type::Type::duration_type_v0:
      case fbs::type::Type::time_type_v0:
      case fbs::type::Type::string_type_v0:
      case fbs::type::Type::pattern_type_v0:
      case fbs::type::Type::address_type_v0:
      case fbs::type::Type::subnet_type_v0:
      case fbs::type::Type::enumeration_type_v0:
      case fbs::type::Type::list_type_v0:
      case fbs::type::Type::map_type_v0:
      case fbs::type::Type::record_type_v0:
        transparent = type::transparent::no;
        break;
      case fbs::type::Type::tagged_type_v0:
        root = root->type_as_tagged_type_v0()->type_nested_root();
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
      case fbs::type::Type::bool_type_v0:
      case fbs::type::Type::integer_type_v0:
      case fbs::type::Type::count_type_v0:
      case fbs::type::Type::real_type_v0:
      case fbs::type::Type::duration_type_v0:
      case fbs::type::Type::time_type_v0:
      case fbs::type::Type::string_type_v0:
      case fbs::type::Type::pattern_type_v0:
      case fbs::type::Type::address_type_v0:
      case fbs::type::Type::subnet_type_v0:
      case fbs::type::Type::enumeration_type_v0:
      case fbs::type::Type::list_type_v0:
      case fbs::type::Type::map_type_v0:
      case fbs::type::Type::record_type_v0:
        return result;
      case fbs::type::Type::tagged_type_v0:
        const auto* tagged = root->type_as_tagged_type_v0();
        VAST_ASSERT(tagged);
        root = tagged->type_nested_root();
        VAST_ASSERT(root);
        result = as_bytes(*tagged->type());
        break;
    }
  }
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
    = std::vector<flatbuffers::Offset<fbs::type::enumeration_type::field::v0>>{};
  field_offsets.reserve(end - begin);
  uint32_t next_key = 0;
  for (const auto* it = begin; it != end; ++it) {
    const auto key
      = it->key != std::numeric_limits<uint32_t>::max() ? it->key : next_key;
    next_key = key + 1;
    const auto name_offset = builder.CreateString(it->name);
    field_offsets.emplace_back(
      fbs::type::enumeration_type::field::Createv0(builder, key, name_offset));
  }
  const auto fields_offset = builder.CreateVectorOfSortedTables(&field_offsets);
  const auto enumeration_type_offset
    = fbs::type::enumeration_type::Createv0(builder, fields_offset);
  const auto type_offset
    = fbs::CreateType(builder, fbs::type::Type::enumeration_type_v0,
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
    = std::vector<flatbuffers::Offset<fbs::type::record_type::field::v0>>{};
  field_offsets.reserve(end - begin);
  for (auto it = begin; it != end; ++it) {
    const auto type_bytes = as_bytes(it->type);
    const auto name_offset = builder.CreateString(it->name);
    const auto type_offset = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(type_bytes.data()), type_bytes.size());
    field_offsets.emplace_back(fbs::type::record_type::field::Createv0(
      builder, name_offset, type_offset));
  }
  const auto fields_offset = builder.CreateVector(field_offsets);
  const auto record_type_offset
    = fbs::type::record_type::Createv0(builder, fields_offset);
  const auto type_offset = fbs::CreateType(
    builder, fbs::type::Type::record_type_v0, record_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  VAST_ASSERT(result.size() == reserved_size);
  auto chunk = chunk::make(std::move(result));
  self = type{std::move(chunk)};
}

size_t flat_size(const fbs::Type* view) noexcept {
  VAST_ASSERT(view);
  switch (view->type_type()) {
    case fbs::type::Type::NONE:
    case fbs::type::Type::bool_type_v0:
    case fbs::type::Type::integer_type_v0:
    case fbs::type::Type::count_type_v0:
    case fbs::type::Type::real_type_v0:
    case fbs::type::Type::duration_type_v0:
    case fbs::type::Type::time_type_v0:
    case fbs::type::Type::string_type_v0:
    case fbs::type::Type::pattern_type_v0:
    case fbs::type::Type::address_type_v0:
    case fbs::type::Type::subnet_type_v0:
    case fbs::type::Type::enumeration_type_v0:
    case fbs::type::Type::list_type_v0:
    case fbs::type::Type::map_type_v0:
      return 1;
    case fbs::type::Type::record_type_v0: {
      const auto* record = view->type_as_record_type_v0();
      auto result = size_t{0};
      for (const auto& field : *record->fields())
        result += flat_size(field->type_nested_root());
      return result;
    }
    case fbs::type::Type::tagged_type_v0:
      return flat_size(view->type_as_tagged_type_v0()->type_nested_root());
  }
};

} // namespace

// -- stateful_type_base ------------------------------------------------------

const fbs::Type&
stateful_type_base::table(enum transparent transparent) const noexcept {
  const auto& repr = as_bytes(static_cast<const type&>(*this));
  return *resolve_transparent(fbs::GetType(repr.data()), transparent);
}

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
           const std::vector<struct tag>& tags) noexcept {
  if (name.empty() && tags.empty()) {
    // This special case exists for easier conversion of legacy types, which did
    // not require an legacy alias type wrapping to have a name.
    *this = nested;
  } else {
    const auto nested_bytes = as_bytes(nested);
    const auto reserved_size = [&]() noexcept {
      // The total length is made up from the following terms:
      // - 52 bytes FlatBuffers table framing
      // - Nested type FlatBuffers table size
      // - All contained string lengths, rounded up to four each
      // Note that this cannot account for tags, since they are stored in hash
      // map which makes calculating the space requirements non-trivial.
      size_t size = 52;
      size += nested_bytes.size();
      size += reserved_string_size(name);
      return size;
    };
    auto builder = tags.empty()
                     ? flatbuffers::FlatBufferBuilder{reserved_size()}
                     : flatbuffers::FlatBufferBuilder{};
    const auto nested_type_offset = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(nested_bytes.data()),
      nested_bytes.size());
    const auto name_offset = name.empty() ? 0 : builder.CreateString(name);
    const auto tags_offset =
      [&]() noexcept -> flatbuffers::Offset<flatbuffers::Vector<
                       flatbuffers::Offset<fbs::type::tagged_type::tag::v0>>> {
      if (tags.empty())
        return 0;
      auto tags_offsets
        = std::vector<flatbuffers::Offset<fbs::type::tagged_type::tag::v0>>{};
      tags_offsets.reserve(tags.size());
      for (const auto& tag : tags) {
        const auto key_offset = builder.CreateString(tag.key);
        const auto value_offset
          = tag.value ? builder.CreateString(*tag.value) : 0;
        tags_offsets.emplace_back(fbs::type::tagged_type::tag::Createv0(
          builder, key_offset, value_offset));
      }
      return builder.CreateVectorOfSortedTables(&tags_offsets);
    }();
    const auto tagged_type_offset = fbs::type::tagged_type::Createv0(
      builder, nested_type_offset, name_offset, tags_offset);
    const auto type_offset = fbs::CreateType(
      builder, fbs::type::Type::tagged_type_v0, tagged_type_offset.Union());
    builder.Finish(type_offset);
    auto result = builder.Release();
    table_ = chunk::make(std::move(result));
  }
}

type::type(std::string_view name, const type& nested) noexcept
  : type(name, nested, {}) {
  // nop
}

type::type(const type& nested, const std::vector<struct tag>& tags) noexcept
  : type("", nested, tags) {
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
  const auto tags = [&] {
    auto result = std::vector<struct tag>{};
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
      return type{other.name(), none_type{}, tags};
    },
    [&](const legacy_bool_type&) {
      return type{other.name(), bool_type{}, tags};
    },
    [&](const legacy_integer_type&) {
      return type{other.name(), integer_type{}, tags};
    },
    [&](const legacy_count_type&) {
      return type{other.name(), count_type{}, tags};
    },
    [&](const legacy_real_type&) {
      return type{other.name(), real_type{}, tags};
    },
    [&](const legacy_duration_type&) {
      return type{other.name(), duration_type{}, tags};
    },
    [&](const legacy_time_type&) {
      return type{other.name(), time_type{}, tags};
    },
    [&](const legacy_string_type&) {
      return type{other.name(), string_type{}, tags};
    },
    [&](const legacy_pattern_type&) {
      return type{other.name(), pattern_type{}, tags};
    },
    [&](const legacy_address_type&) {
      return type{other.name(), address_type{}, tags};
    },
    [&](const legacy_subnet_type&) {
      return type{other.name(), subnet_type{}, tags};
    },
    [&](const legacy_enumeration_type& enumeration) {
      auto fields = std::vector<struct enumeration_type::field>{};
      fields.reserve(enumeration.fields.size());
      for (const auto& field : enumeration.fields)
        fields.push_back({field});
      return type{other.name(), enumeration_type{fields}, tags};
    },
    [&](const legacy_list_type& list) {
      return type{other.name(), list_type{from_legacy_type(list.value_type)},
                  tags};
    },
    [&](const legacy_map_type& list) {
      return type{other.name(),
                  map_type{from_legacy_type(list.key_type),
                           from_legacy_type(list.value_type)},
                  tags};
    },
    [&](const legacy_alias_type& alias) {
      return type{other.name(), from_legacy_type(alias.value_type), tags};
    },
    [&](const legacy_record_type& record) {
      auto fields = std::vector<struct record_type::field_view>{};
      fields.reserve(record.fields.size());
      for (const auto& field : record.fields)
        fields.push_back({field.name, from_legacy_type(field.type)});
      return type{other.name(), record_type{fields}, tags};
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
  if (!name().empty())
    result = legacy_alias_type{std::move(result)}.name(std::string{name()});
  for (const auto& tag : tags()) {
    if (tag.value.empty())
      result.update_attributes({{std::string{tag.key}}});
    else
      result.update_attributes({{
        std::string{tag.key},
        std::string{tag.value},
      }});
  }
  return result;
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
  const auto name = other.name();
  const auto tags = other.tags();
  if (name.empty() && tags.empty())
    return;
  const auto nested_bytes = as_bytes(table_);
  const auto reserved_size = [&]() noexcept {
    // The total length is made up from the following terms:
    // - 52 bytes FlatBuffers table framing
    // - Nested type FlatBuffers table size
    // - All contained string lengths, rounded up to four each
    // Note that this cannot account for tags, since they are stored in hash
    // map which makes calculating the space requirements non-trivial.
    size_t size = 52;
    size += nested_bytes.size();
    size += reserved_string_size(name);
    return size;
  };
  auto builder = tags.empty() ? flatbuffers::FlatBufferBuilder{reserved_size()}
                              : flatbuffers::FlatBufferBuilder{};
  const auto nested_type_offset = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(nested_bytes.data()), nested_bytes.size());
  const auto name_offset = name.empty() ? 0 : builder.CreateString(name);
  const auto tags_offset
    = [&]() noexcept -> flatbuffers::Offset<flatbuffers::Vector<
                       flatbuffers::Offset<fbs::type::tagged_type::tag::v0>>> {
    if (tags.empty())
      return 0;
    auto tags_offsets
      = std::vector<flatbuffers::Offset<fbs::type::tagged_type::tag::v0>>{};
    tags_offsets.reserve(tags.size());
    for (const auto& tag : tags) {
      const auto key_offset = builder.CreateString(tag.key);
      const auto value_offset
        = tag.value.empty() ? 0 : builder.CreateString(tag.value);
      tags_offsets.emplace_back(fbs::type::tagged_type::tag::Createv0(
        builder, key_offset, value_offset));
    }
    return builder.CreateVectorOfSortedTables(&tags_offsets);
  }();
  const auto tagged_type_offset = fbs::type::tagged_type::Createv0(
    builder, nested_type_offset, name_offset, tags_offset);
  const auto type_offset = fbs::CreateType(
    builder, fbs::type::Type::tagged_type_v0, tagged_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  table_ = chunk::make(std::move(result));
}

std::string_view type::name() const& noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type_v0:
      case fbs::type::Type::integer_type_v0:
      case fbs::type::Type::count_type_v0:
      case fbs::type::Type::real_type_v0:
      case fbs::type::Type::duration_type_v0:
      case fbs::type::Type::time_type_v0:
      case fbs::type::Type::string_type_v0:
      case fbs::type::Type::pattern_type_v0:
      case fbs::type::Type::address_type_v0:
      case fbs::type::Type::subnet_type_v0:
      case fbs::type::Type::enumeration_type_v0:
      case fbs::type::Type::list_type_v0:
      case fbs::type::Type::map_type_v0:
      case fbs::type::Type::record_type_v0:
        return "";
      case fbs::type::Type::tagged_type_v0:
        const auto* tagged_type = root->type_as_tagged_type_v0();
        if (const auto* name = tagged_type->name())
          return name->string_view();
        root = tagged_type->type_nested_root();
        VAST_ASSERT(root);
        break;
    }
  }
}

std::vector<std::string_view> type::names() const& noexcept {
  auto result = std::vector<std::string_view>{};
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type_v0:
      case fbs::type::Type::integer_type_v0:
      case fbs::type::Type::count_type_v0:
      case fbs::type::Type::real_type_v0:
      case fbs::type::Type::duration_type_v0:
      case fbs::type::Type::time_type_v0:
      case fbs::type::Type::string_type_v0:
      case fbs::type::Type::pattern_type_v0:
      case fbs::type::Type::address_type_v0:
      case fbs::type::Type::subnet_type_v0:
      case fbs::type::Type::enumeration_type_v0:
      case fbs::type::Type::list_type_v0:
      case fbs::type::Type::map_type_v0:
      case fbs::type::Type::record_type_v0:
        return result;
      case fbs::type::Type::tagged_type_v0:
        const auto* tagged_type = root->type_as_tagged_type_v0();
        if (const auto* name = tagged_type->name())
          result.push_back(name->string_view());
        root = tagged_type->type_nested_root();
        VAST_ASSERT(root);
        break;
    }
  }
}

std::optional<std::string_view> type::tag(const char* key) const& noexcept {
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type_v0:
      case fbs::type::Type::integer_type_v0:
      case fbs::type::Type::count_type_v0:
      case fbs::type::Type::real_type_v0:
      case fbs::type::Type::duration_type_v0:
      case fbs::type::Type::time_type_v0:
      case fbs::type::Type::string_type_v0:
      case fbs::type::Type::pattern_type_v0:
      case fbs::type::Type::address_type_v0:
      case fbs::type::Type::subnet_type_v0:
      case fbs::type::Type::enumeration_type_v0:
      case fbs::type::Type::list_type_v0:
      case fbs::type::Type::map_type_v0:
      case fbs::type::Type::record_type_v0:
        return std::nullopt;
      case fbs::type::Type::tagged_type_v0:
        const auto* tagged_type = root->type_as_tagged_type_v0();
        if (const auto* tags = tagged_type->tags()) {
          if (const auto* tag = tags->LookupByKey(key)) {
            if (const auto* value = tag->value())
              return value->string_view();
            return "";
          }
        }
        root = tagged_type->type_nested_root();
        VAST_ASSERT(root);
        break;
    }
  }
}

std::vector<type::tag_view> type::tags() const& noexcept {
  auto result = std::vector<type::tag_view>{};
  const auto* root = &table(transparent::no);
  while (true) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type_v0:
      case fbs::type::Type::integer_type_v0:
      case fbs::type::Type::count_type_v0:
      case fbs::type::Type::real_type_v0:
      case fbs::type::Type::duration_type_v0:
      case fbs::type::Type::time_type_v0:
      case fbs::type::Type::string_type_v0:
      case fbs::type::Type::pattern_type_v0:
      case fbs::type::Type::address_type_v0:
      case fbs::type::Type::subnet_type_v0:
      case fbs::type::Type::enumeration_type_v0:
      case fbs::type::Type::list_type_v0:
      case fbs::type::Type::map_type_v0:
      case fbs::type::Type::record_type_v0:
        return result;
      case fbs::type::Type::tagged_type_v0:
        const auto* tagged_type = root->type_as_tagged_type_v0();
        if (const auto* tags = tagged_type->tags()) {
          result.reserve(result.size() + tags->size());
          for (const auto& tag : *tags)
            result.push_back({
              tag->key()->string_view(),
              tag->value() ? tag->value()->string_view() : "",
            });
        }
        root = tagged_type->type_nested_root();
        VAST_ASSERT(root);
        break;
    }
  }
}

bool is_container(const type& type) noexcept {
  const auto& root = type.table(type::transparent::yes);
  switch (root.type_type()) {
    case fbs::type::Type::NONE:
    case fbs::type::Type::bool_type_v0:
    case fbs::type::Type::integer_type_v0:
    case fbs::type::Type::count_type_v0:
    case fbs::type::Type::real_type_v0:
    case fbs::type::Type::duration_type_v0:
    case fbs::type::Type::time_type_v0:
    case fbs::type::Type::string_type_v0:
    case fbs::type::Type::pattern_type_v0:
    case fbs::type::Type::address_type_v0:
    case fbs::type::Type::subnet_type_v0:
    case fbs::type::Type::enumeration_type_v0:
      return false;
    case fbs::type::Type::list_type_v0:
    case fbs::type::Type::map_type_v0:
    case fbs::type::Type::record_type_v0:
      return true;
    case fbs::type::Type::tagged_type_v0:
      die("tagged types must be resolved at this point.");
  }
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
      const auto xf = x.fields();
      const auto yf = y.fields();
      if (xf.size() != yf.size())
        return false;
      for (size_t i = 0; i < xf.size(); ++i)
        if (!congruent(xf[i].type, yf[i].type))
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
      const auto xf = x.fields();
      return std::any_of(xf.begin(), xf.end(), [&](const auto& field) {
        return field.name == y;
      });
    },
    [](const list_type&, const list&) noexcept {
      return true;
    },
    [](const map_type&, const map&) noexcept {
      return true;
    },
    [](const record_type& x, const list& y) noexcept {
      const auto xf = x.fields();
      if (xf.size() != y.size())
        return false;
      for (size_t i = 0; i < xf.size(); ++i)
        if (!congruent(xf[i].type, y[i]))
          return false;
      return true;
    },
    [](const record_type& x, const record& y) noexcept {
      const auto xf = x.fields();
      if (xf.size() != y.size())
        return false;
      for (const auto& field : xf) {
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
  die("missing case for relational operator");
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
  die("missing case for relational operator");
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
      auto tf = t.fields();
      if (u.size() != tf.size())
        return false;
      for (size_t i = 0; i < u.size(); ++i) {
        const auto field = tf[i];
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

uint8_t none_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::NONE);
}

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

uint8_t bool_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::bool_type_v0);
}

std::span<const std::byte> as_bytes(const bool_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto bool_type = fbs::type::bool_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::bool_type_v0,
                                      bool_type.Union());
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

uint8_t integer_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::integer_type_v0);
}

std::span<const std::byte> as_bytes(const integer_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto integer_type = fbs::type::integer_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::integer_type_v0,
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

uint8_t count_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::count_type_v0);
}

std::span<const std::byte> as_bytes(const count_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto count_type = fbs::type::count_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::count_type_v0,
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

uint8_t real_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::real_type_v0);
}

std::span<const std::byte> as_bytes(const real_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto real_type = fbs::type::real_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::real_type_v0,
                                      real_type.Union());
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

uint8_t duration_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::duration_type_v0);
}

std::span<const std::byte> as_bytes(const duration_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto duration_type = fbs::type::duration_type::Createv0(builder);
    const auto type = fbs::CreateType(
      builder, fbs::type::Type::duration_type_v0, duration_type.Union());
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

uint8_t time_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::time_type_v0);
}

std::span<const std::byte> as_bytes(const time_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto time_type = fbs::type::time_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::time_type_v0,
                                      time_type.Union());
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

uint8_t string_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::string_type_v0);
}

std::span<const std::byte> as_bytes(const string_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto string_type = fbs::type::string_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::string_type_v0,
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

uint8_t pattern_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::pattern_type_v0);
}

std::span<const std::byte> as_bytes(const pattern_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto pattern_type = fbs::type::pattern_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::pattern_type_v0,
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

uint8_t address_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::address_type_v0);
}

std::span<const std::byte> as_bytes(const address_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto address_type = fbs::type::address_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::address_type_v0,
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

uint8_t subnet_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::subnet_type_v0);
}

std::span<const std::byte> as_bytes(const subnet_type&) noexcept {
  static const auto buffer = []() noexcept {
    constexpr auto reserved_size = 32;
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
    const auto subnet_type = fbs::type::subnet_type::Createv0(builder);
    const auto type = fbs::CreateType(builder, fbs::type::Type::subnet_type_v0,
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

uint8_t enumeration_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::enumeration_type_v0);
}

std::span<const std::byte> as_bytes(const enumeration_type& x) noexcept {
  return as_bytes_complex(x);
}

enumeration enumeration_type::construct() const noexcept {
  const auto* fields
    = table(transparent::yes).type_as_enumeration_type_v0()->fields();
  VAST_ASSERT(fields);
  VAST_ASSERT(fields->size() > 0);
  const auto value = fields->Get(0)->key();
  // TODO: Currently, enumeration can not holds keys that don't fit a uint8_t;
  // when switching to a strong typedef for enumeration we should change that.
  // An example use case is NetFlow, where many enumeration values require
  // usage of a uint16_t, which for now we would need to model as strings in
  // schemas.
  VAST_ASSERT(value <= std::numeric_limits<enumeration>::max());
  return static_cast<enumeration>(value);
}

std::string_view enumeration_type::field(uint32_t key) const& noexcept {
  const auto* fields
    = table(transparent::yes).type_as_enumeration_type_v0()->fields();
  VAST_ASSERT(fields);
  if (const auto* field = fields->LookupByKey(key))
    return field->name()->string_view();
  return "";
}

std::vector<enumeration_type::field_view>
enumeration_type::fields() const& noexcept {
  const auto* fields
    = table(transparent::yes).type_as_enumeration_type_v0()->fields();
  VAST_ASSERT(fields);
  auto result = std::vector<field_view>{};
  result.reserve(fields->size());
  for (const auto& field : *fields)
    result.push_back({field->name()->string_view(), field->key()});
  return result;
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
  const auto list_type_offset = fbs::type::list_type::Createv0(
    builder, builder.CreateVector(
               reinterpret_cast<const uint8_t*>(value_type_bytes.data()),
               value_type_bytes.size()));
  const auto type_offset = fbs::CreateType(
    builder, fbs::type::Type::list_type_v0, list_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  VAST_ASSERT(result.size() == reserved_size);
  table_ = chunk::make(std::move(result));
}

uint8_t list_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::list_type_v0);
}

std::span<const std::byte> as_bytes(const list_type& x) noexcept {
  return as_bytes_complex(x);
}

list list_type::construct() noexcept {
  return {};
}

type list_type::value_type() const noexcept {
  const auto* view = table(transparent::yes).type_as_list_type_v0()->type();
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
  const auto map_type_offset = fbs::type::map_type::Createv0(
    builder, key_type_offset, value_type_offset);
  const auto type_offset = fbs::CreateType(
    builder, fbs::type::Type::map_type_v0, map_type_offset.Union());
  builder.Finish(type_offset);
  auto result = builder.Release();
  VAST_ASSERT(result.size() == reserved_size);
  table_ = chunk::make(std::move(result));
}

uint8_t map_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::map_type_v0);
}

std::span<const std::byte> as_bytes(const map_type& x) noexcept {
  return as_bytes_complex(x);
}

map map_type::construct() noexcept {
  return {};
}

type map_type::key_type() const noexcept {
  const auto* view = table(transparent::yes).type_as_map_type_v0()->key_type();
  VAST_ASSERT(view);
  return type{table_->slice(as_bytes(*view))};
}

type map_type::value_type() const noexcept {
  const auto* view
    = table(transparent::yes).type_as_map_type_v0()->value_type();
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

uint8_t record_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::record_type_v0);
}

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

record_type::iterable record_type::fields() const noexcept {
  return iterable{*this};
};

record_type::leaf_iterable record_type::leaves() const noexcept {
  return leaf_iterable{*this};
}

size_t record_type::num_fields() const noexcept {
  const auto* record = table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  return record->fields()->size();
}

size_t record_type::num_leaves() const noexcept {
  return flat_size(&table(transparent::yes));
}

offset record_type::resolve_flat_index(size_t flat_index) const noexcept {
  for (const auto& [_, offset] : leaves())
    if (flat_index-- == 0)
      return offset;
  die("index out of bounds");
}

std::optional<offset>
record_type::resolve_key(std::string_view key) const noexcept {
  auto do_resolve_key = [](auto&& self, const record_type& record,
                           std::string_view key) -> std::optional<offset> {
    if (key.empty())
      return {};
    auto index = static_cast<offset::size_type>(-1);
    for (auto&& field : record.fields()) {
      ++index;
      VAST_ASSERT(!field.name.empty());
      // Check whether the field name is a prefix of the key to resolve.
      auto [name_mismatch, key_mismatch] = std::mismatch(
        field.name.begin(), field.name.end(), key.begin(), key.end());
      if (name_mismatch == field.name.end()) {
        // If it's an exact match we already have our result.
        if (key_mismatch == key.end())
          return offset{index};
        // Otherwise, if the remainder begings with the . separator and the
        // nested type is a record type, we can recurse and try to match the
        // remainder.
        if (*key_mismatch++ != '.')
          continue;
        if (const auto* nested = caf::get_if<record_type>(&field.type)) {
          if (auto result
              = self(self, *nested, key.substr(key_mismatch - key.begin()))) {
            result->insert(result->begin(), index);
            return result;
          }
        }
      }
    }
    return {};
  };
  return do_resolve_key(do_resolve_key, *this, key);
}

std::vector<offset>
record_type::resolve_key_suffix(std::string_view key,
                                std::string_view prefix) const noexcept {
  auto result = std::vector<offset>{};
  // TODO: Once we support queries for nested records, we must not just iterate
  // over leafs here, but rather include nested record types.
  for (auto&& [_, offset] : leaves()) {
    const auto name = this->key(offset);
    auto [name_mismatch, key_mismatch]
      = std::mismatch(name.rbegin(), name.rend(), key.rbegin(), key.rend());
    if (key_mismatch == key.rend()) {
      if (name_mismatch == name.rend() || *name_mismatch == '.')
        result.push_back(std::move(offset));
    } else if (!prefix.empty() && *key_mismatch == '.'
               && name_mismatch == name.rend()) {
      // TODO: This handles the special case where the field name suffix
      // includes the name of the type that we're looking at even. This is done
      // for backwards compatibility reasons, as otherwise some queries would no
      // longer function. We should get rid of this long term and just have
      // these queries error.
      auto [prefix_mismatch, remaining_key_mismatch] = std::mismatch(
        prefix.rbegin(), prefix.rend(), ++key_mismatch, key.rend());
      if (remaining_key_mismatch == key.rend()) {
        if (prefix_mismatch == prefix.rend()) {
          result.push_back(std::move(offset));
        } else if (*prefix_mismatch == '.') {
          VAST_WARN("partial match '{}' against type name '{}' will be "
                    "removed in the future",
                    fmt::join(prefix_mismatch.base(), prefix.end(), ""),
                    prefix);
          result.push_back(std::move(offset));
        }
      }
    }
  }
  return result;
}

std::string_view record_type::key(size_t index) const& noexcept {
  const auto* record = table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  VAST_ASSERT(index < record->fields()->size(), "index out of bounds");
  const auto* field = record->fields()->Get(index);
  VAST_ASSERT(field);
  return field->name()->string_view();
}

std::string record_type::key(const offset& index) const noexcept {
  auto result = std::string{};
  const auto* record = table(type::transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  for (size_t i = 0; i < index.size() - 1; ++i) {
    VAST_ASSERT(index[i] < record->fields()->size());
    const auto* field = record->fields()->Get(index[i]);
    VAST_ASSERT(field);
    fmt::format_to(std::back_inserter(result), "{}.",
                   field->name()->string_view());
    record
      = resolve_transparent(field->type_nested_root())->type_as_record_type_v0();
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
  const auto* record = table(transparent::yes).type_as_record_type_v0();
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
  const auto* record = table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  for (size_t i = 0; i < index.size() - 1; ++i) {
    VAST_ASSERT(index[i] < record->fields()->size(), "index out of bounds");
    record
      = resolve_transparent(record->fields()->Get(index[i])->type_nested_root())
          ->type_as_record_type_v0();
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
  VAST_ASSERT(!index.empty(), "offset must not be empty");
  auto result = size_t{0};
  const auto* record = table(transparent::yes).type_as_record_type_v0();
  for (size_t i = 0; i < index.size(); ++i) {
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // Add the flat size of the fields up until before the record type the
    // offset tells us to recurse into.
    VAST_ASSERT(index[i] < fields->size(), "index out of bounds");
    for (size_t j = 0; j < index[i]; ++j)
      result += flat_size(fields->Get(j)->type_nested_root());
    // Recurse into the next layer, but don't count that record type itself.
    record = resolve_transparent(fields->Get(index[i])->type_nested_root())
               ->type_as_record_type_v0();
  }
  return result;
}

record_type::transformation::second_type record_type::drop() noexcept {
  return [](const field_view&) noexcept -> std::vector<struct field> {
    return {};
  };
}

record_type::transformation::second_type
record_type::assign(std::vector<struct field> fields) noexcept {
  return [fields = std::move(fields)](
           const field_view&) noexcept -> std::vector<struct field> {
    return fields;
  };
}

record_type::transformation::second_type
record_type::insert_before(std::vector<struct field> fields) noexcept {
  return [fields = std::move(fields)](const field_view& field) mutable noexcept
         -> std::vector<struct field> {
    fields.reserve(fields.size() + 1);
    fields.push_back({std::string{field.name}, field.type});
    return fields;
  };
}

record_type::transformation::second_type
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
  const auto do_transform
    = [](const auto& do_transform, const record_type& self, offset index,
         std::vector<transformation>::iterator& current,
         const std::vector<transformation>::iterator end) noexcept
    -> std::optional<record_type> {
    if (current == end)
      return self;
    auto new_fields = std::vector<struct field>{};
    auto old_fields = self.fields();
    new_fields.reserve(old_fields.size());
    index.emplace_back(old_fields.size());
    while (index.back() > 0 && current != end) {
      const auto& old_field = old_fields[--index.back()];
      // Compare the offsets of the next target with our current offset.
      auto& target = *current;
      const auto [index_mismatch, target_mismatch] = std::mismatch(
        index.begin(), index.end(), target.first.begin(), target.first.end());
      if (index_mismatch == index.end()
          && target_mismatch == target.first.end()) {
        // The offset matches exactly, so we apply the transformation.
        do {
          auto replacements
            = std::invoke(std::move(current->second), old_field);
          std::move(replacements.rbegin(), replacements.rend(),
                    std::back_inserter(new_fields));
          ++current;
        } while (current->first == index);
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
        VAST_ASSERT(index != current->first,
                    "cannot apply transformations to both a nested record type "
                    "and its children at the same time.");
      } else {
        // Check for invalid arguments on the way out.
        VAST_ASSERT(target_mismatch != target.first.end(),
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
    // In case we exited the loop earlier, we still have to add all the
    // remaining fields back to the modified record (untouched).
    while (index.back() > 0) {
      const auto& old_field = old_fields[--index.back()];
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
  // Sort transformations by offsets in reverse order.
  std::stable_sort(transformations.begin(), transformations.end(),
                   [](const auto& lhs, const auto& rhs) noexcept {
                     return lhs.first > rhs.first;
                   });
  auto current = transformations.begin();
  auto result
    = do_transform(do_transform, *this, {}, current, transformations.end());
  VAST_ASSERT(current == transformations.end(), "index out of bounds");
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
              const auto lhs_tags = lfield.type.tags();
              const auto rhs_tags = rfield.type.tags();
              const auto conflicting_tag
                = std::any_of(lhs_tags.begin(), lhs_tags.end(),
                              [&](const auto& lhs_tag) noexcept {
                                return rfield.type.tag(lhs_tag.key.data())
                                       != lhs_tag.value;
                              });
              if (conflicting_tag)
                return caf::make_error(
                  ec::logic_error,
                  fmt::format("conflicting tags ['{}'] and ['{}'] for "
                              "field {}; failed to merge {} and {}",
                              fmt::join(lhs_tags, "', '"),
                              fmt::join(rhs_tags, "', '"), rfield.name, lhs,
                              rhs));
              auto tags = std::vector<struct type::tag>{};
              tags.reserve(lhs_tags.size() + rhs_tags.size());
              for (const auto& tag : lhs_tags)
                tags.push_back({std::string{tag.key},
                                std::optional{std::string{tag.value}}});
              for (const auto& tag : rhs_tags)
                tags.push_back({std::string{tag.key},
                                std::optional{std::string{tag.value}}});
              return type{lfield.type.name(), lfield.type, tags};
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
        die("unhandled merge conflict case");
      },
    };
  };
  auto transformations = std::vector<record_type::transformation>{};
  auto additions = std::vector<struct record_type::field>{};
  auto rfields = rhs.fields();
  transformations.reserve(rfields.size());
  auto err = caf::error{};
  for (auto rfield : rfields) {
    if (const auto& lindex = lhs.resolve_key(rfield.name)) {
      transformations.emplace_back(
        *lindex,
        [&, rfield = std::move(rfield)](
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
        });
    } else {
      additions.push_back({std::string{rfield.name}, std::move(rfield.type)});
    }
  }
  auto result = lhs.transform(std::move(transformations));
  if (err)
    return err;
  VAST_ASSERT(result);
  result = result->transform({{
    {result->fields().size() - 1},
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

/// Access a field by index.
[[nodiscard]] record_type::field_view
record_type::iterable::operator[](size_t index) const noexcept {
  const auto* record = type_.table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  const auto* field = record->fields()->Get(index);
  VAST_ASSERT(field);
  return {
    field->name()->string_view(),
    type{type_.table_->slice(as_bytes(*field->type()))},
  };
}

/// Get the number of fields in the record field.
size_t record_type::iterable::size() const noexcept {
  const auto* record = type_.table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  return record->fields()->size();
}

record_type::iterable::iterable(record_type type) noexcept
  : index_{0}, type_{std::move(type)} {
  // nop
}

void record_type::iterable::next() noexcept {
  ++index_;
}

bool record_type::iterable::done() const noexcept {
  const auto* record = type_.table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  return index_ >= record->fields()->size();
}

record_type::field_view record_type::iterable::get() const noexcept {
  return (*this)[index_];
}

record_type::leaf_iterable::leaf_iterable(record_type type) noexcept
  : index_(), type_{std::move(type)} {
  // Set the index of the first leaf.
  const auto* record = type_.table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  do {
    index_.push_back(0);
    record = resolve_transparent(record->fields()->begin()->type_nested_root())
               ->type_as_record_type_v0();
  } while (record != nullptr);
}

void record_type::leaf_iterable::next() noexcept {
  if (index_.empty())
    return;
  const auto* view = &type_.table(transparent::yes);
  VAST_ASSERT(view);
  auto records = std::vector{view->type_as_record_type_v0()};
  VAST_ASSERT(records.back());
  // Resolve everything but the last index.
  VAST_ASSERT(!index_.empty());
  for (size_t i = 0; i < index_.size() - 1; ++i) {
    VAST_ASSERT(index_[i] < records.back()->fields()->size());
    view = resolve_transparent(
      records.back()->fields()->Get(index_[i])->type_nested_root());
    VAST_ASSERT(view);
    records.push_back(view->type_as_record_type_v0());
    VAST_ASSERT(records.back());
  }
  // Increment the last index, and step out of nestec records until we're back
  // in a record that we have not iterated over completely.
  while (++index_.back() == records.back()->fields()->size()) {
    index_.pop_back();
    if (index_.empty())
      return;
    records.pop_back();
  }
  // Find the next valid offset by going to the next field, and recursively
  // stepping into it until we've arrived at a leaf field.
  while (true) {
    VAST_ASSERT(index_.back() < records.back()->fields()->size());
    view = resolve_transparent(
      records.back()->fields()->Get(index_.back())->type_nested_root());
    VAST_ASSERT(view);
    switch (view->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type_v0:
      case fbs::type::Type::integer_type_v0:
      case fbs::type::Type::count_type_v0:
      case fbs::type::Type::real_type_v0:
      case fbs::type::Type::duration_type_v0:
      case fbs::type::Type::time_type_v0:
      case fbs::type::Type::string_type_v0:
      case fbs::type::Type::pattern_type_v0:
      case fbs::type::Type::address_type_v0:
      case fbs::type::Type::subnet_type_v0:
      case fbs::type::Type::enumeration_type_v0:
      case fbs::type::Type::list_type_v0:
      case fbs::type::Type::map_type_v0:
        return;
      case fbs::type::Type::record_type_v0:
        records.push_back(view->type_as_record_type_v0());
        VAST_ASSERT(records.back());
        index_.push_back(0);
        break;
      case fbs::type::Type::tagged_type_v0:
        die("tagged types must be resolved at this point.");
        break;
    }
  }
}

bool record_type::leaf_iterable::done() const noexcept {
  return index_.empty();
}

std::pair<record_type::field_view, offset>
record_type::leaf_iterable::get() const noexcept {
  const auto* record = type_.table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  // Resolve everything but the last layer.
  VAST_ASSERT(!index_.empty());
  for (size_t i = 0; i < index_.size() - 1; ++i) {
    VAST_ASSERT(index_[i] < record->fields()->size());
    record = resolve_transparent(
               record->fields()->Get(index_[i])->type_nested_root())
               ->type_as_record_type_v0();
    VAST_ASSERT(record);
  }
  // Resolve the last layer.
  VAST_ASSERT(index_.back() < record->fields()->size());
  const auto* field = record->fields()->Get(index_.back());
  VAST_ASSERT(field);
  VAST_ASSERT(
    !resolve_transparent(field->type_nested_root())->type_as_record_type_v0(),
    "leaf field must not be a record type");
  return {
    {
      field->name()->string_view(),
      type{type_.table_->slice(as_bytes(*field->type()))},
    },
    index_,
  };
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
    (static_cast<void>(tbl[Ts::type_index()] = Indices), ...);
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
