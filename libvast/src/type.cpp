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

template <class T>
  requires(std::is_same_v<T, struct record_type::field>  //
           || std::is_same_v<T, record_type::field_view> //
           || std::is_same_v<T, record_field>)
void construct_record_type(type& self, const T* begin, const T* end) {
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

} // namespace

// -- type --------------------------------------------------------------------

type::type() noexcept = default;

type::type(const type& other) noexcept = default;

type& type::operator=(const type& rhs) noexcept = default;

type::type(type&& other) noexcept = default;

type& type::operator=(type&& other) noexcept = default;

type::~type() noexcept = default;

type::type(chunk_ptr&& table) noexcept : table_{std::move(table)} {
#if VAST_ENABLE_ASSERTIONS
  VAST_ASSERT(table_);
  VAST_ASSERT(table_->size() > 0);
  const auto* const data = reinterpret_cast<const uint8_t*>(table_->data());
  auto verifier = flatbuffers::Verifier{data, table_->size()};
  VAST_ASSERT(fbs::GetType(data)->Verify(verifier),
              "Encountered invalid vast.fbs.Type FlatBuffers table.");
#  if defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
  VAST_ASSERT(verifier.GetComputedSize() == table_->size(),
              "Encountered unexpected excess bytes in vast.fbs.Type "
              "FlatBuffers table.");
#  endif // defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
#endif   // VAST_ENABLE_ASSERTIONS
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

type::type(const legacy_type& other) noexcept {
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
      *this = type{other.name(), none_type{}, tags};
    },
    [&](const legacy_bool_type&) {
      *this = type{other.name(), bool_type{}, tags};
    },
    [&](const legacy_integer_type&) {
      *this = type{other.name(), integer_type{}, tags};
    },
    [&](const legacy_count_type&) {
      *this = type{other.name(), count_type{}, tags};
    },
    [&](const legacy_real_type&) {
      *this = type{other.name(), real_type{}, tags};
    },
    [&](const legacy_duration_type&) {
      *this = type{other.name(), duration_type{}, tags};
    },
    [&](const legacy_time_type&) {
      *this = type{other.name(), time_type{}, tags};
    },
    [&](const legacy_string_type&) {
      *this = type{other.name(), string_type{}, tags};
    },
    [&](const legacy_pattern_type&) {
      *this = type{other.name(), pattern_type{}, tags};
    },
    [&](const legacy_address_type&) {
      *this = type{other.name(), address_type{}, tags};
    },
    [&](const legacy_subnet_type&) {
      *this = type{other.name(), subnet_type{}, tags};
    },
    [&](const legacy_enumeration_type& enumeration) {
      auto fields = std::vector<struct enumeration_type::field>{};
      fields.reserve(enumeration.fields.size());
      for (const auto& field : enumeration.fields)
        fields.push_back({field});
      *this = type{other.name(), enumeration_type{fields}, tags};
    },
    [&](const legacy_list_type& list) {
      *this = type{other.name(), list_type{type{list.value_type}}, tags};
    },
    [&](const legacy_map_type& list) {
      *this = type{other.name(),
                   map_type{type{list.key_type}, type{list.value_type}}, tags};
    },
    [&](const legacy_alias_type& alias) {
      return type{other.name(), type{alias.value_type}, tags};
    },
    [&](const legacy_record_type& record) {
      auto fields = std::vector<struct record_type::field_view>{};
      fields.reserve(record.fields.size());
      for (const auto& field : record.fields)
        fields.push_back({field.name, type{field.type}});
      *this = type{other.name(), record_type{fields}, tags};
    },
  };
  caf::visit(f, other);
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

const fbs::Type& type::table(enum transparent transparent) const noexcept {
  const auto& repr = as_bytes(*this);
  return *resolve_transparent(fbs::GetType(repr.data()), transparent);
}

uint8_t type::type_index() const noexcept {
  static_assert(
    std::is_same_v<uint8_t, std::underlying_type_t<vast::fbs::type::Type>>);
  return static_cast<uint8_t>(table(transparent::yes).type_type());
}

std::span<const std::byte> as_bytes(const type& x) noexcept {
  return x.table_ ? as_bytes(*x.table_) : none_type_representation();
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

type flatten(const type& type) noexcept {
  if (const auto* record = caf::get_if<record_type>(&type))
    return flatten(*record);
  return type;
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
  const std::vector<struct field>& fields) noexcept {
  VAST_ASSERT(!fields.empty(), "An enumeration type must not have zero fields");
  // Unlike for other concrete types, we do not calculate the exact amount of
  // bytes we need to allocate beforehand. This is because the individual
  // fields are stored in a flat hash map, whose size cannot trivially be
  // determined.
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto field_offsets
    = std::vector<flatbuffers::Offset<fbs::type::enumeration_type::field::v0>>{};
  field_offsets.reserve(fields.size());
  for (uint32_t next_key = 0; const auto& field : fields) {
    const auto key = field.key ? *field.key : next_key;
    next_key = key + 1;
    const auto name_offset = builder.CreateString(field.name);
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
  static_cast<type&>(*this) = type{std::move(chunk)};
}

uint8_t enumeration_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::enumeration_type_v0);
}

std::span<const std::byte> as_bytes(const enumeration_type& x) noexcept {
  return as_bytes(static_cast<const type&>(x));
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
  auto chunk = chunk::make(std::move(result));
  static_cast<type&>(*this) = type{std::move(chunk)};
}

uint8_t list_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::list_type_v0);
}

std::span<const std::byte> as_bytes(const list_type& x) noexcept {
  return as_bytes(static_cast<const type&>(x));
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
  auto chunk = chunk::make(std::move(result));
  static_cast<type&>(*this) = type{std::move(chunk)};
}

uint8_t map_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::map_type_v0);
}

std::span<const std::byte> as_bytes(const map_type& x) noexcept {
  return as_bytes(static_cast<const type&>(x));
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

record_type::record_type(const std::vector<struct field_view>& fields) noexcept {
  construct_record_type(*this, fields.data(), fields.data() + fields.size());
}

record_type::record_type(
  std::initializer_list<struct field_view> fields) noexcept
  : record_type{std::vector<struct field_view>{fields}} {
  // nop
}

record_type::record_type(const std::vector<struct field>& fields) noexcept {
  construct_record_type(*this, fields.data(), fields.data() + fields.size());
}

record_type::iterable record_type::fields() const noexcept {
  return iterable{*this};
};

record_type::leaf_iterable record_type::leaves() const noexcept {
  return leaf_iterable{*this};
}

record_type::field_view record_type::field(size_t index) const noexcept {
  const auto* record = table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  VAST_ASSERT(index < record->fields()->size(), "index out of bounds");
  const auto* field = record->fields()->Get(index);
  VAST_ASSERT(field);
  return {
    field->name()->string_view(),
    type{table_->slice(as_bytes(*field->type()))},
  };
}

record_type::field_view record_type::field(offset index) const noexcept {
  VAST_ASSERT(!index.empty(), "offset must not be empty");
  const auto* record = table(transparent::yes).type_as_record_type_v0();
  VAST_ASSERT(record);
  for (size_t i = 0; i < index.size() - 1; ++i) {
    VAST_ASSERT(index[i] < record->fields()->size(), "index out of bounds");
    record = record->fields()
               ->Get(index[i])
               ->type_nested_root()
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

record_type flatten(const record_type& type) noexcept {
  auto fields = std::vector<record_type::field_view>{};
  for (const auto& [field, _] : type.leaves())
    fields.push_back(field);
  return record_type{fields};
}

uint8_t record_type::type_index() noexcept {
  return static_cast<uint8_t>(fbs::type::Type::record_type_v0);
}

std::span<const std::byte> as_bytes(const record_type& x) noexcept {
  return as_bytes(static_cast<const type&>(x));
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
  index_.push_back(0);
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
  VAST_ASSERT(!field->type_nested_root()->type_as_record_type_v0());
  return {
    {
      field->name()->string_view(),
      type{type_.table_->slice(as_bytes(*field->type()))},
    },
    index_,
  };
}

} // namespace vast
