//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/type.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/fbs/type.hpp"
#include "vast/legacy_type.hpp"

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
      // By default the builder allocates 1024 bytes, which is much more than
      // what we require, and since we can easily calculate the exact amount we
      // should do that. The total length is made up from the following terms:
      // - 52 bytes FlatBuffers table framing
      // - Nested type FlatBuffers table size
      // - All contained string lengths, rounded up to four each
      // - 12 bytes if tags exist
      // - 16 bytes for every tag
      // - 16 bytes for every tag with a value
      size_t size = 52;
      size += nested_bytes.size();
      size += reserved_string_size(name);
      if (!tags.empty()) {
        size += 12;
        for (const auto& tag : tags) {
          size += 16;
          size += reserved_string_size(tag.key);
          if (tag.value) {
            size += 16;
            size += reserved_string_size(*tag.value);
          }
        }
      }
      return size;
    }();
    auto builder = flatbuffers::FlatBufferBuilder{reserved_size};
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
    VAST_ASSERT(result.size() == reserved_size);
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
      result.push_back({attribute.key, attribute.value
                                         ? *attribute.value
                                         : std::optional<std::string>{}});
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
      *this = type{other.name(), type{list_type{type{list.value_type}}}, tags};
    },
    [&](const legacy_map_type& list) {
      *this = type{other.name(),
                   type{map_type{type{list.key_type}, type{list.value_type}}},
                   tags};
    },
    [&](const legacy_alias_type& alias) {
      return type{other.name(), type{alias.value_type}, tags};
    },
    [&](const auto&) {
      // TODO: Implement for all legacy types, then remove this handler.
      // - legacy_record_type,
    },
  };
  caf::visit(f, other);
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

std::string enumeration_type::signature() const noexcept {
  const auto* fields
    = table(transparent::yes).type_as_enumeration_type_v0()->fields();
  VAST_ASSERT(fields);
  auto result = std::string{};
  auto it = fields->begin();
  VAST_ASSERT(it != fields->end());
  fmt::format_to(std::back_inserter(result), "enum {{{}: {}",
                 (*it)->name()->string_view(), (*it)->key());
  while (++it != fields->end())
    fmt::format_to(std::back_inserter(result), ", {}: {}",
                   (*it)->name()->string_view(), (*it)->key());
  fmt::format_to(std::back_inserter(result), "}}");
  return result;
}

std::string_view enumeration_type::field(uint32_t key) const& noexcept {
  const auto* fields
    = table(transparent::yes).type_as_enumeration_type_v0()->fields();
  VAST_ASSERT(fields);
  if (const auto* field = fields->LookupByKey(key))
    return field->name()->string_view();
  return "";
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

std::string list_type::signature() const noexcept {
  return fmt::format("list<{}>", value_type());
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

std::string map_type::signature() const noexcept {
  return fmt::format("map<{}, {}>", key_type(), value_type());
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
} // namespace vast
