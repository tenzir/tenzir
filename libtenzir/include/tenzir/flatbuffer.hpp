//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/chunk.hpp"

#include <flatbuffers/flatbuffers.h>
#include <fmt/core.h>

#include <compare>
#include <string_view>

namespace tenzir {

/// Determines whether the FlatBuffers table is a root or a child table, i.e.,
/// whether the only data contained in the owned chunk is the table itself
/// (root) or other the table is just part of a bigger root table (child).
enum class flatbuffer_type {
  size_prefixed, ///< The table type is a root type with a size prefix.
  root,  ///< The table type is a root type, or a nested FlatBuffers table.
  child, ///< The table is sliced from a root table.
};

/// A utility function for determining the size prefixed buffer length of a
/// FlatBuffers table.
/// @pre chunk != nullptr
/// @pre chunk->size() >= sizeof(flatbuffers::uoffset_t)
inline auto size_prefixed_flatbuffer_size(const chunk_ptr& chunk) {
  TENZIR_ASSERT(chunk);
  TENZIR_ASSERT(chunk->size() >= sizeof(flatbuffers::uoffset_t));
  // The version of FlatBuffers we're using in the Dockerfile doesn't have
  // GetSizePrefixedBufferLength, so we take GetPrefixedSize and add the buffer
  // length on top manually.
  return sizeof(::flatbuffers::uoffset_t)
         + flatbuffers::GetPrefixedSize(
           reinterpret_cast<const uint8_t*>(chunk->data()));
}

/// A function returning a FlatBuffers table identifier.
/// NOTE: Unfortunately, for a given FlatBuffers table *Foo* there is no
/// built-in mechanism to get *FooIdentifier* even when enabling the static
/// reflection option of the flatc compiler, so users of the API must pass it in
/// manually.
using flatbuffer_identifier = auto() -> const char*;

/// A wrapper class around a FlatBuffers table that allows for sharing the
/// lifetime with the chunk containing the table.
/// @tparam Table The generated FlatBuffers table type.
/// @tparam Identifier The FlatBuffers table's flle identifier function.
/// @tparam Type Determines whether the table is a root or a child table.
template <class Table, flatbuffer_identifier Identifier = nullptr,
          flatbuffer_type Type = flatbuffer_type::root>
class flatbuffer final {
public:
  // -- member types and constants --------------------------------------------

  static_assert(Type != flatbuffer_type::child or Identifier == nullptr,
                "child FlatBuffers tables must not have a buffer identifier");

  template <class ParentTable, flatbuffer_identifier ParentIdentifier,
            flatbuffer_type ParentType>
  friend class flatbuffer;

  friend struct ::fmt::formatter<flatbuffer>;

  // -- constructors, destructors, and assignment operators -------------------

  /// Constructs a ref-counted FlatBuffers root table that shares the
  /// lifetime with the chunk it's constructed from.
  /// @pre *chunk* must hold a valid *Table*.
  [[nodiscard]] static auto make_unsafe(chunk_ptr&& chunk) noexcept
    -> caf::expected<flatbuffer>
    requires(Type != flatbuffer_type::child)
  {
    // FlatBuffers does not correctly use '::flatbuffers::{s,u}offset_t' over
    // '{s,u}offset_t' in `FLATBUFFERS_{MIN,MAX}_BUFFER_SIZE`.
    using flatbuffers::soffset_t, flatbuffers::uoffset_t;
    if (not chunk) {
      return caf::make_error(ec::logic_error, fmt::format("failed to read {} "
                                                          "from a nullptr",
                                                          qualified_name()));
    }
    if (chunk->size() < FLATBUFFERS_MIN_BUFFER_SIZE) {
      return caf::make_error(
        ec::format_error,
        fmt::format("failed to read {} because its size {} "
                    "is below the minimum required size of {}",
                    qualified_name(), chunk->size(),
                    FLATBUFFERS_MIN_BUFFER_SIZE));
    }
    if constexpr (Type == flatbuffer_type::size_prefixed) {
      const auto expected_size = size_prefixed_flatbuffer_size(chunk);
      if (chunk->size() != expected_size) {
        return caf::make_error(
          ec::logic_error,
          fmt::format("failed to read {} from a chunk of length {} with a size "
                      "prefixed buffer length of {}",
                      qualified_name(), chunk->size(), expected_size));
      }
    }
    if constexpr (Identifier != nullptr) {
      if (not flatbuffers::BufferHasIdentifier(
            chunk->data(), Identifier(),
            Type == flatbuffer_type::size_prefixed)) {
        return caf::make_error(ec::format_error,
                               fmt::format("failed to read {} because its "
                                           "buffer identifier is wrong or "
                                           "missing",
                                           qualified_name()));
      }
    }
    if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE) {
      return caf::make_error(
        ec::format_error, fmt::format("failed to read {} because its size {} "
                                      "exceeds the maximum allowed size of {}",
                                      qualified_name(), chunk->size(),
                                      FLATBUFFERS_MAX_BUFFER_SIZE));
    }
    return flatbuffer{chunk};
  }

  /// Constructs a ref-counted FlatBuffers root table that shares the
  /// lifetime with the chunk it's constructed from.
  /// @note This verifies the FlatBuffers table recursively, potentially
  /// loading memory in the chunk, which can be expensive. Use `make_unsafe`
  /// instead to skip this verification.
  /// @pre *chunk* must hold a valid *Table*.
  [[nodiscard]] static auto make(chunk_ptr&& chunk) noexcept
    -> caf::expected<flatbuffer>
    requires(Type != flatbuffer_type::child)
  {
    auto result = make_unsafe(std::move(chunk));
    if (not result) {
      return std::move(result.error());
    }
    // FlatBuffers defaults to erroring out after 1M table entries in the
    // verifier. This was chosen rather randomly and for historic reasons, they
    // cannot change it. We use the much saner default of not erroring out for
    // large tables here.
    auto options = flatbuffers::Verifier::Options{};
    options.max_tables = std::numeric_limits<flatbuffers::uoffset_t>::max();
    const auto* const data
      = reinterpret_cast<const uint8_t*>(result->chunk()->data());
    auto verifier
      = flatbuffers::Verifier{data, result->chunk()->size(), options};
    if (not result->root()->Verify(verifier)) {
      return caf::make_error(ec::format_error,
                             fmt::format("failed to read {} because its "
                                         "verification failed",
                                         qualified_name()));
    }
#if defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
    TENZIR_ASSERT(verifier.GetComputedSize() >= result->chunk()->size());
    if (verifier.GetComputedSize() > result->chunk()->size()) {
      return caf::make_error(
        ec::format_error,
        fmt::format("failed to read {} because of {} unexpected excess bytes",
                    qualified_name(),
                    verifier.GetComputedSize() - result.chunk()->size()));
    }
#endif // defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
    return std::move(*result);
  }

  /// Constructs a ref-counted FlatBuffers root table.
  /// @pre *buffer* must hold a valid *Table*.
  [[nodiscard]] static auto make(flatbuffers::DetachedBuffer&& buffer) noexcept
    -> caf::expected<flatbuffer>
    requires(Type != flatbuffer_type::child)
  {
    return make_unsafe(chunk::make(std::move(buffer)));
  }

  /// Default-constructs a FlatBuffers table.
  flatbuffer() noexcept = default;

  /// Constructs a ref-counted FlatBuffers root table from a FlatBufferBuilder
  /// by finishing it.
  flatbuffer(flatbuffers::FlatBufferBuilder& builder,
             flatbuffers::Offset<Table> offset) noexcept
    requires(Type != flatbuffer_type::child)
  {
    if constexpr (Type == flatbuffer_type::root) {
      builder.Finish(offset, Identifier != nullptr ? Identifier() : nullptr);
    } else if constexpr (Type == flatbuffer_type::size_prefixed) {
      builder.FinishSizePrefixed(offset, Identifier != nullptr ? Identifier()
                                                               : nullptr);
    } else {
      static_assert(detail::always_false_v<decltype(Type)>,
                    "unhandled FlatBuffers Type");
    }
    auto chunk = chunk::make(builder.Release());
    TENZIR_ASSERT(chunk);
    *this = flatbuffer{chunk};
  }

  /// Constructs a ref-counted FlatBuffers table that shares the
  /// lifetime with another FlatBuffer pointer.
  /// @pre `parent`
  /// @pre *table* must be accessible from *parent*.
  template <class ParentTable, flatbuffer_identifier ParentIdentifier,
            flatbuffer_type ParentType>
  flatbuffer(flatbuffer<ParentTable, ParentIdentifier, ParentType> parent,
             const Table& table) noexcept
    requires(Type == flatbuffer_type::child)
    : chunk_{std::exchange(parent.chunk_, {})}, table_{&table} {
    TENZIR_ASSERT(chunk_);
    TENZIR_ASSERT(reinterpret_cast<const std::byte*>(table_) >= chunk_->data());
    TENZIR_ASSERT(reinterpret_cast<const std::byte*>(table_)
                  < (chunk_->data() + chunk_->size()));
  }

  ~flatbuffer() noexcept = default;

  flatbuffer(const flatbuffer& other) noexcept
    : chunk_{other.chunk_}, table_{other.table_} {
    // nop
  }

  flatbuffer(flatbuffer&& other) noexcept
    : chunk_{std::exchange(other.chunk_, {})},
      table_{std::exchange(other.table_, {})} {
    // nop
  }

  auto operator=(const flatbuffer& rhs) noexcept -> flatbuffer& {
    if (&rhs == this)
      return *this;
    chunk_ = rhs.chunk_;
    table_ = rhs.table_;
    return *this;
  }

  auto operator=(flatbuffer&& rhs) noexcept -> flatbuffer& {
    chunk_ = std::exchange(rhs.chunk_, {});
    table_ = std::exchange(rhs.table_, {});
    return *this;
  }

  [[nodiscard]] auto as_child() const
    -> flatbuffer<Table, Identifier, flatbuffer_type::child>
    requires(Type != flatbuffer_type::child)
  {
    return this->slice(*this->root());
  }

  // -- operators -------------------------------------------------------------

  explicit operator bool() const noexcept {
    return table_ != nullptr;
  }

  auto operator*() const noexcept -> const Table& {
    TENZIR_ASSERT(table_);
    return *table_;
  }

  auto operator->() const noexcept -> const Table* {
    TENZIR_ASSERT(table_);
    return table_;
  }

  // -- accessors -------------------------------------------------------------

  /// Slices a nested FlatBuffers table pointer with shared lifetime.
  /// @note The child table will not be a root pointer; consider using the
  /// two-argument overload the if sliced table is a nested FlatBuffers root
  /// table and operations that require root tables must be supported.
  /// @pre `*this != nullptr`
  template <class ChildTable>
  [[nodiscard]] auto slice(const ChildTable& child_table) const noexcept
    -> flatbuffer<ChildTable, nullptr, flatbuffer_type::child> {
    TENZIR_ASSERT(*this);
    return {*this, child_table};
  }

  /// Slices a nested FlatBuffers root table pointer with shared lifetime.
  /// @pre `*this != nullptr`
  /// @pre *child_table* and *nested_flatbuffer* must point to the same nested
  /// FlatBuffers table, i.e., `*(*this)->child_nested_root()` and
  /// `*(*this)->child()` respectively.
  template <flatbuffer_identifier ChildIdentifier = nullptr, class ChildTable>
  [[nodiscard]] auto
  slice(const ChildTable& child_table,
        const flatbuffers::Vector<uint8_t>& nested_flatbuffer) const noexcept
    -> flatbuffer<ChildTable, ChildIdentifier, flatbuffer_type::root> {
    TENZIR_ASSERT(*this);
    TENZIR_ASSERT(
      &child_table
      == flatbuffers::GetRoot<ChildTable>(nested_flatbuffer.data()));
    return {chunk_->slice(as_bytes(nested_flatbuffer))};
  }

  /// Accesses the underlying chunk.
  /// @note The returned chunk may contain more than just the FlatBuffers table
  /// if it is not a root table.
  [[nodiscard]] auto chunk() const& noexcept -> const chunk_ptr& {
    return chunk_;
  }

  /// Accesses the underlying chunk.
  /// @note The returned chunk may contain more than just the FlatBuffers table
  /// if it is not a root table.
  [[nodiscard]] auto chunk() && noexcept -> chunk_ptr {
    return std::move(chunk_);
  }

  // -- concepts --------------------------------------------------------------

  TENZIR_CONSTEVAL static auto qualified_name() noexcept -> std::string_view {
    return std::string_view{Table::GetFullyQualifiedName()};
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, flatbuffer& x) -> bool {
    // When serializing, we decompose the FlatBuffers table into the chunk it
    // lives in and the offset of the table pointer inside it, and when
    // deserializing we put it all back together.
    auto table_offset = x.chunk_ ? reinterpret_cast<const std::byte*>(x.table_)
                                     - x.chunk_->data()
                                 : 0;
    auto load_callback = [&]() noexcept {
      if (x.chunk_)
        x.table_
          = reinterpret_cast<const Table*>(x.chunk_->data() + table_offset);
      return true;
    };
    const auto name = qualified_name();
    return f.object(x)
      .pretty_name(std::string_view{name.data(), name.size()})
      .on_load(load_callback)
      .fields(f.field("chunk", x.chunk_),
              f.field("table-offset", table_offset));
  }

  friend auto inspect(caf::detail::stringification_inspector& f, flatbuffer& x)
    -> bool {
    auto str = fmt::to_string(x);
    return f(str);
  }

private:
  // -- implementation details ------------------------------------------------

  /// Constructs a ref-counted FlatBuffers root table that shares the
  /// lifetime with the chunk it's constructed from.
  /// @pre *chunk* must hold a valid *Table*.
  flatbuffer(chunk_ptr chunk) noexcept
    requires(Type != flatbuffer_type::child)
    : chunk_{std::move(chunk)}, table_{root()} {
    // nop
  }

  /// Returns a pointer into the underlying FlatBuffers tbale.
  /// @pre *chunk* must hold a valid *Table*.
  auto root() const noexcept -> const Table* {
    const auto* const data = reinterpret_cast<const uint8_t*>(chunk_->data());
    if constexpr (Type == flatbuffer_type::root) {
      return flatbuffers::GetRoot<Table>(data);
    } else if constexpr (Type == flatbuffer_type::size_prefixed) {
      return flatbuffers::GetSizePrefixedRoot<Table>(data);
    } else {
      static_assert(detail::always_false_v<decltype(Type)>,
                    "unhandled FlatBuffers Type");
    }
  }

  /// A pointer to the underlying chunk. For root tables, the beginning of the
  /// contained data starts with the root table directly. This is used for
  /// sharing the lifetime of the flatbuffer with the chunk as well.
  chunk_ptr chunk_ = {};

  /// A pointer to the table that this FlatBuffers table pointer wraps.
  const Table* table_ = {};
};

/// A convenience alias for size prefixed FlatBuffers tables.
template <class Table, flatbuffer_identifier Identifier = nullptr>
using size_prefixed_flatbuffer
  = flatbuffer<Table, Identifier, flatbuffer_type::size_prefixed>;

/// A convenience alias for child FlatBuffers tables.
template <class Table>
using child_flatbuffer = flatbuffer<Table, nullptr, flatbuffer_type::child>;

// -- deduction guides --------------------------------------------------------

template <class Table, class ParentTable,
          flatbuffer_identifier ParentIdentifier, flatbuffer_type ParentType>
flatbuffer(flatbuffer<ParentTable, ParentIdentifier, ParentType>, const Table*)
  -> flatbuffer<Table, nullptr, flatbuffer_type::child>;

template <class Table, flatbuffer_identifier ParentIdentifier,
          flatbuffer_type ParentType>
auto as_bytes(const flatbuffer<Table, ParentIdentifier, ParentType> fb)
  -> std::span<const std::byte> {
  return {fb.chunk()->data(), fb.chunk()->size()};
}

} // namespace tenzir

// -- formatter ---------------------------------------------------------------

template <class Table, tenzir::flatbuffer_identifier Identifier,
          tenzir::flatbuffer_type Type>
struct fmt::formatter<tenzir::flatbuffer<Table, Identifier, Type>> {
  template <class ParseContext>
  auto parse(const ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::flatbuffer<Table, Identifier, Type>& flatbuffer,
              FormatContext& ctx) -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}({})", flatbuffer.qualified_name(),
                          fmt::ptr(flatbuffer.table_));
  }
};
