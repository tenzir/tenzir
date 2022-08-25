//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"

#include <flatbuffers/flatbuffers.h>
#include <fmt/core.h>

#include <compare>

namespace vast {

/// Determines whether the FlatBuffers table is a root or a child table, i.e.,
/// whether the only data contained in the owned chunk is the table itself
/// (root) or other the table is just part of a bigger root table (child).
enum class flatbuffer_type {
  root,  ///< The table type is a root type, or a nested FlatBuffers table.
  child, ///< The table is sliced from a root table.
};

/// A wrapper class around a FlatBuffers table that allows for sharing the
/// lifetime with the chunk containing the table.
/// @tparam Table The generated FlatBuffers table type.
/// @tparam Type Determines whether the table is a root or a child table.
template <class Table, flatbuffer_type Type = flatbuffer_type::root>
class flatbuffer final {
public:
  // -- member types and constants --------------------------------------------

  template <class ParentTable, flatbuffer_type ParentType>
  friend class flatbuffer;

  friend struct ::fmt::formatter<flatbuffer>;

  /// Indicates whether to verify the FlatBuffers table on construction.
  enum class verify {
    yes, ///< Perform extensive buffer verification.
    no,  ///< Skip extensive buffer verification.
  };

#if VAST_ENABLE_ASSERTIONS
  static constexpr auto verify_default = verify::yes;
#else
  static constexpr auto verify_default = verify::no;
#endif // VAST_ENABLE_ASSERTIONS

  // -- constructors, destructors, and assignment operators -------------------

  /// Constructs a ref-counted FlatBuffers root table that shares the
  /// lifetime with the chunk it's constructed from.
  /// @pre *chunk* must hold a valid *Table*.
  [[nodiscard]] static caf::expected<flatbuffer>
  make(chunk_ptr&& chunk, enum verify verify = verify_default) noexcept
    requires(Type == flatbuffer_type::root)
  {
    if (!chunk)
      return caf::make_error(ec::logic_error,
                             fmt::format("failed to read {} from a nullptr",
                                         qualified_name()));
    if (chunk->size() == 0)
      return caf::make_error(
        ec::logic_error, fmt::format("failed to read {} from an empty chunk",
                                     qualified_name()));
    if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE)
      return caf::make_error(
        ec::format_error,
        fmt::format("failed to read {} because its size {} "
                    "exceeds the maximum allowed size of {}",
                    qualified_name(), chunk->size(),
                    FLATBUFFERS_MAX_BUFFER_SIZE));
    if (verify == verify::yes) {
      const auto* const data = reinterpret_cast<const uint8_t*>(chunk->data());
      auto verifier = flatbuffers::Verifier{data, chunk->size()};
      if (!flatbuffers::GetRoot<Table>(data)->Verify(verifier))
        return caf::make_error(ec::format_error,
                               fmt::format("failed to read {} because its "
                                           "verification failed",
                                           qualified_name()));
#if defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
      VAST_ASSERT(verifier.GetComputedSize() >= chunk->size());
      if (verifier.GetComputedSize() > chunk->size())
        return caf::make_error(
          ec::format_error,
          fmt::format("failed to read {} because of {} unexpected excess bytes",
                      qualified_name(),
                      verifier.GetComputedSize() - chunk->size()));
#endif // defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
    }
    return flatbuffer{chunk};
  }

  /// Constructs a ref-counted FlatBuffers root table.
  /// @pre *buffer* must hold a valid *Table*.
  [[nodiscard]] static caf::expected<flatbuffer>
  make(flatbuffers::DetachedBuffer&& buffer, enum verify verify
                                             = verify_default) noexcept {
    return make(chunk::make(std::move(buffer)), verify);
  }

  /// Default-constructs a FlatBuffers table.
  flatbuffer() noexcept = default;

  /// Constructs a ref-counted FlatBuffers root table from a FlatBufferBuilder
  /// by finishing it.
  // TODO: FlatBuffers 2.0's reflection allows for inferring the
  // *file_identifier* from *Table*, which makes this API a lot easier to use.
  // When upgrading to that version from 1.12 we can enable reflection when
  // compiling the schemas.
  flatbuffer(flatbuffers::FlatBufferBuilder& builder,
             flatbuffers::Offset<Table> offset,
             const char* file_identifier) noexcept
    requires(Type == flatbuffer_type::root)
  {
    builder.Finish(offset, file_identifier);
    auto chunk = chunk::make(builder.Release());
    VAST_ASSERT(chunk);
    *this = flatbuffer{chunk};
  }

  /// Constructs a ref-counted FlatBuffers table that shares the
  /// lifetime with another FlatBuffer pointer.
  /// @pre `parent`
  /// @pre *table* must be accessible from *parent*.
  template <class ParentTable, flatbuffer_type ParentType>
  flatbuffer(flatbuffer<ParentTable, ParentType> parent,
             const Table& table) noexcept
    requires(Type == flatbuffer_type::child)
    : chunk_{std::exchange(parent.chunk_, {})}, table_{&table} {
    VAST_ASSERT(chunk_);
    VAST_ASSERT(reinterpret_cast<const std::byte*>(table_) >= chunk_->data());
    VAST_ASSERT(reinterpret_cast<const std::byte*>(table_)
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

  flatbuffer& operator=(const flatbuffer& rhs) noexcept {
    if (&rhs == this)
      return *this;
    chunk_ = rhs.chunk_;
    table_ = rhs.table_;
    return *this;
  }

  flatbuffer& operator=(flatbuffer&& rhs) noexcept {
    chunk_ = std::exchange(rhs.chunk_, {});
    table_ = std::exchange(rhs.table_, {});
    return *this;
  }

  // -- operators -------------------------------------------------------------

  explicit operator bool() const noexcept {
    return table_ != nullptr;
  }

  const Table& operator*() const noexcept {
    VAST_ASSERT(table_);
    return *table_;
  }

  const Table* operator->() const noexcept {
    VAST_ASSERT(table_);
    return table_;
  }

  // -- accessors -------------------------------------------------------------

  /// Slices a nested FlatBuffers table pointer with shared lifetime.
  /// @note The child table will not be a root pointer; consider using the
  /// two-argument overload the if sliced table is a nested FlatBuffers root
  /// table and operations that require root tables must be supported.
  /// @pre `*this != nullptr`
  template <class ChildTable>
  [[nodiscard]] flatbuffer<ChildTable, flatbuffer_type::child>
  slice(const ChildTable& child_table) const noexcept {
    VAST_ASSERT(*this);
    return {*this, child_table};
  }

  /// Slices a nested FlatBuffers root table pointer with shared lifetime.
  /// @pre `*this != nullptr`
  /// @pre *child_table* and *nested_flatbuffer* must point to the same nested
  /// FlatBuffers table, i.e., `*(*this)->child_nested_root()` and
  /// `*(*this)->child()` respectively.
  template <class ChildTable>
  [[nodiscard]] flatbuffer<ChildTable, flatbuffer_type::root>
  slice(const ChildTable& child_table,
        const flatbuffers::Vector<uint8_t>& nested_flatbuffer) const noexcept {
    VAST_ASSERT(*this);
    VAST_ASSERT(&child_table
                == flatbuffers::GetRoot<ChildTable>(nested_flatbuffer.data()));
    return {chunk_->slice(as_bytes(nested_flatbuffer))};
  }

  /// Accesses the underlying chunk.
  /// @note The returned chunk may contain more than just the FlatBuffers table
  /// if it is not a root table.
  [[nodiscard]] const chunk_ptr& chunk() const noexcept {
    return chunk_;
  }

  // -- concepts --------------------------------------------------------------
  consteval static auto qualified_name() noexcept -> std::string_view {
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
    return f
      .object(x)
      // TODO CAF 0.19 just pass string_view to pretty name as caf::string_view
      // is removed or obsolete
      .pretty_name(caf::string_view{name.data(), name.size()})
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
    requires(Type == flatbuffer_type::root)
    : chunk_{std::move(chunk)},
      table_{flatbuffers::GetRoot<Table>(chunk_->data())} {
    // nop
  }

  /// A pointer to the underlying chunk. For root tables, the beginning of the
  /// contained data starts with the root table directly. This is used for
  /// sharing the lifetime of the flatbuffer with the chunk as well.
  chunk_ptr chunk_ = {};

  /// A pointer to the table that this FlatBuffers table pointer wraps.
  const Table* table_ = {};
};

// -- deduction guides --------------------------------------------------------

template <class Table, class ParentTable, flatbuffer_type ParentType>
flatbuffer(flatbuffer<ParentTable, ParentType>, const Table*)
  -> flatbuffer<Table, flatbuffer_type::child>;

} // namespace vast

// -- formatter ---------------------------------------------------------------

template <class Table, vast::flatbuffer_type Type>
struct fmt::formatter<vast::flatbuffer<Table, Type>> {
  template <class ParseContext>
  auto parse(const ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto
  format(const vast::flatbuffer<Table, Type>& flatbuffer, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}({})", flatbuffer.qualified_name(),
                          fmt::ptr(flatbuffer.table_));
  }
};
