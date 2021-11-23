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

/// A wrapper class around a FlatBuffers table that allows for sharing the
/// lifetime with the chunk containing the table.
/// @tparam Table The generated FlatBuffers table type.
/// @tparam IsRootTable Whether the table is a root table, i.e., starts at the
/// beginning of the chunk.
template <class Table, bool IsRootTable = true>
class flatbuffer final {
public:
  // -- member types and constants --------------------------------------------

  template <class ParentTable, bool ParentIsRootTable>
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
    requires(IsRootTable) {
    if (!chunk)
      return caf::make_error(ec::logic_error,
                             fmt::format("failed to read {} from a nullptr",
                                         Table::GetFullyQualifiedName()));
    if (chunk->size() == 0)
      return caf::make_error(
        ec::logic_error, fmt::format("failed to read {} from an empty chunk",
                                     Table::GetFullyQualifiedName()));
    if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE)
      return caf::make_error(
        ec::format_error,
        fmt::format("failed to read {} because its size {} "
                    "exceeds the maximum allowed size of {}",
                    Table::GetFullyQualifiedName(), chunk->size(),
                    FLATBUFFERS_MAX_BUFFER_SIZE));
    if (verify == verify::yes) {
      const auto* const data = reinterpret_cast<const uint8_t*>(chunk->data());
      auto verifier = flatbuffers::Verifier{data, chunk->size()};
      if (!flatbuffers::GetRoot<Table>(data)->Verify(verifier))
        return caf::make_error(ec::format_error,
                               fmt::format("failed to read {} because its "
                                           "verification failed",
                                           Table::GetFullyQualifiedName()));
#if defined(FLATBUFFERS_TRACK_VERIFIER_BUFFER_SIZE)
      VAST_ASSERT(verifier.GetComputedSize() >= chunk->size());
      if (verifier.GetComputedSize() > chunk->size())
        return caf::make_error(
          ec::format_error,
          fmt::format("failed to read {} because of {} unexpected excess bytes",
                      Table::GetFullyQualifiedName(),
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
             const char* file_identifier) noexcept requires(IsRootTable) {
    builder.Finish(offset, file_identifier);
    auto chunk = chunk::make(builder.Release());
    VAST_ASSERT(chunk);
    *this = flatbuffer{chunk};
  }

  /// Constructs a ref-counted FlatBuffers table that shares the
  /// lifetime with another FlatBuffer pointer.
  /// @pre `parent`
  /// @pre *table* must be accessible from *parent*.
  template <class ParentTable, bool ParentIsRootTable>
  flatbuffer(flatbuffer<ParentTable, ParentIsRootTable> parent,
             const Table& table) noexcept requires(!IsRootTable)
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

  flatbuffer(std::nullptr_t) noexcept {
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

  flatbuffer& operator=(std::nullptr_t) noexcept {
    chunk_ = nullptr;
    table_ = nullptr;
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

  friend bool operator==(flatbuffer lhs, flatbuffer rhs) noexcept
    requires(IsRootTable) {
    if (&lhs == &rhs)
      return true;
    if (lhs && rhs) {
      const auto lhs_bytes = as_bytes(lhs);
      const auto rhs_bytes = as_bytes(rhs);
      return std::equal(lhs_bytes.begin(), lhs_bytes.end(), rhs_bytes.begin(),
                        rhs_bytes.end());
    }
    return static_cast<bool>(lhs) == static_cast<bool>(rhs);
  }

  friend std::strong_ordering
  operator<=>(flatbuffer lhs, flatbuffer rhs) noexcept requires(IsRootTable) {
    if (&lhs == &rhs)
      return std::strong_ordering::equal;
    if (!lhs && !rhs)
      return std::strong_ordering::equal;
    if (!lhs)
      return std::strong_ordering::less;
    if (!rhs)
      return std::strong_ordering::greater;
    // TODO: Replace implementation with `std::lexicographical_compare_three_way`
    // once that is implemented for all compilers we need to support. This does
    // the same thing essentially, just a lot less generic.
    auto lhs_bytes = as_bytes(lhs);
    auto rhs_bytes = as_bytes(rhs);
    if (lhs_bytes.data() == rhs_bytes.data()
        && lhs_bytes.size() == rhs_bytes.size())
      return std::strong_ordering::equivalent;
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

  // -- accessors -------------------------------------------------------------

  /// Slices a nested FlatBuffers table pointer with shared lifetime.
  /// @note The child table will not be a root pointer; consider using the
  /// two-argument overload the if sliced table is a nested FlatBuffers root
  /// table and operations that require root tables must be supported.
  /// @pre `*this != nullptr`
  template <class ChildTable>
  [[nodiscard]] flatbuffer<ChildTable, false>
  slice(const ChildTable& child_table) const noexcept {
    VAST_ASSERT(*this);
    return flatbuffer<ChildTable, false>{*this, child_table};
  }

  /// Slices a nested FlatBuffers root table pointer with shared lifetime.
  /// @pre `*this != nullptr`
  /// @pre *child_table* and *nested_flatbuffer* must point to the same nested
  /// FlatBuffers table, i.e., `*(*this)->child_nested_root()` and
  /// `*(*this)->child()` respectively.
  template <class ChildTable>
  [[nodiscard]] flatbuffer<ChildTable, true>
  slice(const ChildTable& child_table,
        const flatbuffers::Vector<uint8_t>& nested_flatbuffer) const noexcept {
    VAST_ASSERT(*this);
    VAST_ASSERT(&child_table
                == flatbuffers::GetRoot<ChildTable>(nested_flatbuffer.data()));
    return flatbuffer<ChildTable, true>{
      chunk_->slice(as_bytes(nested_flatbuffer))};
  }

  /// Accesses the underlying chunk.
  /// @note This is only available for FlatBuffers root table pointers, as
  /// this operation is fundamentally unsafe for non-root tables, for which
  /// the table pointer is not at the beginning of the chunk.
  [[nodiscard]] const chunk_ptr& chunk() const noexcept requires(IsRootTable) {
    return chunk_;
  }

  // -- concepts --------------------------------------------------------------

  /// Gets the underlying binary representation of a FlatBuffers root table.
  /// @note This is intentionally disabled for FlatBuffers tables that are not
  /// root tables, as they are not guaranteed to be contiguous in memory.
  /// @pre `flatbuffer != nullptr`
  [[nodiscard]] friend std::span<const std::byte>
  as_bytes(const flatbuffer& flatbuffer) noexcept requires(IsRootTable) {
    VAST_ASSERT(flatbuffer);
    return as_bytes(*flatbuffer.chunk_);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, flatbuffer& x) ->
    typename Inspector::result_type {
    // When serializing, we decompose the FlatBuffers table into the chunk it
    // lives in and the offset of the table pointer inside it, and when
    // deserializing we put it all back together.
    auto table_offset = x.chunk_ ? reinterpret_cast<const std::byte*>(x.table_)
                                     - x.chunk_->data()
                                 : 0;
    auto load_callback = caf::meta::load_callback([&]() noexcept -> caf::error {
      if (x.chunk_)
        x.table_
          = reinterpret_cast<const Table*>(x.chunk_->data() + table_offset);
      return caf::none;
    });
    return f(caf::meta::type_name(Table::GetFullyQualifiedName()), x.chunk_,
             table_offset, std::move(load_callback));
  }

  friend auto
  inspect(caf::detail::stringification_inspector& f, flatbuffer& x) {
    auto str = fmt::to_string(x);
    return f(str);
  }

private:
  // -- implementation details ------------------------------------------------

  /// Constructs a ref-counted FlatBuffers root table that shares the
  /// lifetime with the chunk it's constructed from.
  /// @pre *chunk* must hold a valid *Table*.
  explicit flatbuffer(chunk_ptr chunk) noexcept requires(IsRootTable)
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

template <class Table, class ParentTable, bool ParentIsRootTable>
flatbuffer(flatbuffer<ParentTable, ParentIsRootTable>, const Table*)
  -> flatbuffer<Table, false>;

} // namespace vast

// -- formatter ---------------------------------------------------------------

template <class Table, bool IsRootTable>
struct fmt::formatter<vast::flatbuffer<Table, IsRootTable>> {
  template <class ParseContext>
  auto parse(const ParseContext& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto
  format(vast::flatbuffer<Table, IsRootTable> flatbuffer, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}({})", Table::GetFullyQualifiedName(),
                          fmt::ptr(flatbuffer.table_));
  }
};
