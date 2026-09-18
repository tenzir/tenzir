//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/structured_storage.hpp"
#include "tenzir/ref.hpp"

#include <memory>

namespace tenzir::nova {

template <>
class RowView<List>;

template <>
class Array<List> {
public:
  using PhysicalStorage = Type<List>::PhysicalStorage::apply<variant>;
  Array(storage::ListStorage storage);
  Array(storage::ConstantStorage<List, RowView<List>> storage);
  Array(storage::SharedOwner<storage::Span[]> spans, Array<Data> values);
  ~Array();

  Array(const Array&);
  Array(Array&&) noexcept;
  auto operator=(const Array&) -> Array&;
  auto operator=(Array&&) noexcept -> Array&;

  /// Borrows the physical representation.
  auto storage() const& -> PhysicalStorage const&;
  /// Exposes the physical representation for moving.
  auto storage() && -> PhysicalStorage&&;
  /// Expands constants to columnar storage; primary arrays retain sharing.
  auto to_primary() const -> Array;

  auto as_unique() const& -> Array;
  auto as_unique() && -> Array;
  auto length() const noexcept -> storage::Index;
  auto get(storage::Index i) const -> RowView<List>;

private:
  PhysicalStorage storage_;
};

static_assert(storage::unique_ownership<Array<List>>);

template <>
class RowView<List> {
public:
  RowView(List const& value);
  auto get(storage::Index j) const -> RowView<Data>;
  auto length() const noexcept -> storage::Index;

  class iterator;
  auto begin() const -> iterator;
  auto end() const -> iterator;

private:
  friend class storage::ListStorage;
  RowView(storage::ListStorage::Storage const& storage, storage::Index row);

  struct Primary {
    Ref<storage::ListStorage::Storage const> storage;
    storage::Index row;

    auto operator==(Primary const& other) const -> bool {
      return std::addressof(storage.get())
               == std::addressof(other.storage.get())
             and row == other.row;
    }
  };

  struct Constant {
    Ref<List const> value;

    auto operator==(Constant const& other) const -> bool {
      return &value.get() == &other.value.get();
    }
  };

  using Representation = variant<Primary, Constant>;
  Representation representation_;
};

class RowView<List>::iterator {
public:
  using value_type = RowView<Data>;
  using difference_type = std::ptrdiff_t;

  auto operator*() const -> value_type;
  auto operator++() -> iterator&;
  friend auto operator==(iterator const&, iterator const&) -> bool;

private:
  friend class RowView<List>;
  iterator(Representation representation, storage::Index index);

  Representation representation_;
  storage::Index index_;
};

static_assert(fully_implemented_type<Type<List>>);

} // namespace tenzir::nova
