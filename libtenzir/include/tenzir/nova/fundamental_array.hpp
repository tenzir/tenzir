//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/variant.hpp"

namespace tenzir::nova {

template <fundamental_type Tag>
class Array<Tag> {
public:
  using ViewType = Type<Tag>::ViewType;
  using storage_t = Type<Tag>::PhysicalStorage::template apply<variant>;

  auto as_unique() const& -> Array;
  auto as_unique() && -> Array;
  auto length() const noexcept -> storage::Index;
  auto get(storage::Index i) const -> RowView<ViewType>;
  auto storage() const& -> const storage_t&;
  auto storage() && -> storage_t&&;

  template <class Storage>
    requires Type<Tag>::PhysicalStorage::template
  contains<Storage> Array(Storage storage) : storage_{std::move(storage)} {
  }

private:
  storage_t storage_;
};

static_assert(storage::unique_ownership<Array<Null>>);
static_assert(storage::unique_ownership<Array<Bool>>);
static_assert(storage::unique_ownership<Array<Int>>);
static_assert(storage::unique_ownership<Array<UInt>>);
static_assert(storage::unique_ownership<Array<Float>>);
static_assert(storage::unique_ownership<Array<String>>);
static_assert(storage::unique_ownership<Array<Blob>>);
static_assert(storage::unique_ownership<Array<Ip>>);
static_assert(storage::unique_ownership<Array<Subnet>>);
static_assert(storage::unique_ownership<Array<Time>>);
static_assert(storage::unique_ownership<Array<Duration>>);

extern template class Array<Null>;
extern template class Array<Bool>;
extern template class Array<Int>;
extern template class Array<UInt>;
extern template class Array<Float>;
extern template class Array<String>;
extern template class Array<Blob>;
extern template class Array<Ip>;
extern template class Array<Subnet>;
extern template class Array<Time>;
extern template class Array<Duration>;

} // namespace tenzir::nova
