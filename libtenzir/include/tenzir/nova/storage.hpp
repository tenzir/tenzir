//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/shared_owner.hpp"
#include "tenzir/nova/storage_fwd.hpp"

#include <algorithm>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace tenzir::nova::storage {

class NullStorage {
public:
  using ViewType = std::monostate;

  auto as_unique() const& -> NullStorage {
    return *this;
  }

  auto as_unique() && -> NullStorage {
    return std::move(*this);
  }

  auto length() const noexcept -> Index {
    return length_;
  }

  auto get(Index i) const -> std::monostate {
    TENZIR_UNUSED(i);
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
    TENZIR_ASSERT_LT_EXPENSIVE(i, length_);
    return {};
  }

  NullStorage(Index length) : length_{length} {
  }

  class Mutable {
  public:
    explicit Mutable(Index length) : length_{length} {
    }

    auto length() const noexcept -> Index {
      return length_;
    }

    auto finish() const -> NullStorage {
      return NullStorage{length_};
    }

    Mutable(const Mutable&) = delete;
    Mutable& operator=(const Mutable&) = delete;
    Mutable(Mutable&&) = default;
    Mutable& operator=(Mutable&&) = default;
    ~Mutable() = default;

  private:
    Index length_ = 0;
  };

private:
  Index length_ = 0;
};
static_assert(storage<NullStorage>);

template <typename StorageT, typename ViewT = StorageT>
class ConstantStorage {
public:
  using ViewType = ViewT;

  auto as_unique() const& -> ConstantStorage {
    return *this;
  }

  auto as_unique() && -> ConstantStorage {
    return std::move(*this);
  }

  auto length() const noexcept -> Index {
    return length_;
  }

  auto get(Index i) const -> ViewType {
    TENZIR_UNUSED(i);
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
    TENZIR_ASSERT_LT_EXPENSIVE(i, length_);
    return value_;
  }

  auto value() const -> StorageT const& {
    return value_;
  }

  ConstantStorage(Index length, StorageT value)
    : length_{length}, value_(std::move(value)) {
  }

private:
  Index length_ = 0;
  StorageT value_;
};
static_assert(storage<ConstantStorage<int>>);

template <typename T>
class SparseStorage {
public:
  using ViewType = T;

  auto as_unique() const& -> SparseStorage {
    return SparseStorage{data_.as_unique()};
  }

  auto as_unique() && -> SparseStorage {
    return SparseStorage{std::move(data_).as_unique()};
  }

  auto length() const noexcept -> Index {
    return data_.length();
  }

  auto get(Index i) const -> T {
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
    TENZIR_ASSERT_LT_EXPENSIVE(i, data_.length());
    return data_[i];
  }

  SparseStorage(DataOwner<T[]> data) : data_{std::move(data)} {
  }

  class Mutable {
  public:
    explicit Mutable(Index length)
      requires std::default_initializable<T>
      : data_{DataOwner<T[]>::make_value(length, T{})} {
    }

    explicit Mutable(SparseStorage storage)
      : data_{std::move(storage.data_).as_unique()} {
    }

    auto length() const noexcept -> Index {
      return data_.length();
    }

    auto get(Index i) const -> T {
      TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
      TENZIR_ASSERT_LT_EXPENSIVE(i, data_.length());
      return data_[i];
    }

    auto set(Index i, T value) -> void {
      TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
      TENZIR_ASSERT_LT_EXPENSIVE(i, data_.length());
      data_[i] = std::move(value);
    }

    /// Raw access to the `length()` elements, for tight loops that have
    /// already validated their indices.
    auto data() noexcept -> T* {
      return data_.begin();
    }

    auto copy_from(SparseStorage const& other) -> void
      requires std::is_trivially_copyable_v<T>
    {
      TENZIR_ASSERT_EQ(length(), other.length());
      auto const byte_size = static_cast<std::size_t>(length()) * sizeof(T);
      if (byte_size > 0) {
        std::memcpy(data_.begin(), other.data_.begin(), byte_size);
      }
    }

    auto finish() && -> SparseStorage {
      return SparseStorage{std::move(data_)};
    }

    Mutable(const Mutable&) = delete;
    Mutable& operator=(const Mutable&) = delete;
    Mutable(Mutable&&) = default;
    Mutable& operator=(Mutable&&) = default;
    ~Mutable() = default;

  private:
    DataOwner<T[]> data_;
  };

private:
  DataOwner<T[]> data_;
};
static_assert(storage<SparseStorage<int>>);

template <typename T>
class DenseOffsetStorage {
public:
  using ViewType = T;
  using DataOwner = storage::DataOwner<T[]>;
  using OffsetOwner = storage::DataOwner<Index[]>;

  auto as_unique() const& -> DenseOffsetStorage {
    return DenseOffsetStorage{data_.as_unique(), offsets_.as_unique()};
  }

  auto as_unique() && -> DenseOffsetStorage {
    return DenseOffsetStorage{std::move(data_).as_unique(),
                              std::move(offsets_).as_unique()};
  }

  auto length() const noexcept -> Index {
    return offsets_ ? offsets_.length() : data_.length();
  }

  auto get(Index i) const -> T {
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
    if (not offsets_) {
      TENZIR_ASSERT_LT_EXPENSIVE(i, data_.length());
      return data_[i];
    }
    TENZIR_ASSERT_LT_EXPENSIVE(i, offsets_.length());
    const auto offset = offsets_[i];
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, offset);
    TENZIR_ASSERT_LT_EXPENSIVE(offset, data_.length());
    return data_[offset];
  }

  DenseOffsetStorage(DataOwner data, OffsetOwner offsets)
    : data_{std::move(data)}, offsets_{std::move(offsets)} {
  }

  class Mutable {
  public:
    explicit Mutable(Index length)
      : offsets_{OffsetOwner::make_value(length, Index{-1})} {
    }

    auto length() const noexcept -> Index {
      return offsets_.length();
    }

    auto set(Index i, T value) -> void {
      TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
      TENZIR_ASSERT_LT_EXPENSIVE(i, offsets_.length());
      data_.emplace_back(std::move(value));
      offsets_[i] = data_.size() - 1;
    }

    auto finish() && -> DenseOffsetStorage {
      return DenseOffsetStorage{std::move(data_).finish(), std::move(offsets_)};
    }

    Mutable(const Mutable&) = delete;
    Mutable& operator=(const Mutable&) = delete;
    Mutable(Mutable&&) = default;
    Mutable& operator=(Mutable&&) = default;
    ~Mutable() = default;

  private:
    typename DataOwner::Builder data_;
    OffsetOwner offsets_;
  };

private:
  DataOwner data_;
  OffsetOwner offsets_;
};
static_assert(storage<DenseOffsetStorage<int>>);

template <typename Char, typename View>
class DenseOffsetBytesStorage {
public:
  using ViewType = View;
  using DataOwner = storage::DataOwner<Char[]>;
  using RangeOwner = storage::DataOwner<Span[]>;

  auto as_unique() const& -> DenseOffsetBytesStorage {
    return DenseOffsetBytesStorage{data_.as_unique(), ranges_.as_unique()};
  }

  auto as_unique() && -> DenseOffsetBytesStorage {
    return DenseOffsetBytesStorage{std::move(data_).as_unique(),
                                   std::move(ranges_).as_unique()};
  }

  auto length() const noexcept -> Index {
    return ranges_ ? ranges_.length() : data_.length();
  }

  auto get(Index i) const -> View {
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
    TENZIR_ASSERT_LT_EXPENSIVE(i, ranges_.length());
    const auto span = ranges_[i];
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, span.begin);
    TENZIR_ASSERT_LEQ_EXPENSIVE(span.begin, span.end);
    TENZIR_ASSERT_LEQ_EXPENSIVE(span.end, data_.length());
    return View{data_.begin() + span.begin, data_.begin() + span.end};
  }

  DenseOffsetBytesStorage(DataOwner data, RangeOwner ranges)
    : data_{std::move(data)}, ranges_{std::move(ranges)} {
  }

  auto data() const -> const DataOwner& {
    return data_;
  }

  auto span(Index i) const -> Span {
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
    TENZIR_ASSERT_LT_EXPENSIVE(i, ranges_.length());
    return ranges_[i];
  }

  class Mutable {
  public:
    explicit Mutable(Index length)
      : ranges_{RangeOwner::make_value(length, Span{-1, -1})} {
    }

    auto length() const noexcept -> Index {
      return ranges_.length();
    }

    auto reserve_data(Index size) -> void {
      TENZIR_ASSERT_LEQ(0, size);
      data_builder_.reserve_exact(size);
    }

    auto copy_from(DenseOffsetBytesStorage const& other) -> void {
      TENZIR_ASSERT_EQ(length(), other.length());
      TENZIR_ASSERT_EQ(data_builder_.size(), 0);
      if (other.data_.length() > 0) {
        data_builder_.move_append(other.data_.begin(), other.data_.end());
      }
      auto const byte_size = static_cast<std::size_t>(length()) * sizeof(Span);
      if (byte_size > 0) {
        std::memcpy(ranges_.begin(), other.ranges_.begin(), byte_size);
      }
    }

    auto set(Index i, View value) -> void {
      TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
      TENZIR_ASSERT_LT_EXPENSIVE(i, ranges_.length());
      const auto begin = data_builder_.size();
      data_builder_.move_append(value.begin(), value.end());
      ranges_[i] = Span{begin, begin + static_cast<Index>(value.size())};
    }

    auto set_same_as(Index destination, Index source) -> void {
      TENZIR_ASSERT_LEQ_EXPENSIVE(0, destination);
      TENZIR_ASSERT_LT_EXPENSIVE(destination, ranges_.length());
      TENZIR_ASSERT_LEQ_EXPENSIVE(0, source);
      TENZIR_ASSERT_LT_EXPENSIVE(source, ranges_.length());
      auto const span = ranges_[source];
      TENZIR_ASSERT_LEQ(0, span.begin);
      TENZIR_ASSERT_LEQ(span.begin, span.end);
      TENZIR_ASSERT_LEQ(span.end, data_builder_.size());
      ranges_[destination] = span;
    }

    auto finish(bool trim = false) && -> DenseOffsetBytesStorage {
      return DenseOffsetBytesStorage{std::move(data_builder_).finish(trim),
                                     std::move(ranges_)};
    }

    Mutable(const Mutable&) = delete;
    Mutable& operator=(const Mutable&) = delete;
    Mutable(Mutable&&) = default;
    Mutable& operator=(Mutable&&) = default;
    ~Mutable() = default;

  private:
    typename DataOwner::Builder data_builder_;
    RangeOwner ranges_;
  };

private:
  DataOwner data_;
  RangeOwner ranges_;
};

} // namespace tenzir::nova::storage
