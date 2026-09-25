#include "tenzir/hash/hash.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/comparison.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/variant.hpp"
#include "tenzir/variant_traits.hpp"

#include <bit>
#include <cmath>
#include <limits>
#include <span>
#include <tuple>
#include <utility>

namespace tenzir::nova {

namespace {

template <bool Reflexive>
auto equal_records(const RowView<Record>& l, const RowView<Record>& r) -> bool;
template <bool Reflexive>
auto equal_lists(const RowView<List>& l, const RowView<List>& r) -> bool;

/// `equal`, or `equivalent` if `Reflexive`, which also equates NaNs.
template <bool Reflexive>
auto equal_impl(const RowView<Data>& l, const RowView<Data>& r) -> bool {
  return match(
    std::tie(l, r),
    []<typename L, typename R>(RowView<L> lv, RowView<R> rv) -> bool {
      if constexpr (std::same_as<L, Null> and std::same_as<R, Null>) {
        return true;
      } else if constexpr (std::same_as<L, Record>
                           and std::same_as<R, Record>) {
        return equal_records<Reflexive>(lv, rv);
      } else if constexpr (std::same_as<L, List> and std::same_as<R, List>) {
        return lv.length() == rv.length() and equal_lists<Reflexive>(lv, rv);
      } else if constexpr (concepts::one_of<L, Int, UInt, Float>
                           and concepts::one_of<R, Int, UInt, Float>) {
        if constexpr (Reflexive
                      and std::same_as<L, Float> and std::same_as<R, Float>) {
          if (std::isnan(*lv) and std::isnan(*rv)) {
            return true;
          }
        }
        return compare<ast::binary_op::eq>(*lv, *rv);
      } else if constexpr (std::same_as<L, Secret>
                           and std::same_as<R, Secret>) {
        // Secrets never expose their value, so all of them are equivalent.
        return true;
      } else if constexpr (std::same_as<L, R>) {
        return *lv == *rv;
      } else {
        return false;
      }
    });
}

} // namespace

auto equal(const RowView<Data>& l, const RowView<Data>& r) -> bool {
  return equal_impl<false>(l, r);
}

auto equivalent(const RowView<Data>& l, const RowView<Data>& r) -> bool {
  return equal_impl<true>(l, r);
}

namespace {

enum class category : uint8_t {
  null,
  integer,
  negative_integer,
  floating,
  string,
  blob,
  ip,
  subnet,
  time,
  duration,
  boolean,
  list,
  record,
  secret,
};

/// The hash of one value of type `V`, consistent with `equal`: numbers hash
/// by value regardless of their type.
template <typename V>
auto hash_view(RowView<V> const& view) noexcept -> std::size_t {
  if constexpr (std::same_as<V, Null>) {
    return tenzir::hash(category::null);
  } else if constexpr (std::same_as<V, Int>) {
    if (*view >= 0) {
      return tenzir::hash(category::integer, static_cast<uint64_t>(*view));
    }
    return tenzir::hash(category::negative_integer, *view);
  } else if constexpr (std::same_as<V, UInt>) {
    return tenzir::hash(category::integer, *view);
  } else if constexpr (std::same_as<V, Float>) {
    auto const value = *view;
    auto const integral = std::trunc(value) == value;
    auto const unsigned_upper
      = std::ldexp(1.0, std::numeric_limits<UInt>::digits);
    if (integral and value >= 0.0 and value < unsigned_upper) {
      return tenzir::hash(category::integer, static_cast<UInt>(value));
    }
    auto const signed_lower
      = -std::ldexp(1.0, std::numeric_limits<Int>::digits);
    if (integral and value >= signed_lower and value < 0.0) {
      return tenzir::hash(category::negative_integer, static_cast<Int>(value));
    }
    // Every NaN hashes alike, whatever its sign or payload, so that the hash
    // is also consistent with `equivalent`.
    if (std::isnan(value)) {
      return tenzir::hash(
        category::floating,
        std::bit_cast<uint64_t>(std::numeric_limits<double>::quiet_NaN()));
    }
    return tenzir::hash(category::floating, std::bit_cast<uint64_t>(value));
  } else if constexpr (std::same_as<V, List>) {
    auto result = tenzir::hash(category::list, view.length());
    for (auto element : view) {
      result = tenzir::hash(result, nova::hash(element));
    }
    return result;
  } else if constexpr (std::same_as<V, Record>) {
    auto combined = std::size_t{0};
    auto size = std::size_t{0};
    for (auto [name, value] : view) {
      combined += tenzir::hash(name, nova::hash(value));
      ++size;
    }
    return tenzir::hash(category::record, combined, size);
  } else if constexpr (std::same_as<V, Secret>) {
    // Secrets never expose their value, so all of them hash alike.
    TENZIR_UNUSED(view);
    return tenzir::hash(category::secret);
  } else {
    constexpr auto tag = [] {
      if constexpr (std::same_as<V, String>) {
        return category::string;
      } else if constexpr (std::same_as<V, Blob>) {
        return category::blob;
      } else if constexpr (std::same_as<V, Ip>) {
        return category::ip;
      } else if constexpr (std::same_as<V, Subnet>) {
        return category::subnet;
      } else if constexpr (std::same_as<V, Time>) {
        return category::time;
      } else if constexpr (std::same_as<V, Duration>) {
        return category::duration;
      } else {
        static_assert(std::same_as<V, Bool>);
        return category::boolean;
      }
    }();
    return tenzir::hash(tag, *view);
  }
}

} // namespace

auto hash(const RowView<Data>& row) noexcept -> std::size_t {
  return match(row, []<typename V>(RowView<V> const& view) -> std::size_t {
    return hash_view(view);
  });
}

auto hash_rows(Array<Data> const& column, storage::BitMap const& rows,
               std::span<uint64_t> hashes) -> void {
  TENZIR_ASSERT_EQ(static_cast<size_t>(rows.true_count()), hashes.size());
  auto i = size_t{0};
  match(
    column,
    [&]<data_type Tag>(Array<Tag> const& array) {
      for (auto row : storage::true_bits(rows)) {
        hashes[i] = tenzir::hash(
          hashes[i], static_cast<uint64_t>(hash_view(array.get(row))));
        ++i;
      }
    },
    [&](UnionArray const& array) {
      for (auto row : storage::true_bits(rows)) {
        hashes[i] = tenzir::hash(hashes[i],
                                 static_cast<uint64_t>(hash(array.get(row))));
        ++i;
      }
    });
}

namespace {

template <bool Reflexive>
auto equal_lists(const RowView<List>& l, const RowView<List>& r) -> bool {
  auto rit = r.begin();
  for (auto lv : l) {
    if (not equal_impl<Reflexive>(lv, *rit)) {
      return false;
    }
    ++rit;
  }
  return true;
}

auto field_count(const RowView<Record>& row) -> std::size_t {
  auto count = std::size_t{0};
  for (auto&& _ : row) {
    static_cast<void>(_);
    ++count;
  }
  return count;
}

template <bool Reflexive>
auto equal_records(const RowView<Record>& l, const RowView<Record>& r) -> bool {
  if (field_count(l) != field_count(r)) {
    return false;
  }
  for (auto [name, lv] : l) {
    auto found = false;
    for (auto [rname, rv] : r) {
      if (rname == name) {
        found = true;
        if (not equal_impl<Reflexive>(lv, rv)) {
          return false;
        }
        break;
      }
    }
    if (not found) {
      return false;
    }
  }
  return true;
}

} // namespace

} // namespace tenzir::nova
