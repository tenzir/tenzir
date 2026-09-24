#include "tenzir/hash/hash.hpp"
#include "tenzir/nova/comparison.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/variant.hpp"
#include "tenzir/variant_traits.hpp"

#include <bit>
#include <cmath>
#include <limits>
#include <tuple>
#include <utility>

namespace tenzir::nova {

namespace {

auto equal_records(const RowView<Record>& l, const RowView<Record>& r) -> bool;
auto equal_lists(const RowView<List>& l, const RowView<List>& r) -> bool;

} // namespace

auto equal(const RowView<Data>& l, const RowView<Data>& r) -> bool {
  return match(
    std::tie(l, r),
    []<typename L, typename R>(RowView<L> lv, RowView<R> rv) -> bool {
      if constexpr (std::same_as<L, Null> and std::same_as<R, Null>) {
        return true;
      } else if constexpr (std::same_as<L, Record>
                           and std::same_as<R, Record>) {
        return equal_records(lv, rv);
      } else if constexpr (std::same_as<L, List> and std::same_as<R, List>) {
        return lv.length() == rv.length() and equal_lists(lv, rv);
      } else if constexpr (concepts::one_of<L, Int, UInt, Float>
                           and concepts::one_of<R, Int, UInt, Float>) {
        return compare<ast::binary_op::eq>(*lv, *rv);
      } else if constexpr (std::same_as<L, R>) {
        return *lv == *rv;
      } else {
        return false;
      }
    });
}

auto hash(const RowView<Data>& row) noexcept -> std::size_t {
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
  };
  return match(row, []<typename V>(RowView<V> view) -> std::size_t {
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
        return tenzir::hash(category::negative_integer,
                            static_cast<Int>(value));
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
  });
}

namespace {

auto equal_lists(const RowView<List>& l, const RowView<List>& r) -> bool {
  auto rit = r.begin();
  for (auto lv : l) {
    if (not equal(lv, *rit)) {
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

auto equal_records(const RowView<Record>& l, const RowView<Record>& r) -> bool {
  if (field_count(l) != field_count(r)) {
    return false;
  }
  for (auto [name, lv] : l) {
    auto found = false;
    for (auto [rname, rv] : r) {
      if (rname == name) {
        found = true;
        if (not equal(lv, rv)) {
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
