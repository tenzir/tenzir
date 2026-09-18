#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/variant.hpp"
#include "tenzir/variant_traits.hpp"

#include <cmath>
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
      } else if constexpr (std::same_as<L, R>) {
        if constexpr (std::same_as<L, Float>) {
          return (std::isnan(*lv) and std::isnan(*rv)) or *lv == *rv;
        } else {
          return *lv == *rv;
        }
      } else if constexpr ((std::same_as<L, Int> and std::same_as<R, UInt>)
                           or (std::same_as<L, UInt>
                               and std::same_as<R, Int>)) {
        return std::cmp_equal(*lv, *rv);
      } else {
        return false;
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
