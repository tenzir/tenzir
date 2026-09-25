//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/nova/aggregation.hpp>

#include <cmath>
#include <concepts>

namespace tenzir::plugins::nova_statistics {

/// The input kind of a statistical aggregation, which accepts either numbers
/// or, if allowed, durations. Values are visited one at a time: nulls and
/// NaNs are skipped, the first value fixes the kind, and a value of another
/// type warns and fails the aggregation for good.
class NumericKind {
public:
  enum class Kind { none, numeric, duration, failed };

  explicit NumericKind(bool allow_duration = true)
    : allow_duration_{allow_duration} {
  }

  auto kind() const -> Kind {
    return kind_;
  }

  auto failed() const -> bool {
    return kind_ == Kind::failed;
  }

  /// Calls `f` with the value of `view` as `Int`, `UInt`, `Float` or
  /// `Duration` if it contributes.
  template <class Tag, class F>
  auto
  visit(nova::RowView<Tag> view, location source, diagnostic_handler& dh, F&& f)
    -> void {
    using namespace nova;
    if (failed()) {
      return;
    }
    if constexpr (std::same_as<Tag, Null>) {
      return;
    } else if constexpr (concepts::one_of<Tag, Int, UInt, Float>) {
      if (kind_ == Kind::duration) {
        return incompatible("duration", Type<Tag>::static_name, source, dh);
      }
      kind_ = Kind::numeric;
      if constexpr (std::same_as<Tag, Float>) {
        if (std::isnan(*view)) {
          return;
        }
      }
      f(*view);
    } else if constexpr (std::same_as<Tag, Duration>) {
      if (not allow_duration_) {
        return invalid(Type<Tag>::static_name, source, dh);
      }
      if (kind_ == Kind::numeric) {
        return incompatible("number", Type<Tag>::static_name, source, dh);
      }
      kind_ = Kind::duration;
      f(*view);
    } else {
      invalid(Type<Tag>::static_name, source, dh);
    }
  }

  /// Visits the values of `array` at `rows`, in order.
  template <class F>
  auto
  visit(nova::Array<nova::Data> const& array, nova::storage::BitMap const& rows,
        location source, diagnostic_handler& dh, F&& f) -> void {
    using namespace nova;
    auto const typed = [&]<data_type Tag>(Array<Tag> const& array,
                                          storage::BitMap const& rows) {
      if constexpr (not std::same_as<Tag, Null>) {
        for (auto row : storage::true_bits(rows)) {
          visit(array.get(row), source, dh, f);
          if (failed()) {
            return;
          }
        }
      }
    };
    match(
      array,
      [&]<data_type Tag>(Array<Tag> const& array) {
        typed(array, rows);
      },
      [&](UnionArray const& u) {
        for (auto const& field : u.fields()) {
          auto const selected = rows & field.present;
          if (selected.any() and not failed()) {
            match(field.data, [&]<data_type Tag>(Array<Tag> const& array) {
              typed(array, selected);
            });
          }
        }
      });
  }

  /// Visits the elements of a list row, in order.
  template <class F>
  auto visit(nova::ListElements const& elements, location source,
             diagnostic_handler& dh, F&& f) -> void {
    elements.for_each([&](auto value) {
      visit(value, source, dh, f);
    });
  }

private:
  auto incompatible(std::string_view kind, std::string_view type,
                    location source, diagnostic_handler& dh) -> void {
    diagnostic::warning("got incompatible types `{}` and `{}`", kind, type)
      .primary(source)
      .emit(dh);
    kind_ = Kind::failed;
  }

  auto invalid(std::string_view type, location source, diagnostic_handler& dh)
    -> void {
    if (allow_duration_) {
      diagnostic::warning("expected `int`, `uint`, `float` or `duration`, got "
                          "`{}`",
                          type)
        .primary(source)
        .emit(dh);
    } else {
      diagnostic::warning("expected `int`, `uint` or `float`, got `{}`", type)
        .primary(source)
        .emit(dh);
    }
    kind_ = Kind::failed;
  }

  bool allow_duration_ = true;
  Kind kind_ = Kind::none;
};

/// Converts a visited value to a `double`.
template <class T>
auto to_double(T value) -> double {
  if constexpr (std::same_as<T, nova::Duration>) {
    return static_cast<double>(value.count());
  } else {
    return static_cast<double>(value);
  }
}

/// The result of a statistic computed over `double`s: `null` without values
/// or after a failure, a `duration` for duration input, and a `float`
/// otherwise.
inline auto make_result(NumericKind const& kind, Option<double> value)
  -> nova::Data {
  if (not value) {
    return nova::Data{};
  }
  switch (kind.kind()) {
    case NumericKind::Kind::none:
    case NumericKind::Kind::failed:
      return nova::Data{};
    case NumericKind::Kind::duration:
      return nova::Data{
        nova::Duration{static_cast<nova::Duration::rep>(*value)}};
    case NumericKind::Kind::numeric:
      return nova::Data{*value};
  }
  TENZIR_UNREACHABLE();
}

/// The list kernel of a statistic whose accumulator `Stat` provides
/// `add(double)` and `get(NumericKind const&) -> Data`, with `NumericKind`
/// built from `allow_duration`. For statistics whose list call and
/// accumulator coincide in everything but the input representation.
template <class Stat, class Make>
auto eval_statistic(nova::ValueArgument const& x, nova::EvalFrame const& frame,
                    bool allow_duration, Make make) -> nova::Array<nova::Data> {
  return nova::aggregate_lists(x, frame,
                               [&](nova::ListElements const& elements,
                                   nova::ArrayBuilder<nova::Data>& builder) {
                                 auto kind = NumericKind{allow_duration};
                                 Stat stat = make();
                                 kind.visit(elements, x.source, frame,
                                            [&](auto value) {
                                              stat.add(value);
                                            });
                                 nova::append_data(builder, stat.get(kind));
                               });
}

} // namespace tenzir::plugins::nova_statistics
