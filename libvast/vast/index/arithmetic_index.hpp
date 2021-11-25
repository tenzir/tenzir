//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/base.hpp"
#include "vast/bitmap_index.hpp"
#include "vast/coder.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/base.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/ids.hpp"
#include "vast/index/container_lookup.hpp"
#include "vast/type.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

#include <algorithm>
#include <memory>
#include <type_traits>

namespace vast {

/// An index for arithmetic values.
template <class T, class Binner = void>
class arithmetic_index : public value_index {
public:
  // clang-format off
  using value_type =
    std::conditional_t<
      detail::is_any_v<T, time, duration>,
      duration::rep,
      std::conditional_t<
        detail::is_any_v<T, bool, integer::value_type, count, real>,
        T,
        std::false_type
      >
    >;
  // clang-format on

  static_assert(!std::is_same_v<value_type, std::false_type>,
                "invalid type T for arithmetic_index");

  using multi_level_range_coder = multi_level_coder<range_coder<ids>>;

  // clang-format off
  using coder_type = std::conditional_t<
    std::is_same_v<T, bool>,
    singleton_coder<ids>,
    multi_level_range_coder
  >;
  // clang-format on

  // clang-format off
  using binner_type =
    std::conditional_t<
      std::is_void_v<Binner>,
      // Choose a space-efficient binner if none specified.
      std::conditional_t<
        detail::is_any_v<T, time, duration>,
        decimal_binner<9>, // nanoseconds -> seconds
        std::conditional_t<
          std::is_same_v<T, real>,
          precision_binner<10>, // no fractional part
          identity_binner
        >
      >,
      Binner
    >;
  // clang-format on

  using bitmap_index_type = bitmap_index<value_type, coder_type, binner_type>;

  /// Constructs an arithmetic index.
  /// @param t An arithmetic type.
  /// @param opts Runtime context for index parameterization.
  explicit arithmetic_index(vast::type t, caf::settings opts = {})
    : value_index{std::move(t), std::move(opts)} {
    if constexpr (std::is_same_v<coder_type, multi_level_range_coder>) {
      auto i = options().find("base");
      if (i == options().end()) {
        // Some early experiments found that 8 yields the best average
        // performance, presumably because it's a power of 2.
        bmi_ = bitmap_index_type{base::uniform<64>(8)};
      } else {
        auto str = caf::get<caf::config_value::string>(i->second);
        auto b = to<base>(str);
        VAST_ASSERT(b); // pre-condition is that this was validated
        bmi_ = bitmap_index_type{base{std::move(*b)}};
      }
    }
  }

  caf::error serialize(caf::serializer& sink) const override {
    return caf::error::eval(
      [&] {
        return value_index::serialize(sink);
      },
      [&] {
        return sink(bmi_);
      });
  }

  caf::error deserialize(caf::deserializer& source) override {
    return caf::error::eval(
      [&] {
        return value_index::deserialize(source);
      },
      [&] {
        return source(bmi_);
      });
  }

  bool deserialize(detail::legacy_deserializer& source) override {
    if (!value_index::deserialize(source))
      return false;
    return source(bmi_);
  }

private:
  bool append_impl(data_view d, id pos) override {
    auto append = [&](auto x) {
      bmi_.skip(pos - bmi_.size());
      bmi_.append(x);
      return true;
    };
    auto f = detail::overload{
      [&](auto&&) {
        return false;
      },
      [&](view<bool> x) {
        return append(x);
      },
      [&](view<integer> x) {
        return append(x.value);
      },
      [&](view<count> x) {
        return append(x);
      },
      [&](view<real> x) {
        return append(x);
      },
      [&](view<duration> x) {
        return append(x.count());
      },
      [&](view<time> x) {
        return append(x.time_since_epoch().count());
      },
    };
    return caf::visit(f, d);
  }

  [[nodiscard]] caf::expected<ids>
  lookup_impl(relational_operator op, data_view d) const override {
    auto f = detail::overload{
      [&](auto x) -> caf::expected<ids> {
        return caf::make_error(ec::type_clash, value_type{}, materialize(x));
      },
      [&](view<bool> x) -> caf::expected<ids> {
        return bmi_.lookup(op, x);
      },
      [&](view<integer> x) -> caf::expected<ids> {
        return bmi_.lookup(op, x.value);
      },
      [&](view<count> x) -> caf::expected<ids> {
        return bmi_.lookup(op, x);
      },
      [&](view<real> x) -> caf::expected<ids> {
        return bmi_.lookup(op, x);
      },
      [&](view<duration> x) -> caf::expected<ids> {
        return bmi_.lookup(op, x.count());
      },
      [&](view<time> x) -> caf::expected<ids> {
        return bmi_.lookup(op, x.time_since_epoch().count());
      },
      [&](view<list> xs) {
        return detail::container_lookup(*this, op, xs);
      },
    };
    return caf::visit(f, d);
  };

  [[nodiscard]] size_t memusage_impl() const override {
    return bmi_.memusage();
  }

  bitmap_index_type bmi_;
};

} // namespace vast
