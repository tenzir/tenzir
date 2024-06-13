//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/arrow_utils.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::time_ {

namespace {

class time_ final : public function_plugin0 {
public:
  auto name() const -> std::string override {
    return "tql2.time";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = variant<basic_series<time_type>, basic_series<string_type>>{};
    auto success
      = function_argument_parser{"time"}.add(arg, "<string>").parse(inv, dh);
    if (not success) {
      return series::null(time_type{}, inv.length);
    }
    auto result = arg.match(
      [](basic_series<time_type>& arg) {
        return std::move(arg.array);
      },
      [&](basic_series<string_type>& arg) {
        auto b = arrow::TimestampBuilder{
          std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
          arrow::default_memory_pool()};
        check(b.Reserve(inv.length));
        for (auto i = 0; i < arg.array->length(); ++i) {
          if (arg.array->IsNull(i)) {
            check(b.AppendNull());
            continue;
          }
          auto result = tenzir::time{};
          if (parsers::time(arg.array->GetView(i), result)) {
            check(b.Append(result.time_since_epoch().count()));
          } else {
            // TODO: ?
            check(b.AppendNull());
          }
        }
        return finish(b);
      });
    return series{time_type{}, std::move(result)};
  }
};

class seconds_since_epoch final : public function_plugin0 {
public:
  auto name() const -> std::string override {
    return "tql2.seconds_since_epoch";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = basic_series<time_type>{};
    auto success = function_argument_parser{"seconds_since_epoch"}
                     .add(arg, "<time>")
                     .parse(inv, dh);
    if (not success) {
      return series::null(double_type{}, inv.length);
    }
    auto& ty = caf::get<arrow::TimestampType>(*arg.array->type());
    TENZIR_ASSERT(ty.unit() == arrow::TimeUnit::NANO);
    TENZIR_ASSERT(ty.timezone().empty());
    auto factor = 1000 * 1000 * 1000;
    auto b = arrow::DoubleBuilder{};
    check(b.Reserve(inv.length));
    for (auto i = 0; i < inv.length; ++i) {
      if (arg.array->IsNull(i)) {
        check(b.AppendNull());
        continue;
      }
      auto val = arg.array->Value(i);
      auto pre = static_cast<double>(val / factor);
      auto post = static_cast<double>(val % factor) / factor;
      check(b.Append(pre + post));
    }
    return series{double_type{}, finish(b)};
  }
};

class now final : public function_plugin0 {
public:
  auto name() const -> std::string override {
    return "tql2.now";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto success = function_argument_parser{"now"}.parse(inv, dh);
    if (not success) {
      return series::null(time_type{}, inv.length);
    }
    auto result = time{time::clock::now()};
    auto b = series_builder{type{time_type{}}};
    for (auto i = int64_t{0}; i < inv.length; ++i) {
      b.data(result);
    }
    return b.finish_assert_one_array();
  }
};

} // namespace

} // namespace tenzir::plugins::time_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::time_)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::seconds_since_epoch)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::now)
