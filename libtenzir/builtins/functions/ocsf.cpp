//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/tql2/arrow_utils.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::ocsf {

namespace {

class category_uid final : public function_plugin0 {
public:
  auto name() const -> std::string override {
    return "tql2.ocsf_category_uid";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = basic_series<string_type>{};
    auto success = function_argument_parser{"ocsf_category_uid"}
                     .add(arg, "<string>")
                     .parse(inv, dh);
    if (not success) {
      return series::null(int64_type{}, inv.length);
    }
    auto b = arrow::Int64Builder{};
    check(b.Reserve(inv.length));
    for (auto i = int64_t{0}; i < inv.length; ++i) {
      auto name = arg.array->GetView(i);
      // Uncategorized [0]
      // System Activity [1]
      // Findings [2]
      // Identity & Access Management [3]
      // Network Activity [4]
      // Discovery [5]
      // Application Activity [6]
      auto id = std::invoke([&] {
        if (name == "System Activity") {
          return 1;
        }
        if (name == "Findings") {
          return 2;
        }
        if (name == "Identity & Access Management") {
          return 3;
        }
        if (name == "Network Activity") {
          return 4;
        }
        if (name == "Discovery") {
          return 5;
        }
        if (name == "Application Activity") {
          return 6;
        }
        return 0;
      });
      check(b.Append(id));
    }
    return series{int64_type{}, finish(b)};
  }
};

class class_uid final : public function_plugin0 {
public:
  auto name() const -> std::string override {
    return "tql2.ocsf_class_uid";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = basic_series<string_type>{};
    auto success = function_argument_parser{"ocsf_class_uid"}
                     .add(arg, "<string>")
                     .parse(inv, dh);
    if (not success) {
      return series::null(int64_type{}, inv.length);
    }
    auto b = arrow::Int64Builder{};
    check(b.Reserve(inv.length));
    for (auto i = int64_t{0}; i < inv.length; ++i) {
      auto name = arg.array->GetView(i);
      // TODO: Auto-generate a table for this!
      auto id = std::invoke([&] {
        if (name == "Process Activity") {
          return 1007;
        }
        return 0;
      });
      check(b.Append(id));
    }
    return series{int64_type{}, finish(b)};
  }
};

} // namespace

} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::category_uid)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::class_uid)
