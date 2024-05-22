//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::ocsf {

namespace {

using namespace tql2;

class category_uid final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.ocsf_category_uid";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    TENZIR_ASSERT(args.size() == 1);
    auto arg = caf::get_if<arrow::StringArray>(&*args[0].array);
    TENZIR_ASSERT(arg);
    auto b = arrow::Int64Builder{};
    for (auto i = int64_t{0}; i < arg->length(); ++i) {
      auto name = arg->GetView(i);
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
      (void)b.Append(id);
    }
    return series{int64_type{}, b.Finish().ValueOrDie()};
  }
};

class class_uid final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.ocsf_class_uid";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    TENZIR_ASSERT(args.size() == 1);
    auto arg = caf::get_if<arrow::StringArray>(&*args[0].array);
    TENZIR_ASSERT(arg);
    auto b = arrow::Int64Builder{};
    for (auto i = int64_t{0}; i < arg->length(); ++i) {
      auto name = arg->GetView(i);
      // TODO: Auto-generate a table for this!
      auto id = std::invoke([&] {
        if (name == "Process Activity") {
          return 1007;
        }
        return 0;
      });
      (void)b.Append(id);
    }
    return series{int64_type{}, b.Finish().ValueOrDie()};
  }
};

} // namespace

} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::category_uid)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::class_uid)
