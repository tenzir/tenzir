//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/eval_impl.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::misc {

namespace {

class type_id final : public tql2::function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.type_id";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = series{};
    auto success
      = function_argument_parser{"type_id"}.add(arg, "<value>").parse(inv, dh);
    if (not success) {
      return series::null(string_type{}, inv.length);
    }
    auto type_id = arg.type.make_fingerprint();
    auto b = arrow::StringBuilder{};
    // TODO
    (void)b.Reserve(inv.length);
    for (auto i = int64_t{0}; i < inv.length; ++i) {
      (void)b.Append(type_id);
    }
    return {string_type{}, tql2::finish(b)};
  }
};

} // namespace

} // namespace tenzir::plugins::misc

TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::type_id)
