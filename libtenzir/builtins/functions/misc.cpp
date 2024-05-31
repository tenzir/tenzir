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
    if (inv.args.size() != 1) {
      diagnostic::error("`type_id` expects exactly one argument")
        .primary(inv.self.get_location())
        .emit(dh);
      return series::null(string_type{}, inv.length);
    }
    auto type_id = inv.args[0].type.make_fingerprint();
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
