//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

#include "saver.hpp"

namespace tenzir::plugins::abs {
class save_abs_operator final : public crtp_operator<save_abs_operator> {
public:
  save_abs_operator() = default;

  explicit save_abs_operator(located<std::string> uri) : uri_{std::move(uri)} {
  }

  auto
  operator()(generator<chunk_ptr> input,
             operator_control_plane& ctrl) const -> generator<std::monostate> {
    auto saver = abs_saver{uri_};
    auto instance = saver.instantiate(ctrl, {});
    if (not instance) {
      co_return;
    }
    for (auto&& chunk : input) {
      (*instance)(std::move(chunk));
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "tql2.save_azure_blob_storage";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_abs_operator& x) -> bool {
    return f.apply(x.uri_);
  }

private:
  located<std::string> uri_;
};

class save_abs_plugin final : public operator_plugin2<save_abs_operator> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto uri = located<std::string>{};
    TRY(argument_parser2::operator_("save_azure_blob_storage")
          .add("uri", uri)
          .parse(inv, ctx));
    return std::make_unique<save_abs_operator>(std::move(uri));
  }
};

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::save_abs_plugin)
