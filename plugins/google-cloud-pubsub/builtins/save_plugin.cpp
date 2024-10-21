//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

#include "saver.hpp"

namespace tenzir::plugins::google_cloud_pubsub {
class save_operator final : public crtp_operator<save_operator> {
public:
  save_operator() = default;

  explicit save_operator(saver::args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input,
             operator_control_plane& ctrl) const -> generator<std::monostate> {
    auto instance = saver{args_}.instantiate(ctrl, {});
    if (not instance) {
      co_return;
    }
    for (auto&& chunk : input) {
      (*instance)(std::move(chunk));
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "tql2.save_google_cloud_pubsub";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  saver::args args_;
};

class save_plugin final : public operator_plugin2<save_operator> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = saver::args{};
    auto parser = argument_parser2::operator_("save_google_cloud_pubsub");
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    return std::make_unique<save_operator>(std::move(args));
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::save_plugin)
