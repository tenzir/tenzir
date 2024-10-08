//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

#include "loader.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

class load_operator final : public crtp_operator<load_operator> {
public:
  load_operator() = default;

  explicit load_operator(loader::args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto instance = loader{args_}.instantiate(ctrl);
    if (not instance) {
      co_return;
    }
    for (auto&& chunk : *instance) {
      co_yield std::move(chunk);
    }
  }

  auto name() const -> std::string override {
    return "tql2.load_google_cloud_pubsub";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  loader::args args_;
};

class load_plugin final : public operator_plugin2<load_operator> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = loader::args{};
    auto parser = argument_parser2::operator_("load_google_cloud_pubsub");
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    return std::make_unique<load_operator>(std::move(args));
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::load_plugin)
