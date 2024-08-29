//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/os.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::sockets {

namespace {

class sockets_operator final : public crtp_operator<sockets_operator> {
public:
  sockets_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto system = os::make();
    if (not system) {
      diagnostic::error("failed to create OS shim").emit(ctrl.diagnostics());
      co_return;
    }
    co_yield system->sockets();
  }

  auto name() const -> std::string override {
    return "sockets";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, sockets_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sockets.sockets_operator")
      .fields();
  }
};

class plugin final : public virtual operator_plugin<sockets_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("sockets").parse(inv, ctx).ignore();
    return std::make_unique<sockets_operator>();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"sockets", "https://docs.tenzir.com/"
                                             "operators/sockets"};
    parser.parse(p);
    return std::make_unique<sockets_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::sockets

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sockets::plugin)
