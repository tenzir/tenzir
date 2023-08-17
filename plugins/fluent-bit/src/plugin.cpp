//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>

#include <fluent-bit/fluent-bit-minimal.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::fluentbit {

namespace {

struct operator_args {
  located<std::vector<std::string>> args;

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("args", x.args));
  }
};

class fluent_bit_operator final : public crtp_operator<fluent_bit_operator> {
public:
  fluent_bit_operator() = default;

  explicit fluent_bit_operator(operator_args args)
    : args_{std::move(args)}, ctx_{flb_create()} {
    TENZIR_ASSERT(ctx_ != nullptr);
  }

  ~fluent_bit_operator() final {
    flb_destroy(ctx_);
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: implement
  }

  auto name() const -> std::string override {
    return "fluentbit";
  }

  auto detached() const -> bool override {
    return false;
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

  friend auto inspect(auto& f, fluent_bit_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_;
  flb_ctx_t* ctx_;
};

class plugin final : public operator_plugin<fluent_bit_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = operator_args{};
    parser.parse(p);
    return std::make_unique<fluent_bit_operator>(std::move(args));
  }

  auto name() const -> std::string override {
    return "fluentbit";
  }
};

} // namespace

} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::plugin)
