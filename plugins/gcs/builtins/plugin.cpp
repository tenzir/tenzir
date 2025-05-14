//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/argument_parser2.hpp"

#include "operator.hpp"

namespace tenzir::plugins::gcs {
namespace {

struct load_gcs : public operator_plugin2<gcs_loader> {
  load_gcs() = default;

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = gcs_args{};
    TRY(argument_parser2::operator_(name())
          .positional("uri", args.uri)
          .named("anonymous", args.anonymous)
          .parse(inv, ctx));
    if (not args.uri.inner.starts_with("gs://")) {
      args.uri.inner = fmt::format("gs://{}", args.uri.inner);
    }
    return std::make_unique<gcs_loader>(std::move(args));
  }

  auto load_properties() const -> load_properties_t override {
    return {.schemes = {"gs"}};
  }
};

struct save_gcs : public operator_plugin2<gcs_saver> {
  save_gcs() = default;

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = gcs_args{};
    TRY(argument_parser2::operator_(name())
          .positional("uri", args.uri)
          .named("anonymous", args.anonymous)
          .parse(inv, ctx));
    if (not args.uri.inner.starts_with("gs://")) {
      args.uri.inner = fmt::format("gs://{}", args.uri.inner);
    }
    return std::make_unique<gcs_saver>(std::move(args));
  }

  auto save_properties() const -> save_properties_t override {
    return {.schemes = {"gs"}};
  }
};

} // namespace
} // namespace tenzir::plugins::gcs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::load_gcs)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::save_gcs)
