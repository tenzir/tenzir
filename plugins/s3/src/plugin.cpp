//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

#include "operator.hpp"

namespace tenzir::plugins::s3 {

namespace {

class plugin final : public virtual loader_plugin<s3_loader>,
                     public virtual saver_plugin<s3_saver> {
public:
  ~plugin() noexcept override {
    const auto finalized = arrow::fs::FinalizeS3();
    TENZIR_ASSERT(finalized.ok(), finalized.ToString().c_str());
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)global_config;
    auto initialized = arrow::fs::EnsureS3Initialized();
    if (not initialized.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to initialize Arrow S3 "
                                         "functionality: {}",
                                         initialized.ToString()));
    }
    if (plugin_config.empty()) {
      return {};
    }
    config_.emplace();
    for (const auto& [key, value] : plugin_config) {
#define X(opt, var)                                                            \
  if (key == (opt)) {                                                          \
    if (value == data{}) {                                                     \
      continue;                                                                \
    }                                                                          \
    if (const auto* str = try_as<std::string>(&value)) {                       \
      config_->var = *str;                                                     \
      continue;                                                                \
    }                                                                          \
    return diagnostic::error("invalid S3 configuration: {} must be a string",  \
                             key)                                              \
      .note("{} is configured as {}", key, value)                              \
      .to_error();                                                             \
  }
      X("access-key", access_key)
      X("secret-key", secret_key)
      X("session-token", session_token)
#undef X
      return diagnostic::error(
               "invalid S3 configuration: unrecognized option {}", key)
        .note("{} is configured as {}", key, value)
        .to_error();
    }
    return {};
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = s3_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("s3://")) {
      args.uri.inner = fmt::format("s3://{}", args.uri.inner);
    }
    args.config = config_;
    return std::make_unique<s3_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = s3_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("s3://")) {
      args.uri.inner = fmt::format("s3://{}", args.uri.inner);
    }
    args.config = config_;
    return std::make_unique<s3_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "s3";
  }

  std::optional<s3_config> config_ = {};
};

} // namespace
} // namespace tenzir::plugins::s3
TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::plugin)
