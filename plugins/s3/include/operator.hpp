//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/argument_parser.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution_utilities.hpp>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

#include "sts_helpers.hpp"

namespace tenzir::plugins::s3 {

namespace {

struct s3_args {
  bool anonymous = {};
  located<secret> uri;
  std::optional<aws_iam_options> aws_iam;

  template <class Inspector>
  friend auto inspect(Inspector& f, s3_args& x) -> bool {
    return f.object(x).pretty_name("s3_args").fields(
      f.field("anonymous", x.anonymous), f.field("uri", x.uri),
      f.field("aws_iam", x.aws_iam));
  }
};

auto get_options(const s3_args& args, const arrow::util::Uri& uri,
                 const std::optional<resolved_aws_credentials>& resolved_creds)
  -> caf::expected<arrow::fs::S3Options> {
  auto opts = arrow::fs::S3Options::FromUri(uri);
  if (not opts.ok()) {
    return diagnostic::error("failed to parse S3 options: {}",
                             opts.status().ToString())
      .to_error();
  }
  if (args.anonymous) {
    opts->ConfigureAnonymousCredentials();
  } else if (args.aws_iam) {
    const auto has_explicit_creds
      = resolved_creds and not resolved_creds->access_key_id.empty();
    const auto has_role = resolved_creds and not resolved_creds->role.empty();
    const auto has_profile = args.aws_iam->profile.has_value();

    if (has_explicit_creds and has_role) {
      // Explicit credentials + role: use STS to assume role
      auto sts_creds
        = assume_role_with_credentials(*resolved_creds, resolved_creds->role,
                                       args.aws_iam->session_name.value_or(""),
                                       resolved_creds->external_id,
                                       args.aws_iam->region);
      if (not sts_creds) {
        return sts_creds.error();
      }
      opts->ConfigureAccessKey(sts_creds->access_key_id,
                               sts_creds->secret_access_key,
                               sts_creds->session_token);
    } else if (has_explicit_creds) {
      // Explicit credentials only
      opts->ConfigureAccessKey(resolved_creds->access_key_id,
                               resolved_creds->secret_access_key,
                               resolved_creds->session_token);
    } else if (has_profile and has_role) {
      // Profile + role: load profile credentials, then assume role
      auto profile_creds = load_profile_credentials(*args.aws_iam->profile);
      if (not profile_creds) {
        return profile_creds.error();
      }
      auto base_creds = resolved_aws_credentials{
        .access_key_id = profile_creds->access_key_id,
        .secret_access_key = profile_creds->secret_access_key,
        .session_token = profile_creds->session_token,
        .role = {},
        .external_id = {},
      };
      auto sts_creds
        = assume_role_with_credentials(base_creds, resolved_creds->role,
                                       args.aws_iam->session_name.value_or(""),
                                       resolved_creds->external_id,
                                       args.aws_iam->region);
      if (not sts_creds) {
        return sts_creds.error();
      }
      opts->ConfigureAccessKey(sts_creds->access_key_id,
                               sts_creds->secret_access_key,
                               sts_creds->session_token);
    } else if (has_profile) {
      // Profile-based credentials only
      auto profile_creds = load_profile_credentials(*args.aws_iam->profile);
      if (not profile_creds) {
        return profile_creds.error();
      }
      opts->ConfigureAccessKey(profile_creds->access_key_id,
                               profile_creds->secret_access_key,
                               profile_creds->session_token);
    } else if (has_role) {
      // Role assumption with default credentials
      opts->ConfigureAssumeRoleCredentials(
        resolved_creds->role, args.aws_iam->session_name.value_or(""),
        resolved_creds->external_id);
    }
    // Otherwise, use default credential chain (no explicit configuration)
  }
  return opts.MoveValueUnsafe();
}

// We use 2^20 for the upper bound of a chunk size, which exactly matches the
// upper limit defined by execution nodes for transporting events.
// TODO: Get the backpressure-adjusted value at runtime from the execution node.
static constexpr size_t max_chunk_size = 1 << 20;

class s3_loader final : public crtp_operator<s3_loader> {
public:
  s3_loader() = default;

  s3_loader(s3_args args) : args_{std::move(args)} {
  }
  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto& dh = ctrl.diagnostics();
    auto uri = arrow::util::Uri{};
    auto reqs = std::vector<secret_request>{
      make_uri_request(args_.uri, "s3://", uri, dh),
    };
    // Resolve aws_iam credentials/role if provided
    auto resolved_creds = std::optional<resolved_aws_credentials>{};
    if (args_.aws_iam
        and (args_.aws_iam->has_explicit_credentials()
             or args_.aws_iam->role)) {
      resolved_creds.emplace();
      auto aws_reqs = args_.aws_iam->make_secret_requests(*resolved_creds, dh);
      for (auto& r : aws_reqs) {
        reqs.push_back(std::move(r));
      }
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    auto opts = get_options(args_, uri, resolved_creds);
    if (not opts) {
      diagnostic::error(opts.error()).emit(dh);
      co_return;
    }
    auto fs = arrow::fs::S3FileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow S3 filesystem: {}",
                        fs.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(
      fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info: {}",
                        file_info.status().ToStringWithoutContextLines())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto input_stream = fs.ValueUnsafe()->OpenInputStream(*file_info);
    if (not input_stream.ok()) {
      diagnostic::error("failed to open input stream: {}",
                        input_stream.status().ToStringWithoutContextLines())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (not input_stream.ValueUnsafe()->closed()) {
      auto buffer = input_stream.ValueUnsafe()->Read(max_chunk_size);
      if (not input_stream.ok()) {
        diagnostic::error("failed to read from input stream: {}",
                          buffer.status().ToStringWithoutContextLines())
          .primary(args_.uri.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (buffer.ValueUnsafe()->size() == 0) {
        break;
      }
      co_yield chunk::make(buffer.MoveValueUnsafe());
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "load_s3";
  }

  friend auto inspect(auto& f, s3_loader& x) -> bool {
    return f.object(x).pretty_name("s3_loader").fields(f.field("args", x.args_));
  }

private:
  s3_args args_;
};

class s3_saver final : public crtp_operator<s3_saver> {
public:
  s3_saver() = default;

  s3_saver(s3_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto& dh = ctrl.diagnostics();
    auto uri = arrow::util::Uri{};
    auto reqs = std::vector<secret_request>{
      make_uri_request(args_.uri, "s3://", uri, dh),
    };
    // Resolve aws_iam credentials/role if provided
    auto resolved_creds = std::optional<resolved_aws_credentials>{};
    if (args_.aws_iam
        and (args_.aws_iam->has_explicit_credentials()
             or args_.aws_iam->role)) {
      resolved_creds.emplace();
      auto aws_reqs = args_.aws_iam->make_secret_requests(*resolved_creds, dh);
      for (auto& r : aws_reqs) {
        reqs.push_back(std::move(r));
      }
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    auto opts = get_options(args_, uri, resolved_creds);
    if (not opts) {
      diagnostic::error(opts.error()).emit(dh);
      co_return;
    }
    auto fs = arrow::fs::S3FileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow S3 "
                        "filesystem: {}",
                        fs.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return;
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(
      fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info: {}",
                        file_info.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return;
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      diagnostic::error("failed to open output stream: {}",
                        output_stream.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return;
    }
    auto stream_guard
      = detail::scope_guard([this, &dh, output_stream]() noexcept {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not status.ok()) {
            diagnostic::error("failed to close stream: {}",
                              status.ToStringWithoutContextLines())
              .primary(args_.uri.source)
              .emit(dh);
          }
        });
    for (const auto& chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      auto status
        = output_stream.ValueUnsafe()->Write(chunk->data(), chunk->size());
      if (not status.ok()) {
        diagnostic::error("failed to write to stream: {}",
                          status.ToStringWithoutContextLines())
          .primary(args_.uri.source)
          .emit(dh);
        co_return;
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "save_s3";
  }

  friend auto inspect(auto& f, s3_saver& x) -> bool {
    return f.object(x).pretty_name("s3_saver").fields(f.field("args", x.args_));
  }

private:
  s3_args args_;
};
} // namespace
} // namespace tenzir::plugins::s3
