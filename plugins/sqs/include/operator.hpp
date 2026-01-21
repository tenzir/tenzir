//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/env.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/series_builder.hpp>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/Outcome.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>

#include <string_view>

using namespace std::chrono_literals;

namespace tenzir::plugins::sqs {

namespace {

/// The default poll time.
constexpr auto default_poll_time = 3s;
static_assert(default_poll_time >= 1s && default_poll_time <= 20s);

auto to_aws_string(chunk_ptr chunk) -> Aws::String {
  const auto* ptr = reinterpret_cast<Aws::String::const_pointer>(chunk->data());
  auto size = chunk->size();
  return {ptr, size};
}

/// Resolved AWS credentials for SQS.
struct resolved_aws_credentials {
  std::string access_key;
  std::string secret_key;
  std::string session_token;
};

/// AWS IAM authentication options for SQS.
struct aws_iam_options {
  std::string region;
  std::optional<std::string> role;
  std::optional<std::string> session_name;
  std::optional<std::string> ext_id;
  std::optional<secret> access_key;
  std::optional<secret> secret_key;
  std::optional<secret> session_token;
  location loc;

  friend auto inspect(auto& f, aws_iam_options& x) -> bool {
    return f.object(x).fields(
      f.field("region", x.region), f.field("role", x.role),
      f.field("session_name", x.session_name), f.field("ext_id", x.ext_id),
      f.field("access_key", x.access_key), f.field("secret_key", x.secret_key),
      f.field("session_token", x.session_token), f.field("loc", x.loc));
  }

  static auto from_record(located<record> config, diagnostic_handler& dh)
    -> failure_or<aws_iam_options> {
    constexpr auto known = std::array{
      "region",        "assume_role",       "session_name",  "external_id",
      "access_key_id", "secret_access_key", "session_token",
    };
    const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
      return std::ranges::find(known, x.first) == std::ranges::end(known);
    });
    if (unknown != std::ranges::end(config.inner)) {
      diagnostic::error("unknown key '{}' in config", (*unknown).first)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    const auto assign_non_empty_string
      = [&](std::string_view key, auto&& x) -> failure_or<void> {
      if (auto it = config.inner.find(key); it != config.inner.end()) {
        auto* extracted = try_as<std::string>(it->second.get_data());
        if (not extracted) {
          diagnostic::error("'{}' must be a `string`", key)
            .primary(config)
            .emit(dh);
          return failure::promise();
        }
        if (extracted->empty()) {
          diagnostic::error("'{}' must not be empty", key)
            .primary(config)
            .emit(dh);
          return failure::promise();
        }
        x = std::move(*extracted);
      }
      return {};
    };
    const auto assign_secret =
      [&](std::string_view key, std::optional<secret>& x) -> failure_or<void> {
      if (auto it = config.inner.find(key); it != config.inner.end()) {
        if (auto* s = try_as<secret>(it->second.get_data())) {
          x = std::move(*s);
        } else if (auto* str = try_as<std::string>(it->second.get_data())) {
          // Allow plain strings as well, convert to literal secret
          x = secret::make_literal(std::move(*str));
        } else {
          diagnostic::error("'{}' must be a `string` or `secret`", key)
            .primary(config)
            .emit(dh);
          return failure::promise();
        }
      }
      return {};
    };
    auto opts = aws_iam_options{};
    opts.loc = config.source;
    TRY(assign_non_empty_string("region", opts.region));
    TRY(assign_non_empty_string("assume_role", opts.role));
    TRY(assign_non_empty_string("session_name", opts.session_name));
    TRY(assign_non_empty_string("external_id", opts.ext_id));
    TRY(assign_secret("access_key_id", opts.access_key));
    TRY(assign_secret("secret_access_key", opts.secret_key));
    TRY(assign_secret("session_token", opts.session_token));
    // Validate that access_key_id and secret_access_key are specified together
    if (opts.access_key.has_value() xor opts.secret_key.has_value()) {
      diagnostic::error(
        "`access_key_id` and `secret_access_key` must be specified together")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    // Validate that session_token requires access_key_id
    if (opts.session_token and not opts.access_key) {
      diagnostic::error("`session_token` specified without `access_key_id`")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    return opts;
  }
};

/// A wrapper around SQS.
class sqs_queue {
public:
  explicit sqs_queue(located<std::string> name, std::chrono::seconds poll_time,
                     std::optional<std::string> region = std::nullopt,
                     std::optional<std::string> role = std::nullopt,
                     std::optional<std::string> role_session_name
                     = std::nullopt,
                     std::optional<std::string> role_external_id = std::nullopt,
                     std::optional<resolved_aws_credentials> creds
                     = std::nullopt)
    : name_{std::move(name)} {
    auto config = Aws::Client::ClientConfiguration{};
    // Set the region if provided.
    if (region) {
      config.region = *region;
      TENZIR_VERBOSE("[sqs] using region {}", *region);
    }
    // TODO: remove this after upgrading to Arrow 15, as it's no longer
    // necessary. This is just a bandaid fix to make an old version of the SDK
    // honer the AWS_ENDPOINT_URL variable.
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL")) {
      config.endpointOverride = *endpoint_url;
    }
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL_SQS")) {
      config.endpointOverride = *endpoint_url;
    }
    // Proxy settings should be honored.
    // This is documented as "Off by default for legacy reasons" at
    // https://sdk.amazonaws.com/cpp/api/LATEST/aws-cpp-sdk-core/html/struct_aws_1_1_client_1_1_client_configuration.html#a0197eb33dffeb845f98d14e5058921c1
    config.allowSystemProxy = true;
    // The HTTP request timeout should be longer than the poll time. The overall
    // request timeout, including retries, should be even larger.
    auto poll_time_ms = std::chrono::milliseconds{poll_time};
    auto extra_time_for_http_request = 2s;
    auto extra_time_for_retries_and_backoff = 2s;
    auto http_request_timeout = poll_time_ms + extra_time_for_http_request;
    auto request_timeout
      = http_request_timeout + extra_time_for_retries_and_backoff;
    config.httpRequestTimeoutMs = long{http_request_timeout.count()};
    config.requestTimeoutMs = long{request_timeout.count()};
    // Create the credentials provider based on options.
    auto credentials = std::invoke(
      [&]() -> std::shared_ptr<Aws::Auth::AWSCredentialsProvider> {
        // Determine base credentials provider: explicit or default chain.
        auto base_credentials
          = std::shared_ptr<Aws::Auth::AWSCredentialsProvider>{};
        if (creds) {
          base_credentials
            = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
              creds->access_key, creds->secret_key, creds->session_token);
        } else {
          base_credentials
            = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
        }
        // If role assumption is requested, call AssumeRole and use the
        // resulting credentials.
        if (role) {
          TENZIR_VERBOSE("[sqs] assuming role {}", *role);
          // Create an STS client configuration (inherits region from main cfg).
          auto sts_config = Aws::Client::ClientConfiguration{};
          sts_config.region = config.region;
          sts_config.allowSystemProxy = true;
          // Create STS client using the base credentials.
          auto sts_client
            = Aws::STS::STSClient{base_credentials, nullptr, sts_config};
          // Call AssumeRole to get temporary credentials.
          auto session = role_session_name.value_or("tenzir-session");
          auto ext = role_external_id.value_or("");
          auto request = Aws::STS::Model::AssumeRoleRequest{};
          request.SetRoleArn(*role);
          request.SetRoleSessionName(session);
          if (not ext.empty()) {
            request.SetExternalId(ext);
          }
          auto outcome = sts_client.AssumeRole(request);
          if (not outcome.IsSuccess()) {
            const auto& err = outcome.GetError();
            diagnostic::error("failed to assume role")
              .note("{}", err.GetMessage())
              .note("error code: {}", err.GetExceptionName())
              .throw_();
          }
          const auto& assumed_creds = outcome.GetResult().GetCredentials();
          return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
            assumed_creds.GetAccessKeyId(), assumed_creds.GetSecretAccessKey(),
            assumed_creds.GetSessionToken());
        }
        return base_credentials;
      });
    // Create the client with the configuration and credentials provider.
    client_ = Aws::SQS::SQSClient{credentials, nullptr, config};
    // Get the queue URL.
    url_ = queue_url();
  }

  /// Receives N messages from the queue.
  auto receive_messages(size_t num_messages, std::chrono::seconds poll_time) {
    TENZIR_ASSERT(num_messages > 0);
    TENZIR_ASSERT(num_messages <= 10);
    TENZIR_DEBUG("receiving {} messages from {}", num_messages, url_);
    auto request = Aws::SQS::Model::ReceiveMessageRequest{};
    request.SetQueueUrl(url_);
    // TODO: adjust once we have limit pushdown. We still can lose messages
    // because we eagerly fetch them without waiting for ACKs from downstream.
    request.SetMaxNumberOfMessages(detail::narrow_cast<int>(num_messages));
    request.SetWaitTimeSeconds(detail::narrow_cast<int>(poll_time.count()));
    auto outcome = client_.ReceiveMessage(request);
    if (not outcome.IsSuccess()) {
      diagnostic::error("failed receiving message from SQS queue")
        .primary(name_.source)
        .note("URL: {}", url_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
    }
    return outcome.GetResult().GetMessages();
  }

  /// Sends a message to the queue.
  auto send_message(Aws::String data) -> void {
    TENZIR_DEBUG("sending {}-byte message to SQS queue '{}'", data.size(),
                 name_.inner);
    auto request = Aws::SQS::Model::SendMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetMessageBody(std::move(data));
    auto outcome = client_.SendMessage(request);
    if (not outcome.IsSuccess()) {
      diagnostic::error("failed to send message to SQS queue")
        .primary(name_.source)
        .note("URL: {}", url_)
        .note("{}", outcome.GetError().GetMessage())
        .throw_();
    }
  }

  /// Deletes a message from the queue.
  auto delete_message(const auto& message) -> std::optional<diagnostic> {
    TENZIR_DEBUG("deleting message {}", message.GetMessageId());
    auto request = Aws::SQS::Model::DeleteMessageRequest{};
    request.SetQueueUrl(url_);
    request.SetReceiptHandle(message.GetReceiptHandle());
    auto outcome = client_.DeleteMessage(request);
    if (not outcome.IsSuccess()) {
      return diagnostic::warning("failed to delete message from SQS queue")
        .primary(name_.source)
        .note("URL: {}", url_)
        .note("message ID: {}", message.GetMessageId())
        .note("receipt handle: {}", message.GetReceiptHandle())
        .done();
    }
    return std::nullopt;
  }

private:
  auto queue_url() -> Aws::String {
    TENZIR_DEBUG("retrieving URL for queue {}", name_.inner);
    auto request = Aws::SQS::Model::GetQueueUrlRequest{};
    request.SetQueueName(name_.inner);
    auto outcome = client_.GetQueueUrl(request);
    if (not outcome.IsSuccess()) {
      const auto& err = outcome.GetError();
      diagnostic::error("failed to get URL for SQS queue")
        .primary(name_.source)
        .note("{}", err.GetMessage())
        .note("error code: {}", err.GetExceptionName())
        .hint("ensure that $AWS_ENDPOINT_URL points to valid endpoint")
        .throw_();
    }
    return outcome.GetResult().GetQueueUrl();
  }

  located<std::string> name_;
  Aws::String url_;
  Aws::SQS::SQSClient client_;
};

struct connector_args {
  located<std::string> queue;
  std::optional<located<std::chrono::seconds>> poll_time;
  std::optional<aws_iam_options> aws;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sqs.connector_args")
      .fields(f.field("queue", x.queue), f.field("poll_time", x.poll_time),
              f.field("aws", x.aws));
  }
};

class sqs_loader final : public crtp_operator<sqs_loader> {
public:
  sqs_loader() = default;

  explicit sqs_loader(connector_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto& dh = ctrl.diagnostics();
    // Resolve secrets if explicit credentials are provided.
    auto resolved_creds = std::optional<resolved_aws_credentials>{};
    if (args_.aws and args_.aws->access_key) {
      resolved_creds = resolved_aws_credentials{};
      auto requests = std::vector<secret_request>{};
      requests.emplace_back(
        make_secret_request("access_key", *args_.aws->access_key,
                            args_.aws->loc, resolved_creds->access_key, dh));
      requests.emplace_back(
        make_secret_request("secret_key", *args_.aws->secret_key,
                            args_.aws->loc, resolved_creds->secret_key, dh));
      if (args_.aws->session_token) {
        requests.emplace_back(make_secret_request(
          "session_token", *args_.aws->session_token, args_.aws->loc,
          resolved_creds->session_token, dh));
      }
      co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    }
    try {
      auto poll_time
        = args_.poll_time ? args_.poll_time->inner : default_poll_time;
      auto region
        = args_.aws ? std::make_optional(args_.aws->region) : std::nullopt;
      auto role = args_.aws ? args_.aws->role : std::nullopt;
      auto session_name = args_.aws ? args_.aws->session_name : std::nullopt;
      auto ext_id = args_.aws ? args_.aws->ext_id : std::nullopt;
      auto queue = sqs_queue{args_.queue,  poll_time, region,        role,
                             session_name, ext_id,    resolved_creds};
      co_yield {};
      while (true) {
        constexpr auto num_messages = size_t{1};
        auto messages = queue.receive_messages(num_messages, poll_time);
        if (messages.empty()) {
          co_yield {};
        } else {
          for (const auto& message : messages) {
            TENZIR_DEBUG("got message {} ({})", message.GetMessageId(),
                         message.GetReceiptHandle());
            // It seems there's no way to get the Aws::String out of the
            // message to move it into the chunk. So we have to copy it.
            const auto& body = message.GetBody();
            auto str = std::string_view{body.data(), body.size()};
            co_yield chunk::copy(str);
            queue.delete_message(message);
          }
        }
      }
    } catch (diagnostic& d) {
      dh.emit(std::move(d));
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
    return "load_sqs";
  }

  friend auto inspect(auto& f, sqs_loader& x) -> bool {
    return f.object(x)
      .pretty_name("sqs_loader")
      .fields(f.field("args", x.args_));
  }

private:
  connector_args args_;
};

class sqs_saver final : public crtp_operator<sqs_saver> {
public:
  sqs_saver() = default;

  sqs_saver(connector_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto& dh = ctrl.diagnostics();
    // Resolve secrets if explicit credentials are provided.
    auto resolved_creds = std::optional<resolved_aws_credentials>{};
    if (args_.aws and args_.aws->access_key) {
      resolved_creds = resolved_aws_credentials{};
      auto requests = std::vector<secret_request>{};
      requests.emplace_back(
        make_secret_request("access_key", *args_.aws->access_key,
                            args_.aws->loc, resolved_creds->access_key, dh));
      requests.emplace_back(
        make_secret_request("secret_key", *args_.aws->secret_key,
                            args_.aws->loc, resolved_creds->secret_key, dh));
      if (args_.aws->session_token) {
        requests.emplace_back(make_secret_request(
          "session_token", *args_.aws->session_token, args_.aws->loc,
          resolved_creds->session_token, dh));
      }
      co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    }
    auto poll_time
      = args_.poll_time ? args_.poll_time->inner : default_poll_time;
    auto queue = std::shared_ptr<sqs_queue>{};
    try {
      auto region
        = args_.aws ? std::make_optional(args_.aws->region) : std::nullopt;
      auto role = args_.aws ? args_.aws->role : std::nullopt;
      auto session_name = args_.aws ? args_.aws->session_name : std::nullopt;
      auto ext_id = args_.aws ? args_.aws->ext_id : std::nullopt;
      queue = std::make_shared<sqs_queue>(args_.queue, poll_time, region, role,
                                          session_name, ext_id, resolved_creds);
    } catch (diagnostic& d) {
      dh.emit(std::move(d));
    }
    for (auto chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      try {
        queue->send_message(to_aws_string(std::move(chunk)));
      } catch (diagnostic& d) {
        dh.emit(std::move(d));
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
    return "save_sqs";
  }

  friend auto inspect(auto& f, sqs_saver& x) -> bool {
    return f.object(x).pretty_name("sqs_saver").fields(f.field("args", x.args_));
  }

private:
  connector_args args_;
};
} // namespace
} // namespace tenzir::plugins::sqs
