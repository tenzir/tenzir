//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/aws_credentials.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/location.hpp>
#include <tenzir/option.hpp>

#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/sqs/model/Message.h>
#include <folly/executors/IOExecutor.h>

namespace tenzir::plugins::sqs {

/// Returns whether `s` is a queue URL (i.e., starts with `http://` or
/// `https://`) rather than a queue name.
auto is_sqs_queue_url(std::string_view s) -> bool;

/// Extracts the AWS region from a standard SQS queue URL of the form
/// `https://sqs.<region>.amazonaws.com[.cn]/<account>/<queue>`. Returns
/// `None` for non-URLs and for endpoints that do not encode a region
/// (VPC endpoints, LocalStack, custom `AWS_ENDPOINT_URL` overrides).
auto region_from_sqs_url(std::string_view url) -> Option<std::string>;

/// Async SQS client using proxygen for HTTP transport.
///
/// We use proxygen instead of the AWS SDK's built-in HTTP client because
/// the SDK's long-polling calls (e.g., ReceiveMessage with WaitTimeSeconds)
/// cannot be interrupted, which would block pipeline shutdown.
class AsyncSqsQueue {
public:
  /// Creates an async SQS queue client.
  ///
  /// `name` is either a queue name or a full queue URL (`https://...` or
  /// `http://...`). For URLs, the queue URL resolution step is skipped and
  /// the URL's origin is used as the SQS endpoint. For names, the endpoint is
  /// derived from `region` (or `AWS_ENDPOINT_URL[_SQS]`) and the URL is
  /// resolved asynchronously by `init()`.
  ///
  /// This constructor is synchronous; call `init()` before other operations.
  explicit AsyncSqsQueue(
    located<std::string> name, std::chrono::seconds poll_time,
    std::string region, Option<tenzir::resolved_aws_credentials> creds,
    folly::Executor::KeepAlive<folly::IOExecutor> io_executor);

  /// Resolves the queue URL. Must be called before other operations.
  auto init() -> Task<void>;

  /// Receives messages from the queue.
  auto receive_messages(size_t num_messages, std::chrono::seconds poll_time)
    -> Task<Aws::Vector<Aws::SQS::Model::Message>>;

  /// Sends a message to the queue.
  auto send_message(Aws::String data) -> Task<void>;

  /// Deletes a message from the queue.
  auto delete_message(const Aws::SQS::Model::Message& message)
    -> Task<Option<diagnostic>>;

private:
  located<std::string> name_;
  std::string region_;
  std::string endpoint_url_;
  Aws::String url_;
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_;
  std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer_;
  Box<HttpPool> pool_;
};

} // namespace tenzir::plugins::sqs
