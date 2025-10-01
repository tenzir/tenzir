//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/consumer.hpp"

#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>

#include <fmt/format.h>

namespace tenzir::plugins::kafka {

auto consumer::make(configuration config) -> caf::expected<consumer> {
  consumer result;
  std::string error;
  result.consumer_.reset(
    RdKafka::KafkaConsumer::create(config.conf_.get(), error));
  if (! result.consumer_) {
    return caf::make_error(ec::unspecified, error);
  }
  result.config_ = std::move(config);
  return result;
}

auto consumer::subscribe(const std::vector<std::string>& topics) -> caf::error {
  auto result = consumer_->subscribe(topics);
  if (result != RdKafka::ERR_NO_ERROR) {
    return caf::make_error(ec::unspecified, RdKafka::err2str(result));
  }
  return {};
}

auto consumer::consume_raw(std::chrono::milliseconds timeout)
  -> std::shared_ptr<RdKafka::Message> {
  auto ms = detail::narrow_cast<int>(timeout.count());
  return std::shared_ptr<RdKafka::Message>{consumer_->consume(ms)};
}

auto consumer::commit(RdKafka::Message* message, diagnostic_handler& dh,
                      location loc) -> failure_or<void> {
  auto result = consumer_->commitSync(message);
  if (result != RdKafka::ERR_NO_ERROR) {
    diagnostic::error("failed to commit offset: {}", RdKafka::err2str(result))
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto consumer::get_partition_count(const std::string& topic)
  -> caf::expected<size_t> {
  RdKafka::Metadata* metadata = nullptr;
  auto err = consumer_->metadata(false, nullptr, &metadata, 5000);
  if (err != RdKafka::ERR_NO_ERROR) {
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to get metadata: {}",
                                       RdKafka::err2str(err)));
  }

  std::unique_ptr<RdKafka::Metadata> metadata_guard(metadata);

  const auto& topics = *metadata->topics();
  for (const auto& topic_meta : topics) {
    if (topic_meta->topic() == topic) {
      if (topic_meta->err() != RdKafka::ERR_NO_ERROR) {
        return caf::make_error(
          ec::unspecified,
          fmt::format("topic error: {}", RdKafka::err2str(topic_meta->err())));
      }
      return topic_meta->partitions()->size();
    }
  }

  return caf::make_error(ec::unspecified,
                         fmt::format("topic '{}' not found", topic));
}

} // namespace tenzir::plugins::kafka
