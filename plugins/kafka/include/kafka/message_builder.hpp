//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/panic.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/type.hpp>

#include <arrow/record_batch.h>
#include <librdkafka/rdkafkacpp.h>

#include <memory>

namespace tenzir::plugins::kafka {

/// Returns the schema for Kafka message table slices.
inline auto kafka_message_schema() -> const type& {
  static const auto schema = type{
    "tenzir.kafka",
    record_type{
      {"message", string_type{}},
    },
  };
  return schema;
}

/// Builder for converting Kafka messages into table slices.
class KafkaMessageBuilder {
public:
  KafkaMessageBuilder()
    : arrow_schema_{kafka_message_schema().to_arrow_schema()},
      builder_{string_type::make_arrow_builder(arrow_memory_pool())} {
  }

  /// Appends a Kafka message payload to the builder.
  auto append(const RdKafka::Message& message) -> void {
    auto status = builder_->Append(reinterpret_cast<const char*>(message.payload()),
                                   tenzir::detail::narrow<int32_t>(message.len()));
    if (not status.ok()) {
      panic("failed to append kafka payload: {}", status.ToString());
    }
  }

  /// Returns the number of messages currently in the builder.
  [[nodiscard]] auto length() const -> int64_t {
    return builder_->length();
  }

  /// Returns true if the builder has no messages.
  [[nodiscard]] auto empty() const -> bool {
    return builder_->length() == 0;
  }

  /// Finishes building and returns a table slice.
  [[nodiscard]] auto finish() -> table_slice {
    const auto length = builder_->length();
    return table_slice{
      arrow::RecordBatch::Make(arrow_schema_, length, {tenzir::finish(*builder_)}),
      kafka_message_schema(),
    };
  }

private:
  std::shared_ptr<arrow::Schema> arrow_schema_;
  std::shared_ptr<arrow::StringBuilder> builder_;
};

} // namespace tenzir::plugins::kafka
