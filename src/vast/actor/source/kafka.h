#ifndef VAST_ACTOR_SOURCE_KAFKA_H
#define VAST_ACTOR_SOURCE_KAFKA_H

#include <cstdint>

#include <librdkafka/rdkafkacpp.h>

#include "vast/actor/source/base.h"

namespace vast {

class schema;

namespace source {

/// A source that consumes events from a Kafka broker.
class kafka : public base<kafka>
{
public:
  /// Constructs a Kafka source.
  /// @param brokers The endpoint of the brokers. Format:
  ///                `host1:port1,host2:port2,...`.
  /// @param topic Topic to fetch
  /// @param partition The partition to use. Allowed values are numeric or
  ///                  `random`.
  /// @param offset The offset where to start consuming messages. Allowed
  ///               values are numeric, `end`, `beginning`, `stored`.
  /// @param compression The compression codec to use. Allowed values are
  ///                    `none`, `gzip`, or `snappy`.
  kafka(std::string const& brokers,
       std::string const& topic,
       std::string const& partition,
       std::string const& offset = "beginning",
       std::string const& compression = "");

  ~kafka();

  result<event> extract();

  // FIXME: remove
  void set(schema const&);
  schema sniff();

private:
  RdKafka::Consumer* consumer_;
  RdKafka::Topic* topic_;
  int32_t partition_ = RdKafka::Topic::PARTITION_UA;
  int64_t start_offset_ = RdKafka::Topic::OFFSET_BEGINNING;
  bool running_ = false;
};

} // namespace source
} // namespace vast

#endif
