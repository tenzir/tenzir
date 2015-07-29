#include <caf/detail/scope_guard.hpp>

#include "vast/event.h"
#include "vast/actor/source/kafka.h"
#include "vast/concept/parseable/parse.h"
#include "vast/concept/parseable/vast/json.h"
#include "vast/concept/convertible/to.h"
#include "vast/concept/printable/print.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/json.h"
#include "vast/util/assert.h"

namespace vast {
namespace source {

kafka::kafka(std::string const& brokers,
             std::string const& topic,
             std::string const& partition,
             std::string const& offset,
             std::string const& compression)
  : base<kafka>{"kafka-source"}
{
  // Parse start offset.
  if (offset == "end")
    start_offset_ = RdKafka::Topic::OFFSET_END;
  else if (offset == "beginning")
    start_offset_ = RdKafka::Topic::OFFSET_BEGINNING;
  else if (offset == "stored")
    start_offset_ = RdKafka::Topic::OFFSET_STORED;
  else
    start_offset_ = std::strtoll(offset.c_str(), nullptr, 10);
  // Parse partition.
  if (partition == "random")
    partition_ = RdKafka::Topic::PARTITION_UA;
  else
    partition_ = std::strtol(partition.c_str(), nullptr, 10);
  // Create configurations.
  auto global_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  auto topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  std::string err;
  // Set brokers.
  auto result = global_conf->set("metadata.broker.list", brokers.c_str(), err);
  if (result != RdKafka::Conf::CONF_OK)
  {
    VAST_ERROR(this, "failed to set brokers:", err);
    return;
  }
  VAST_DEBUG(this, "set brokers to:", brokers);
  // Set compression codec.
  auto comp = compression.empty() ? "none" : compression.c_str();
  result = global_conf->set("compression.codec", comp, err);
  if (result != RdKafka::Conf::CONF_OK)
  {
    VAST_ERROR(this, "failed to set compression:", err);
    return;
  }
  VAST_DEBUG(this, "set compression to:", comp);
  // Setup consumer.
  consumer_ = RdKafka::Consumer::create(global_conf, err);
  if (! consumer_)
  {
    VAST_ERROR(this, "failed to create consumer:", err);
    return;
  }
  VAST_DEBUG(this, "created consumer:", consumer_->name());
  // Setup topic.
  topic_ = RdKafka::Topic::create(consumer_, topic, topic_conf, err);
  if (! topic_)
  {
    VAST_ERROR(this, "failed to create topic:", err);
    return;
  }
  VAST_DEBUG(this, "created topic:", topic);
}

kafka::~kafka()
{
  if (running_)
  {
    VAST_ASSERT(consumer_ != nullptr);
    VAST_ASSERT(topic_ != nullptr);
    consumer_->stop(topic_, partition_);
    consumer_->poll(0); // TODO: understand why the example does this.
  }
  if (consumer_)
    delete consumer_;
  if (topic_)
    delete topic_;
  RdKafka::wait_destroyed(1000); // FIXME: do we really need to wait here?
}

void kafka::set(schema const&)
{
  // TODO
}

schema kafka::sniff()
{
  // TODO
  return {};
}

result<event> kafka::extract()
{
  if (! consumer_)
    return error{"kafka setup failed"};
  if (! running_)
  {
    auto result = consumer_->start(topic_, partition_, start_offset_);
    if (result != RdKafka::ERR_NO_ERROR)
    {
      auto err = RdKafka::err2str(result);
      return error{"failed to start kafka consumer:", err};
    }
    running_ = true;
  }
  auto msg = consumer_->consume(topic_, partition_, 0);
  auto msg_deleter = caf::detail::make_scope_guard([msg] { delete msg; });
  switch (msg->err())
  {
    default:
      return error{"consume failed: ", msg->errstr()};
    case RdKafka::ERR__TIMED_OUT:
      return {}; // Try again next time.
    case RdKafka::ERR__PARTITION_EOF:
      done(true);
      return {};
    case RdKafka::ERR_NO_ERROR:
      {
        //if (msg->key())
        //  VAST_DEBUG(this, "got message key:", *msg->key());
        auto f = static_cast<char const*>(msg->payload());
        auto l = f + static_cast<int>(msg->len());
        json j;
        if (! parse(f, l, j))
        {
          VAST_WARN(this, "failed to parse message:", std::string(f, l));
          return {};
        }
        std::string line;
        printers::json<policy::oneline>(line, j);
        VAST_DEBUG(line);
        // TODO: convert JSON to event.
        return event{};
      }
      break;
  }
}

} // namespace source
} // namespace vast
