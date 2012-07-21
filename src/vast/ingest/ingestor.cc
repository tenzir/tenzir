#include <vast/ingest/ingestor.h>

#include <vast/comm/bro_event_source.h>
#include <vast/ingest/reader.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

ingestor::ingestor(cppa::actor_ptr archive)
  : archive_(archive)
{
  // FIXME: make batch size configurable.
  size_t batch_size = 500;

  LOG(verbose, core) << "spawning ingestor @" << id();
  using namespace cppa;
  init_state = (
      on(atom("initialize"), arg_match) >> [=](std::string const& host,
                                               unsigned port)
      {
        bro_event_source_ = spawn<comm::bro_event_source>(archive_);
        send(bro_event_source_, atom("bind"), host, port);
      },
      on(atom("subscribe"), arg_match) >> [=](std::string const& event_name)
      {
        bro_event_source_ << last_dequeued();
      },
      on(atom("ingest"), arg_match) >> [=](std::string const& filename)
      {
        files_.push(filename);
        send(self, atom("read"));
      },
      on(atom("read")) >> [=]
      {
        if (files_.empty() || reader_)
          return;

        auto& filename = files_.front();
        files_.pop();

        reader_ = spawn<bro_15_conn_reader>(archive_, filename);
        send(reader_, atom("extract"), batch_size);
      },
      on(atom("reader"), atom("ack")) >> [=]
      {
        assert(last_sender() == reader_);
        send(last_sender(), atom("extract"), batch_size);
      },
      on(atom("reader"), atom("done")) >> [=]
      {
        assert(last_sender() == reader_);
        send(reader_, atom("shutdown"));

        reader_ = nullptr;
        send(self, atom("read"));
      },
      on(atom("shutdown")) >> [=]
      {
        bro_event_source_ << last_dequeued();
        if (reader_)
        {
          reader_ << last_dequeued();
          LOG(verbose, ingest)
            << "waiting for reader @" << reader_->id()
            << " to process last batch";
        }

        quit();
        LOG(verbose, ingest) << "ingestor @" << id() << " terminated";
      });
}

} // namespace ingest
} // namespace vast
