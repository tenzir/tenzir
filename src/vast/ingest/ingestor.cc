#include <vast/ingest/ingestor.h>

#include <vast/comm/bro_event_source.h>
#include <vast/ingest/exception.h>
#include <vast/ingest/reader.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

ingestor::ingestor(cppa::actor_ptr archive)
  : archive_(archive)
{
  // FIXME: make batch size configurable.
  size_t batch_size = 50000;

  LOG(verbose, ingest) << "spawning ingestor @" << id();
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
      on(atom("ingest"), "bro1", arg_match) >> [=](std::string const& filename)
      {
        files_.emplace(file_type::bro1, filename);
        send(self, atom("read"));
      },
      on(atom("ingest"), "bro2", arg_match) >> [=](std::string const& filename)
      {
        files_.emplace(file_type::bro2, filename);
        send(self, atom("read"));
      },
      on(atom("read")) >> [=]
      {
        if (files_.empty() || reader_)
          return;

        auto& file = files_.front();
        files_.pop();

        switch (file.first)
        {
          default:
            throw exception("unsupport file type");
          case file_type::bro1:
            reader_ = spawn<bro_15_conn_reader>(archive_, file.second);
            break;
          case file_type::bro2:
            reader_ = spawn<bro_reader>(archive_, file.second);
            break;
        }

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
