#include <vast/ingest/ingestor.h>

#include <vast/ingest/bro_event_source.h>
#include <vast/ingest/exception.h>
#include <vast/ingest/id_tracker.h>
#include <vast/ingest/reader.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

ingestor::ingestor(cppa::actor_ptr archive, std::string const& id_file)
  : archive_(archive)
{
  // FIXME: make batch size configurable.
  size_t batch_size = 1000;

  LOG(verbose, ingest) << "spawning ingestor @" << id();

  using namespace cppa;
  tracker_ = spawn<id_tracker>(id_file);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](std::string const& host,
                                               unsigned port)
      {
        bro_event_source_ = spawn<bro_event_source>(tracker_, archive_);
        send(bro_event_source_, atom("bind"), host, port);
      },
      on(atom("subscribe"), arg_match) >> [=](std::string const& event_name)
      {
        bro_event_source_ << last_dequeued();
      },
      on(atom("ingest"), "bro1conn", arg_match) >> [=](std::string const& filename)
      {
        files_.emplace(file_type::bro1conn, filename);
        send(self, atom("read"));
      },
      on(atom("ingest"), "bro2", arg_match) >> [=](std::string const& filename)
      {
        files_.emplace(file_type::bro2, filename);
        send(self, atom("read"));
      },
      on(atom("ingest"), val<std::string>, arg_match) >> [=](std::string const&)
      {
        LOG(error, ingest) << "invalid ingestion file type";
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
          case file_type::bro1conn:
            reader_ = spawn<bro_15_conn_reader>(self, tracker_, archive_,
                                                file.second);
            break;
          case file_type::bro2:
            reader_ = spawn<bro_reader>(self, tracker_, archive_, file.second);
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
        tracker_ << last_dequeued();

        quit();
        LOG(verbose, ingest) << "ingestor @" << id() << " terminated";
      });
}

} // namespace ingest
} // namespace vast
