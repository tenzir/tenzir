#include "vast/ingestor.h"

#include "vast/exception.h"
#include "vast/id_tracker.h"
#include "vast/event_source.h"
#include "vast/logger.h"
#include "vast/source/broccoli.h"
#include "vast/source/file.h"

namespace vast {

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
        broccoli_ = spawn<source::broccoli>(tracker_, archive_);
        send(broccoli_, atom("bind"), host, port);
      },
      on(atom("subscribe"), arg_match) >> [=](std::string const& event_name)
      {
        broccoli_ << last_dequeued();
      },
      on(atom("ingest"), "bro15conn", arg_match) >> [=](std::string const& filename)
      {
        auto src = spawn<source::bro15conn>(self, tracker_, filename);
        file_sources_.push(src);
        send(self, atom("process"));
      },
      on(atom("ingest"), "bro2", arg_match) >> [=](std::string const& filename)
      {
        auto src = spawn<source::bro2>(self, tracker_, filename);
        file_sources_.push(src);
        send(self, atom("process"));
      },
      on(atom("ingest"), val<std::string>, arg_match) >> [=](std::string const&)
      {
        LOG(error, ingest) << "invalid ingestion file type";
      },
      on(atom("process")) >> [=]
      {
        if (file_sources_.empty())
          return;

        send(file_sources_.front(), atom("extract"), batch_size);
      },
      on(atom("source"), atom("ack")) >> [=]
      {
        send(last_sender(), atom("extract"), batch_size);
      },
      on(atom("source"), atom("done")) >> [=]
      {
        send(file_sources_.front(), atom("shutdown"));
        file_sources_.pop();
        send(self, atom("process"));
      },
      on(atom("shutdown")) >> [=]
      {
        broccoli_ << last_dequeued();
        while (! file_sources_.empty())
        {
          auto src = file_sources_.front();
          src << last_dequeued();
          file_sources_.pop();
        }
        tracker_ << last_dequeued();

        quit();
        LOG(verbose, ingest) << "ingestor @" << id() << " terminated";
      });
}

} // namespace vast
