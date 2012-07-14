#include <vast/ingest/ingestor.h>

#include <vast/comm/bro_event_source.h>
#include <vast/ingest/reader.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

ingestor::ingestor(cppa::actor_ptr archive)
  : archive_(archive)
{
  using namespace cppa;
  init_state = (
      on(atom("initialize"), arg_match) >> [=](std::string const& host,
                                               unsigned port)
      {
        bro_event_source_ = spawn<comm::bro_event_source>(self);
        send(bro_event_source_, atom("bind"), host, port);
      },
      on(atom("subscribe"), arg_match) >> [=](std::string const& event_name)
      {
        bro_event_source_ << self->last_dequeued();
      },
      on(atom("read_file"), arg_match) >> [=](std::string const& filename)
      {
        auto r = spawn<bro_reader>(archive_);
        send(r, atom("read"), filename);
        readers_.push_back(r);
      },
      on(atom("shutdown")) >> [=]()
      {
        bro_event_source_ << self->last_dequeued();
        for (auto& r : readers_)
          r << self->last_dequeued();
        readers_.clear();

        self->quit();
        LOG(verbose, ingest) << "ingestor terminated";
      });
}

} // namespace ingest
} // namespace vast
