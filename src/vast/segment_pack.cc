#include "vast/segment_pack.h"

#include "vast/event.h"

namespace vast {

using namespace caf;

packer::packer(actor manager, actor sink)
  : manager_{manager},
    sink_{sink},
    writer_{&segment_}
{
}

message_handler packer::act()
{
  attach_functor(
      [=](uint32_t)
      {
        manager_ = invalid_actor;
        sink_ = invalid_actor;
      });

  return
  {
    [=](std::vector<event> const& v)
    {
      auto full = false;
      for (auto& e : v)
        if (! writer_.write(e))
        {
          full = true;
          break;
        }

      if (! full)
        return;

      // TODO: ship it and start over.
    }
  };
}

std::string packer::describe() const
{
  return "packer";
}


unpacker::unpacker(message segment, actor sink, size_t batch_size)
  : segment_{segment},
    reader_{&segment_.get_as<vast::segment>(0)},
    sink_{sink},
    batch_size_{batch_size}
{
  events_.reserve(batch_size_);
}

message_handler unpacker::act()
{
  attach_functor([=](uint32_t) { sink_ = invalid_actor; });

  return
  {
    on(atom("run")) >> [=]
    {
      while (auto e = reader_.read())
      {
        assert(e);

        if (e->type().name().empty() || is<none>(*e))
        {
          VAST_LOG_ACTOR_ERROR("got invalid event: " << *e);
          quit(exit::error);
          return;
        }

        events_.push_back(std::move(*e));

        if (events_.size() == batch_size_)
          break;
      }

      if (events_.empty())
      {
        send(sink_, atom("unpacked"));
        quit(exit::done);
      }
      else
      {
        send(sink_, std::move(events_));
        events_ = {};
        send_tuple(this, last_dequeued());
      }
    }
  };
}

std::string unpacker::describe() const
{
  return "unpacker";
}

} // namespace vast
