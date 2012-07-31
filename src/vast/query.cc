#include "vast/query.h"

#include <ze/chunk.h>
#include <ze/event.h>
#include <ze/type/regex.h>
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/detail/ast.h"
#include "vast/detail/parser/query.h"
#include "vast/util/parser/parse.h"

namespace vast {

using namespace cppa;

void query::window::push(cppa::cow_tuple<segment> s)
{
  segments_.push_back(s);
  if (! current_segment_)
  {
    assert(! reader_);
    current_segment_ = &cppa::get<0>(s);
    reader_.reset(new segment::reader(*current_segment_));
  }
}

bool query::window::one(ze::event& event)
{
  assert(reader_);
  if (reader_->events() == 0 && reader_->chunks() == 0)
    return false;

  *reader_ >> event;

  return true;
}

size_t query::window::size() const
{
  return segments_.size();
}

bool query::window::advance()
{
  if (segments_.empty())
    return false;

  segments_.pop_front();
  current_segment_ = &get<0>(segments_.front());
  reader_.reset(new segment::reader(*current_segment_));

  return true;
}


query::query(cppa::actor_ptr archive,
             cppa::actor_ptr index,
             cppa::actor_ptr sink)
  : archive_(archive)
  , index_(index)
  , sink_(sink)
{
  LOG(verbose, query)
    << "spawning query @" << id() << " with sink @" << sink_->id();

  chaining(false);
  init_state = (
      on(atom("set"), atom("expression"), arg_match) >> [=](std::string const& expr)
      {
        DBG(query)
          << "query @" << id() << " parses expression '" << expr << "'";

        parse(expr);
      },
      on(atom("set"), atom("batch size"), arg_match) >> [=](unsigned batch_size)
      {
        if (batch_size == 0)
        {
          LOG(warn, query)
            << "query @" << id() << " ignore invalid batch size of 0";

          reply(atom("set"), atom("batch size"), atom("failure"));
        }
        else
        {
          LOG(debug, query)
            << "query @" << id() << " sets batch size to " << batch_size;

          batch_size_ = batch_size;
          reply(atom("set"), atom("batch size"), atom("success"));
        }
      },
      on(atom("start")) >> [=]
      {
        DBG(query) << "query @" << id() << " hits index";
        run();
      },
      on_arg_match >> [=](segment const& s) 
      {
        auto opt = tuple_cast<segment>(last_dequeued());
        assert(opt.valid());
        window_.push(*opt);
        ++ack_;

        DBG(query)
          << "query @" << id() << " received segment " << s.id();

        DBG(query)
          << "query @" << id() << " window details: "
          << "size " << window_.size()
          << ", ack " << (ack_ - ids_.cbegin())
          << ", head " << (head_ - ids_.cbegin());

        ze::event e;
        window_.one(e);
        DBG(query) << e;
      },
      on(atom("get"), atom("results")) >> [=]
      {
        extract(batch_size_);
      },
      on(atom("get"), atom("statistics")) >> [=]
      {
        reply(atom("statistics"), stats_.processed, stats_.matched);
      },
      on(atom("shutdown")) >> [=]
      {
        self->quit();
        LOG(verbose, query) << "query @" << id() << " terminated";
      });
}

void query::parse(std::string const& expr)
{
  try
  {
    detail::ast::query query_ast;
    if (! util::parser::parse<detail::parser::query>(expr, query_ast))
      throw error::syntax("parse error", expr);

    if (! detail::ast::validate(query_ast))
      throw error::semantic("parse error", expr);

    expr_.assign(query_ast);
    reply(atom("set"), atom("expression"), atom("success"));
    return;
  }
  catch (error::syntax const& e)
  {
    LOG(error, query)
      << "syntax error in query @" << id() << ": " << e.what();
  }
  catch (error::semantic const& e)
  {
    LOG(error, query)
        << "semantic error in query @" << id() << ": " << e.what();
  }

  reply(atom("set"), atom("expression"), atom("failure"));
}

void query::run()
{
  // TODO: walk the AST and create more fine-grained index queries.
  auto future = sync_send(index_, atom("hit"), atom("all"));

  handle_response(future)(
      on(atom("hit"), arg_match) >> [=](std::vector<ze::uuid> const& ids)
      {
        LOG(info, query)
          << "query @" << id() << " received index hit (" 
          << ids.size() << " segments)";

        ids_ = ids;
        head_ = ack_ = ids_.begin();

        size_t first_fetch = std::min(ids_.size(), 3ul); // TODO: make configurable.
        for (size_t i = 0; i < first_fetch; ++i)
        {
          DBG(query) << "query @" << id() << " prefetches segment " << *head_;
          send(archive_, atom("get"), *head_++);
        }
      },
      on(atom("miss")) >> [=]
      {
        LOG(info, query)
          << "query @" << id() << " received index miss";

        send(sink_, atom("query"), atom("index"), atom("miss"));
      },
      after(std::chrono::minutes(1)) >> [=]
      {
        LOG(error, query)
          << "query @" << id()
          << " timed out after waiting one minute for index answer";

        send(sink_, atom("query"), atom("index"), atom("time-out"));
      });
}


void query::extract(size_t n)
{
  LOG(debug, query) << "query @" << id() << " extracts " << n << " results";

  ze::event e;
  size_t i = 0;
  while (i < n)
  {
    ze::event e;
    bool extracted;
    extracted = window_.one(e);
    if (extracted)
    {
      if (match(e))
        ++i;
    }
    else if (window_.advance())
    {
      DBG(query)
        << "query @" << id() << " advances to next segment in window";

      // Prefetch another segment if we still need to.
      if (ids_.end() - head_ > window_size_)
        send(archive_, atom("get"), *head_++);

      extracted = window_.one(e);
      assert(extracted); // By the post-condition of window::advance().

      if (match(e))
        ++i;
    }
    else if (ack_ < head_)
    {
      DBG(query)
        << "query @" << id() << " has in-flight segments, try again later";

      send(sink_, atom("query"), atom("busy"));
      break;
    }
    else if (head_ == ids_.end())
    {
      DBG(query)
        << "query @" << id() << " has no more segments to process";

      send(sink_, atom("query"), atom("finished"));
      break;
    }
  }
}

bool query::match(ze::event const& event)
{
  ++stats_.processed;
  if (expr_.eval(event))
  {
    cppa::send(sink_, std::move(event));
    ++stats_.matched;
    return true;
  }

  return false;
}

} // namespace vast
