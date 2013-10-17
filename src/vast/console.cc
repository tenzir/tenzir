#include "vast/console.h"

#include <cassert>
#include <iomanip>
#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/util/parse.h"

using namespace cppa;

namespace vast {

console::console(cppa::actor_ptr search)
  : search_{std::move(search)}
{
  cmdline_.mode_add("main", "main mode", "::: ");
  cmdline_.mode_push("main");
  cmdline_.on_unknown_command(
      "main",
      [=](std::string arg)
      {
        std::cout
          << "[error] invalid command: " << arg << ", try 'help'" << std::endl;
        return true;
      });

  cmdline_.cmd_add(
      "main",
      "exit",
      [=](std::string)
      {
        quit(exit::stop);
        return false;
      });

  cmdline_.cmd_add(
      "main",
      "help",
      [=](std::string)
      {
        auto help =
          "ask         enter query mode (leave with 'exit')\n"
          "list        show all queries (active prefixed with *)\n"
          "stats       show query statistics\n"
          "query <id>  enter control mode of given query\n"
          "set <..>    settings\n"
          "exit        exit the console";
        std::cout << help << std::endl;
        return true;
      });

  // TODO: consider using a settings mode instead of just a single command.
  cmdline_.cmd_add(
      "main",
      "set",
      [=](std::string arg)
      {
        match_split(arg, ' ')(
            on("paginate", arg_match) >> [=](std::string const& str)
            {
              uint32_t n;
              auto begin = str.begin();
              if (extract(begin, str.end(), n))
                opts_.paginate = n;
              else
                std::cout
                  << "[error] paginate requires numeric argument" << std::endl;
            },
            on("auto-follow", "T") >> [=](std::string const&)
            {
              opts_.auto_follow = true;
            },
            on("auto-follow", "F") >> [=](std::string const&)
            {
              opts_.auto_follow = false;
            },
            on("show") >> [=]
            {
              std::cout
                << "paginate = " << opts_.paginate << '\n'
                << "auto-follow = " << (opts_.auto_follow ? "T" : "F")
                << std::endl;
            },
            on("help") >> [=]
            {
              auto help =
                "paginate <n>       number of results to display\n"
                "auto-follow <T|F>  follow query after creation\n"
                "show               shows current settings";
              std::cout << help << std::endl;
            },
            others() >> [=]
            {
              std::cout
                << "[error] invalid argument, check 'set help'" << std::endl;
            });
        return true;
      });

  cmdline_.cmd_add(
      "main",
      "ask",
      [=](std::string)
      {
        cmdline_.mode_push("query-ask");
        return true;
      });


  cmdline_.cmd_add(
      "main",
      "list",
      [=](std::string)
      {
        for (auto& p : results_)
          std::cout
            << (&p.second == current_result_ ? " * " : "   ")
            << p.second.id() << '\t' << p.second.ast()
            << std::endl;
        return true;
      });

  cmdline_.cmd_add(
      "main",
      "stats",
      [=](std::string)
      {
        for (auto& p : results_)
          std::cout
            << (&p.second == current_result_ ? " * " : "   ")
            << p.second.id() << '\t'
            << p.second.consumable() << '/' << p.second.size()
            << std::endl;
        return true;
      });

  cmdline_.cmd_add(
      "main",
      "query",
      [=](std::string arg)
      {
        if (arg.empty())
        {
          std::cout
            << "[error] argument required, check 'query help'"
            << std::endl;
          return true;
        }
        if (auto r = to_result(arg))
        {
          VAST_LOG_ACTOR_DEBUG("enters query " << r->id());
          current_result_ = r;
          send(self, atom("key"), atom("get"));
          return false;
        }
        return true;
      });

  cmdline_.mode_add("query-ask", "query asking mode", "-=> ");

  cmdline_.cmd_add(
      "query-ask",
      "exit",
      [=](std::string)
      {
        cmdline_.mode_pop();
        return true;
      });

  cmdline_.on_unknown_command(
      "query-ask",
      [=](std::string q)
      {
        if (q.empty())
          return true;
        sync_send(search_, atom("query"), atom("create"), q).then(
            on_arg_match >> [=](actor_ptr const& qry, expr::ast const& ast)
            {
              assert(! results_.count(qry));
              cmdline_.append_to_history(q);
              if (! qry)
              {
                std::cout << "[error] invalid query: " << q << std::endl;
                return;
              }
              monitor(qry);
              current_result_ = &results_.emplace(qry, ast).first->second;
              cmdline_.mode_pop();
              std::cout
                << "new query " << current_result_->id()
                << " -> " << ast << std::endl;
              if (opts_.auto_follow)
              {
                follow_mode_ = true;
                send(self, atom("key"), atom("get"));
              }
              else
              {
                show_prompt();
              }
            },
            others() >> [=]
            {
              VAST_LOG_ACTOR_ERROR("got unexpected message: " <<
                                   to_string(last_dequeued()));
            });
      return false;
    });
}

console::result::result(expr::ast ast)
  : ast_{std::move(ast)}
{
}

void console::result::add(cow<event> e)
{
  events_.push_back(std::move(e));
}

size_t console::result::apply(size_t n, std::function<void(event const&)> f)
{
  size_t i = 0;
  while (i < n && pos_ < events_.size())
  {
    f(*events_[pos_++]);
    ++i;
  }
  return i;
}


size_t console::result::seek_forward(size_t n)
{
  if (pos_ + n >= events_.size())
  {
    auto seeking = static_cast<size_t>(events_.size() - pos_);
    pos_ = events_.size();
    return seeking;
  }
  else
  {
    pos_ += n;
    return n;
  }
}

size_t console::result::seek_backward(size_t n)
{
  if (n > pos_)
  {
    auto old = pos_;
    pos_ = 0;
    return static_cast<size_t>(old);
  }
  else
  {
    pos_ -= n;
    return n;
  }
}

size_t console::result::consumable() const
{
  assert(pos_ <= events_.size());
  return static_cast<size_t>(pos_type(events_.size()) - pos_);
}


expr::ast const& console::result::ast() const
{
  return ast_;
}

size_t console::result::size() const
{
  return events_.size();
}

void console::act()
{
  chaining(false);

  auto keystroke_monitor = spawn<detached+linked>(
      [=]
      {
        become(
            on(atom("get")) >> [=]
            {
              char c;
              if (cmdline_.get(c))
                reply(atom("key"), c);
            });
      });

  become(
      on(atom("DOWN"), arg_match) >> [=](uint32_t)
      {
        VAST_LOG_ACTOR_ERROR("got DOWN from query @" << last_sender()->id());
        results_.erase(last_sender());
      },
      on(atom("prompt")) >> [=]
      {
        bool callback_result;
        if (! cmdline_.process(callback_result) || callback_result)
          show_prompt();
      },
      on_arg_match >> [=](event const&)
      {
        auto i = results_.find(last_sender());
        assert(i != results_.end());
        i->second.add(*tuple_cast<event>(last_dequeued()));
        if (&i->second == current_result_ && follow_mode_)
          current_result_->apply(
              1,
              [](event const& e) { std::cout << e << std::endl; });
      },
      on(atom("key"), atom("get")) >> [=]
      {
        send(keystroke_monitor, atom("get"));
      },
      on(atom("key"), arg_match) >> [=](char key)
      {
        switch (key)
        {
          default:
            {
              std::string desc;
              if (key == '\n')
                desc = "\\n";
              else if (key == ' ')
                desc = "space";
              else
                desc.push_back(key);
              std::cout << "invalid key: '" << desc << "'" << std::endl;
            }
            break;
          case '':
          case 'q':
            {
              show_prompt();
            }
            return;
          case 'f':
            {
              follow_mode_ = ! follow_mode_;
            }
            break;
          case 'p':
            {
              auto n = current_result_->seek_backward(opts_.paginate);
              std::cout
                << "[query " << current_result_->id() << "] seeked -"
                << n << " events" << std::endl;
            }
            break;
          case 'n':
            {
              auto n = current_result_->seek_forward(opts_.paginate);
              std::cout
                << "[query " << current_result_->id() << "] seeked +"
                << n << " events" << std::endl;
            }
            break;
          case ' ':
            {
              current_result_->apply(
                  opts_.paginate,
                  [](event const& e) { std::cout << e << std::endl; });
            }
            break;
        }
        send(keystroke_monitor, atom("get"));
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR("got unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(last_dequeued()));
      });
}

char const* console::description() const
{
  return "console";
}

void console::show_prompt(size_t ms)
{
  // The delay allows for logging messages to trickle through first
  // before we print the prompt.
  delayed_send(self, std::chrono::milliseconds(ms), atom("prompt"));
}

console::result* console::to_result(std::string const& str)
{
  std::vector<result*> matches;
  for (auto& p : results_)
  {
    auto candidate = to<std::string>(p.second.id());
    auto i = std::mismatch(str.begin(), str.end(), candidate.begin());
    if (i.first == str.end())
      matches.push_back(&p.second);
  }
  if (matches.empty())
    std::cout << "[error] no such query: " << str << std::endl;
  else if (matches.size() > 1)
    std::cout << "[error] ambiguous query: " << str << std::endl;
  else
    return matches[0];
  return nullptr;
}

} // namespace vast
