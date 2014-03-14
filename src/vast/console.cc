#include "vast/console.h"

#include <cassert>
#include <iomanip>
#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/io/serialization.h"
#include "vast/util/color.h"
#include "vast/util/make_unique.h"
#include "vast/util/parse.h"
#include "vast/util/poll.h"

using namespace cppa;

namespace vast {

namespace {

struct keystroke_monitor : actor<keystroke_monitor>
{
  keystroke_monitor(actor_ptr sink)
    : sink_{sink}
  {
    el_.on_char_read(
        [](char *c) -> int
        {
          if (! util::poll(::fileno(stdin), 100000))
            return 0;

          auto ch = ::fgetc(stdin);
          if (ch == '\x04')
            ch = EOF;

          *c = static_cast<char>(ch);

          return 1;
        });
  }

  void act()
  {
    become(
        on(atom("start")) >> [=]
        {
          running_ = true;
          send(self, atom("get"));
        },
        on(atom("stop")) >> [=]
        {
          running_ = false;
        },
        on(atom("get")) >> [=]
        {
          if (! running_)
            return;

          char c;
          if (el_.get(c))
            send(sink_, atom("key"), c);
          else
            send(self, atom("get"));
        });
  };

  char const* description() const
  {
    return "keystroke-monitor";
  };

  bool running_ = true;
  util::editline el_;
  actor_ptr sink_;
};


// Generates a callback function for a mode or command.
struct help_printer
{
  template <typename T>
  auto operator()(T x) const -> util::command_line::callback
  {
    return [=](std::string) -> util::result<bool>
    {
      std::cerr
        << "\noptions for "
        << util::color::cyan << x->name() << util::color::reset << ":\n\n"
        << x->help(4)
        << std::endl;

      return true;
    };
  }
};

help_printer help;

struct cow_event_less_than
{
  bool operator()(cow<event> const& x, cow<event> const& y) const
  {
    return *x < *y;
  }
};

cow_event_less_than cow_event_lt;

} // namespace <anonymous>

console::console(cppa::actor_ptr search, path dir)
  : dir_{std::move(dir)},
    search_{std::move(search)}
{
  if (! exists(dir_) && ! mkdir(dir_))
  {
    VAST_LOG_ACTOR_ERROR("failed to create console directory: " << dir_);
    quit(exit::error);
    return;
  }

  if (! exists(dir_ / "results") && ! mkdir(dir_ / "results"))
  {
    VAST_LOG_ACTOR_ERROR("failed to create console result directory");
    quit(exit::error);
    return;
  }

  // Look for persistent queries.
  traverse(
      dir_ / "results",
      [this](path const& p)
      {
        auto r = make_intrusive<result>();
        io::unarchive(p / "meta", *r);
        r->load(p / "data");
        results_.push_back(r);
        return true;
      });


  auto complete = [](std::string const& prefix, std::vector<std::string> match)
    -> std::string
  {
    if (match.size() == 1)
      return match[0];

    std::cerr << '\n';
    for (auto& m : match)
      std::cerr
        << util::color::yellow << prefix << util::color::reset
        << m.substr(prefix.size()) << std::endl;

    return "";
  };

  auto main = cmdline_.mode_add("main", "::: ", util::color::cyan,
                                to<std::string>(dir_ / "history_main"));

  main->on_unknown_command(help(main));
  main->on_complete(complete);

  main->add("exit", "exit the console")->on(
      [=](std::string) -> util::result<bool>
      {
        quit(exit::stop);
        return {};
      });

  auto set = main->add("set", "adjust console settings");
  set->on(help(set));

  set->add("batch-size", "number of results to display")->on(
      [=](std::string args) -> util::result<bool>
      {
        uint64_t n;
        auto begin = args.begin();
        if (extract(begin, args.end(), n))
        {
          opts_.batch_size = n;
          return true;
        }
        else
        {
          print(error) << "batch-size requires numeric argument" << std::endl;
          return false;
        }
      });

  auto auto_follow = set->add("auto-follow",
                              "enter interactive control mode after query creation");
  auto_follow->on(
      [=](std::string args) -> util::result<bool>
      {
        match_split(args, ' ')(
            on("T") >> [=](std::string const&)
            {
              opts_.auto_follow = true;
            },
            on("F") >> [=](std::string const&)
            {
              opts_.auto_follow = false;
            },
            others() >> [=]
            {
              print(error) << "need 'T' or 'F' as argument" << std::endl;
              return false;
            });

        return true;
      });

  set->add("show", "display the current settings")->on(
      [=](std::string) -> util::result<bool>
      {
        print(none)
          << "batch-size = " << util::color::cyan
          << opts_.batch_size << util::color::reset << '\n'
          << "auto-follow = " << util::color::cyan
          << (opts_.auto_follow ? "T" : "F") << util::color::reset
          << std::endl;

        return true;
      });

  main->add("ask", "enter query mode")->on(
      [=](std::string) -> util::result<bool>
      {
        cmdline_.append_to_history("ask");
        cmdline_.mode_push("ask");
        return false;
      });


  main->add("list", "list existing queries")->on(
      [=](std::string) -> util::result<bool>
      {
        std::set<intrusive_ptr<result>> active;
        for (auto& p : active_)
          if (p.first)
            active.insert(p.second);

        for (auto& r : results_)
          print(none)
            << util::color::green
            << (active.count(r) ? " * " : "   ")
            << util::color::cyan
            << r->id()
            << util::color::blue << " | " << util::color::reset
            << r->percent(true) << "%"
            << util::color::blue << " | " << util::color::reset
            << r->ast()
            << std::endl;

        return true;
      });

  main->add("query", "enter a query")->on(
      [=](std::string args) -> util::result<bool>
      {
        if (args.empty())
        {
          print(error) << "missing query UUID" << std::endl;
          return false;
        }

        std::vector<intrusive_ptr<result>> matches;
        actor_ptr qry = nullptr;
        for (auto& r : results_)
        {
          auto candidate = to<std::string>(r->id());
          auto i = std::mismatch(args.begin(), args.end(), candidate.begin());
          if (i.first == args.end())
          {
            for (auto& p : active_)
              if (p.second == r)
              {
                qry = p.first;
                break;
              }

            matches.push_back(r);
          }
        }

        if (matches.empty())
        {
          print(error) << "no such query: " << args << std::endl;
          return false;
        }
        else if (matches.size() > 1)
        {
          print(error) << "ambiguous query: " << args << std::endl;
          return false;
        }

        if (qry)
          current_.first = qry;
        current_.second = matches[0];

        send(keystroke_monitor_, atom("start"));
        VAST_LOG_ACTOR_DEBUG("enters query " << current_.second->id());
        return {};
      });

  auto ask = cmdline_.mode_add("ask", "-=> ", util::color::green,
                               to<std::string>(dir_ / "history_query"));

  ask->add("exit", "leave query asking mode")->on(
      [=](std::string) -> util::result<bool>
      {
        cmdline_.mode_pop();
        return false;
      });

  ask->on_complete(complete);

  ask->on_unknown_command(
      [=](std::string args) -> util::result<bool>
      {
        if (args.empty())
          return false;

        sync_send(search_, atom("query"), atom("create"), args).then(
            on(atom("EXITED"), arg_match) >> [=](uint32_t reason)
            {
              print(error)
                << "search terminated with exit code " << reason << std::endl;

              quit(exit::error);
            },
            on_arg_match >> [=](std::string const& failure) // TODO: use error class.
            {
              print(error) << "syntax error: " << failure << std::endl;
              send(self, atom("prompt"));
            },
            on_arg_match >> [=](expr::ast const& ast, actor_ptr const& qry)
            {
              assert(! active_.count(qry));
              assert(qry);
              assert(ast);

              cmdline_.append_to_history(args);
              monitor(qry);
              current_.first = qry;
              current_.second = make_intrusive<result>(ast);


              auto i = std::find_if(
                  results_.begin(), results_.end(),
                  [&](intrusive_ptr<result> r) { return r->ast() == ast; });

              if (i != results_.end())
                print(warn) << "duplicate query for " << (*i)->id() << std::endl;

              active_.insert(current_);
              results_.push_back(current_.second);

              print(info)
                << "new query " << current_.second->id()
                << " -> " << ast << std::endl;

              if (opts_.auto_follow)
              {
                follow_mode_ = true;
                send(keystroke_monitor_, atom("start"));
              }
              else
              {
                send(self, atom("prompt"));
              }
            },
            others() >> [=]
            {
              VAST_LOG_ACTOR_ERROR("got unexpected message: " <<
                                   to_string(last_dequeued()));
              send(self, atom("prompt"));
            });

      return {};
    });

  // TODO: this mode is not yet fully fleshed out.
  auto fs = cmdline_.mode_add("file-system", "/// ");

  auto list_directory = [](path const& dir) -> std::vector<std::string>
  {
    std::vector<std::string> files;
    traverse(dir,
             [&](path const& p)
             {
               auto str = to_string(p.basename());
               if (str.size() >= 2 && str[0] == '.' && str[1] == '/')
                 str = str.substr(2);
               if (p.is_directory())
                 str += '/';
               files.push_back(std::move(str));
               std::sort(files.begin(), files.end());
               return true;
             });

    return files;
  };

  auto file_list = std::make_shared<std::vector<std::string>>(list_directory("."));
  fs->complete(*file_list);

  fs->on_complete(
    [=](std::string const& pfx, std::vector<std::string> match) -> std::string
    {
      path next;
      if (match.empty())
        next = path{pfx};
      else if (match.size() == 1)
        next = path{match[0]};

      if (! next.empty())
      {
        if (next.is_directory())
        {
          // If we complete deep in the diretory hierarchy, we may not have a
          // '/' at the end.
          if (next.str().back() != '/')
            next = path{next.str() + '/'};

          auto contents = list_directory(next);

          // TODO: only show the relevant entries relative to the current
          // directory.
          for (auto& f : contents)
            std::cerr
              << util::color::yellow << next << util::color::reset
              << f << std::endl;

          for (auto& f : contents)
            f.insert(0, to_string(next));

          auto n = file_list->size();
          file_list->insert(file_list->end(), contents.begin(), contents.end());
          std::inplace_merge(file_list->begin(),
                             file_list->begin() + n,
                             file_list->end());


          fs->complete(*file_list);
        }

        return to_string(next);
      }

      auto min_len = pfx.size();
      std::string const* shortest = nullptr;

      for (auto& m : match)
      {
        if (m.size() < min_len)
        {
          min_len = m.size();
          shortest = &m;
        }

        std::cerr
          << '\n' << util::color::yellow << pfx << util::color::reset
          << m.substr(pfx.size());
      }

      if (! match.empty())
        std::cerr << '\n';

      return shortest ? *shortest : pfx;
    });


  fs->on_unknown_command(
      [=](std::string) -> util::result<bool>
      {
        *file_list = list_directory(".");
        fs->complete(*file_list);

        cmdline_.mode_pop();
        return true;
      });

  cmdline_.mode_push("main");
}

console::result::result(expr::ast ast)
  : ast_{std::move(ast)}
{
}

bool console::result::save(path const& p) const
{
  file f{p};
  f.open(file::write_only);
  io::file_output_stream fos{f};
  std::unique_ptr<io::compressed_output_stream>
    cos{io::make_compressed_output_stream(io::lz4, fos)};
  binary_serializer sink{*cos};

  // TODO: use segments.
  sink << static_cast<uint64_t>(events_.size());
  for (auto& e : events_)
    sink << e;

  return true;
}

bool console::result::load(path const& p)
{
  file f{p};
  f.open(file::read_only);
  io::file_input_stream fis{f};
  std::unique_ptr<io::compressed_input_stream>
    cos{io::make_compressed_input_stream(io::lz4, fis)};
  binary_deserializer source{*cos};

  // TODO: use segments.
  uint64_t size;
  source >> size;
  if (size > 0)
  {
    events_.resize(size);
    for (size_t i = 0; i < size; ++i)
      source >> events_[i];
  }

  return true;
}

void console::result::add(cow<event> e)
{
  auto i = std::lower_bound(events_.begin(), events_.end(), e, cow_event_lt);
  assert(i == events_.end() || cow_event_lt(e, *i));
  events_.insert(i, e);
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

expr::ast const& console::result::ast() const
{
  return ast_;
}

size_t console::result::size() const
{
  return events_.size();
}

void console::result::progress(double p)
{
  progress_ = p;
}

double console::result::progress() const
{
  return progress_;
}

double console::result::percent(size_t precision) const
{
  double i;
  auto m = std::pow(10, precision);
  auto f = std::modf(progress_ * 100, &i) * m;
  return i + std::trunc(f) / m;

  return progress_;
}

void console::result::serialize(serializer& sink) const
{
  individual::serialize(sink);
  sink << ast_ << progress_ << pos_;
}

void console::result::deserialize(deserializer& source)
{
  individual::deserialize(source);
  source >> ast_ >> progress_ >> pos_;
}


void console::act()
{
  chaining(false);

  print(none)
    << util::color::red
    << "     _   _____   __________\n"
       "    | | / / _ | / __/_  __/\n"
       "    | |/ / __ |_\\ \\  / /\n"
       "    |___/_/ |_/___/ /_/  "
    << util::color::yellow
    << VAST_VERSION << '\n'
    << util::color::reset
    << std::endl;

  keystroke_monitor_ = spawn<keystroke_monitor, detached+linked>(self);

  auto prompt = [=](size_t ms = 100)
  {
    if (ms > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(ms));

    std::string line;
    auto t = cmdline_.get(line);
    if (! t)
    {
      VAST_LOG_ACTOR_ERROR("failed to retrieve command line: " << t.failure());
      quit(exit::error);
      return;
    }

    // Check for CTRL+D.
    if (! *t)
    {
      print(none) << std::endl;
      if (cmdline_.mode_pop() > 0)
        send(self, atom("prompt"));
      else
        send_exit(self, exit::stop);
      return;
    }

    if (line.empty())
    {
      send(self, atom("prompt"));
      return;
    }

    // Only an empty result means that we should not go back to the prompt. If
    // we have a result, then the boolean return value indicates whether to
    // append the command line to the history.
    auto r = cmdline_.process(line);
    if (r.engaged())
    {
      if (*r)
        cmdline_.append_to_history(line);
      send(self, atom("prompt"));
    }
    else if (r.failed())
    {
      print(error) << r.failure() << std::endl;
      send(self, atom("prompt"));
    }
  };

  become(
      on(atom("exited")) >> [=]
      {
        print(error) << "search terminated" << std::endl;
        quit(exit::error);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t)
      {
        VAST_LOG_ACTOR_DEBUG("got DOWN from query " <<
                             VAST_ACTOR_ID(last_sender()));

        // FIXME: we only delay the send operation here because there exists a
        // message reordering bug in libcpppa that causes events to arrive
        // receiving this DOWN message. Giving the actor some time prevents the
        // result state from premature eviction.
        delayed_send(self, std::chrono::seconds(10), atom("remove"),
                     last_sender());
      },
      on(atom("error"), arg_match) >> [=](std::string const& msg)
      {
        print(error) << msg << std::endl;
        prompt();
      },
      on(atom("done")) >> [=]
      {
        VAST_LOG_ACTOR_DEBUG("got done notification from query "
                             << VAST_ACTOR_ID(last_sender()));

        // FIXME: see note above.
        delayed_send(self, std::chrono::seconds(10), atom("remove"),
                     last_sender());
      },
      on(atom("remove"), arg_match) >> [=](actor_ptr doomed)
      {
        VAST_LOG_ACTOR_DEBUG("detaches query " << VAST_ACTOR_ID(doomed));
        if (current_.first == doomed)
          current_ = {};

        active_.erase(doomed);
      },
      on(atom("prompt")) >> [=]
      {
        prompt();
      },
      on(atom("progress"), arg_match) >> [=](double progress, uint64_t hits)
      {
        auto i = active_.find(last_sender());
        assert(i != active_.end());
        auto r = i->second;

        if (r->progress() <= progress + 0.05 || progress == 1.0)
        {
          if (follow_mode_)
          {
            auto base = r->progress();
            if (! append_mode_)
            {
              print(query)
                << "progress "
                << util::color::blue << '|' << util::color::reset;

              base = 0.0;
              append_mode_ = true;
            }

            print(none) << util::color::green;
            for (auto d = base; d < progress; d += 0.05)
              print(none) << '*';
            print(none) << util::color::reset << std::flush;

            if (progress == 1.0)
            {
              print(none)
                << util::color::green << '*'
                << util::color::blue << '|' << util::color::reset << std::endl;

              append_mode_ = false;

              if (hits == 0)
              {
                send(keystroke_monitor_, atom("stop"));
                follow_mode_ = false;
                prompt();
              }
            }
          }

          r->progress(progress);
        }
      },
      on_arg_match >> [=](event const&)
      {
        auto i = active_.find(last_sender());
        assert(i != active_.end());
        auto& r = i->second;

        cow<event> ce = *tuple_cast<event>(last_dequeued());
        r->add(ce);
        if (follow_mode_ && r == current_.second)
        {
          if (append_mode_)
          {
            print(none) << std::endl;
            append_mode_ = false;
          }

          std::cout << *ce << std::endl;
        }
      },
      on(atom("key"), arg_match) >> [=](char key)
      {
        switch (key)
        {
          default:
            {
              std::string desc;
              switch (key)
              {
                default:
                  desc = key;
                  break;
                case '\t':
                  desc = "\\t";
                  break;
              }

              print(error)
                << "invalid key: '" << desc << "', press '?' for help"
                << std::endl;
            }
            break;
          case '\n':
            print(none) << std::endl;
            break;
          case '?':
            {
              print(none)
                << "interactive query control mode:\n"
                << "\n"
                << "     <space>  display the next batch of available results\n"
                << "  " << util::color::green << '*' << util::color::reset <<
                      "     e     ask query for more results\n"
                << "        f     toggle follow mode\n"
                << "        j     seek one batch forward\n"
                << "        k     seek one batch backword\n"
                << "        p     show progress of index lookup\n"
                << "        q     leave query control mode\n"
                << "        s     save the result to file system\n"
                << "        ?     display this help\n"
                << "\n"
                << "entries marked with " <<
                   util::color::green << '*' << util::color::reset <<
                   " require a connected query\n"
                << std::endl;
            }
            break;
          case ' ':
            {
              assert(current_.second);
              auto n = current_.second->apply(
                  opts_.batch_size,
                  [](event const& e) { std::cout << e << std::endl; });

              if (n == 0)
                print(query) << "reached end of results" << std::endl;
            }
            break;
          case 'e':
            {
              if (current_.first)
              {
                send(current_.first, atom("extract"), opts_.batch_size);
                print(query)
                  << "asks for " << opts_.batch_size
                  << " more results" << std::endl;
              }
              else
              {
                print(query) << "not connected" << std::endl;
              }
            }
            break;
          case 'f':
            {
              follow_mode_ = ! follow_mode_;
              print(query)
                << "toggled follow-mode to "
                << util::color::cyan
                << (follow_mode_ ? "on" : "off")
                << util::color::reset << std::endl;
            }
            break;
          case 'j':
            {
              assert(current_.second);
              auto n = current_.second->seek_forward(opts_.batch_size);
              print(query) << "seeked +" << n << " events" << std::endl;
            }
            break;
          case 'k':
            {
              assert(current_.second);
              auto n = current_.second->seek_backward(opts_.batch_size);
              print(query) << "seeked -" << n << " events" << std::endl;
            }
            break;
          case 'p':
            {
              assert(current_.second);
              auto p = static_cast<int>(current_.second->percent(0) / 5);

              print(query)
                << "progress "
                << util::color::blue << '|' << util::color::green;

              for (int i = 0; i < p; ++i)
                print(none) << '*';
              for (int i = 0; i < 20 - p; ++i)
                print(none) << ' ';

              print(none)
                << util::color::blue << '|' << util::color::reset << std::endl;
            }
            break;
          case EOF:
          case '':
          case 'q':
            {
              follow_mode_ = false;
              prompt();
            }
            return;
          case 's':
            {
              assert(current_.second);

              // TODO: look if same AST already exists under a different file.
              auto const dir =
                dir_ / "results" / path{to_string(current_.second->id())};

              if (exists(dir))
              {
                // TODO: support option to overwrite/append.
                print(error) << "results already exists" << std::endl;
              }
              else
              {
                if (! mkdir(dir))
                {
                  print(error) << "failed to create dir: " << dir << std::endl;
                  quit(exit::error);
                  return;
                }
                auto n = current_.second->size();
                print(query) << "saving result to " << dir  << std::endl;
                io::archive(dir / "meta", *current_.second);
                current_.second->save(dir / "data");
                print(query) << "saved " << n << " events" << std::endl;
              }

              prompt();
            }

            return;
        }

        send(keystroke_monitor_, atom("get"));
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

std::ostream& console::print(print_mode mode)
{
  if (mode == none)
    return std::cerr;

  if (append_mode_)
  {
    std::cerr << std::endl;
    append_mode_ = false;
  }

  switch (mode)
  {
    default:
      std::cerr << util::color::red << "[???] ";
      break;
    case error:
      std::cerr << util::color::red << "[!!] ";
      break;
    case warn:
      std::cerr << util::color::yellow << "[!!] ";
      break;
    case info:
      std::cerr << util::color::blue << "[::] ";
      break;
    case query:
      std::cerr
        << util::color::cyan << "[" << current_.second->id() << "] ";
      break;
  }

  std::cerr << util::color::reset;

  return std::cerr;
}

} // namespace vast
