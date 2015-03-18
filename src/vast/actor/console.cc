#include <cassert>
#include <iomanip>

#include <caf/all.hpp>

#include "vast/parse.h"
#include "vast/actor/console.h"
#include "vast/util/color.h"
#include "vast/util/meta.h"
#include "vast/util/poll.h"

using namespace caf;

namespace vast {
namespace {

struct keystroke_monitor : default_actor
{
  keystroke_monitor(actor const& sink)
    : default_actor{"keystroke-monitor"},
      sink_{sink}
  {
    el_.on_char_read(
      [](char *c) -> int
      {
        if (! util::poll(::fileno(stdin), 500000))
          return 0;
        auto ch = ::fgetc(stdin);
        if (ch == '\x04')
          ch = EOF;
        *c = static_cast<char>(ch);
        return 1;
      });
  }

  void on_exit()
  {
    sink_ = invalid_actor;
  }

  behavior make_behavior() override
  {
    return
    {
      [=](start_atom)
      {
        el_.reset();
        running_ = true;
        send(this, get_atom::value);
      },
      [=](stop_atom)
      {
        running_ = false;
      },
      [=](get_atom)
      {
        if (! running_)
          return;

        char c;
        if (el_.get(c))
          send(sink_, key_atom::value, c);
        else
          send(this, get_atom::value);
      }
    };
  };

  bool running_ = true;
  util::editline el_;
  actor sink_;
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

} // namespace <anonymous>

console::console(caf::actor search, path dir)
  : default_actor{"console"},
    dir_{std::move(dir)},
    search_{std::move(search)}
{
  if (! exists(dir_) && ! mkdir(dir_))
  {
    VAST_ERROR(this, "failed to create console directory:", dir_);
    quit(exit::error);
    return;
  }

  if (! exists(dir_ / "results") && ! mkdir(dir_ / "results"))
  {
    VAST_ERROR(this, "failed to create console result directory");
    quit(exit::error);
    return;
  }

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

  auto main = cmdline_.mode_add("main", "> ", util::color::cyan,
                                to_string(dir_ / "history_main"));

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
        auto lval = args.begin();
        if (auto n = parse<uint64_t>(lval, args.end()))
        {
          opts_.batch_size = *n;
          return true;
        }
        else
        {
          print(fail) << "batch-size requires numeric argument" << std::endl;
          return false;
        }
      });

  auto auto_follow = set->add(
      "auto-follow",
      "enter interactive control mode after query creation");

  // FIXME: replace match_split with new function split in CAF > 0.12.2. Since
  // we also work with develop, we just ignore this feature for now since the
  // console needs rewriting in SASH anyway.
  //auto_follow->on(
  //    [=](std::string args) -> util::result<bool>
  //    {
  //      match_split(args, ' ')(
  //          on("T") >> [=](std::string const&)
  //          {
  //            opts_.auto_follow = true;
  //          },
  //          on("F") >> [=](std::string const&)
  //          {
  //            opts_.auto_follow = false;
  //          },
  //          others() >> [=]
  //          {
  //            print(fail) << "need 'T' or 'F' as argument" << std::endl;
  //            return false;
  //          });
  //      return true;
  //    });

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
        for (auto& p : connected_)
          if (p.second.first)
            active.insert(p.second.second);

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
          print(fail) << "missing query UUID" << std::endl;
          return false;
        }

        std::vector<intrusive_ptr<result>> matches;
        for (auto& r : results_)
        {
          auto candidate = to_string(r->id());
          auto i = std::mismatch(args.begin(), args.end(), candidate.begin());
          if (i.first == args.end())
            matches.push_back(r);
        }

        if (matches.empty())
        {
          print(fail) << "no such query: " << args << std::endl;
          return false;
        }
        else if (matches.size() > 1)
        {
          print(fail) << "ambiguous query: " << args << std::endl;
          return false;
        }

        active_ = matches[0];

        follow();

        VAST_DEBUG(this, "enters query", active_->id());
        return {};
      });

  auto ask = cmdline_.mode_add("ask", "? ", util::color::green,
                               to_string(dir_ / "history_query"));

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

        sync_send(search_, query_atom::value, this, args).then(
            on_arg_match >> [=](sync_exited_msg const& e)
            {
              print(fail)
                << "search terminated with exit code " << e.reason << std::endl;

              quit(exit::error);
            },
            [=](error const& e)
            {
              print(fail) << "syntax error: " << e << std::endl;
              send(this, prompt_atom::value);
            },
            [=](expression const& ast, actor const& qry)
            {
              assert(! connected_.count(qry.address()));
              assert(qry);

              cmdline_.append_to_history(args);
              monitor(qry);
              active_ = make_intrusive<result>(ast);

              auto i = std::find_if(
                  results_.begin(), results_.end(),
                  [&](intrusive_ptr<result> r) { return r->ast() == ast; });

              if (i != results_.end())
                print(warn) << "duplicate query for " << (*i)->id() << std::endl;

              connected_.emplace(qry->address(), std::make_pair(qry, active_));
              results_.push_back(active_);

              print(info)
                << "new query " << active_->id()
                << " -> " << ast << std::endl;

              send(qry, extract_atom::value, opts_.batch_size);
              expected_ = opts_.batch_size;
              VAST_DEBUG(this, "expects", expected_, "results as first batch");

              if (opts_.auto_follow)
                follow();
              else
                send(this, prompt_atom::value);
            },
            others() >> [=]
            {
              send(this, prompt_atom::value);
              VAST_ERROR(this, "got unexpected message:",
                         to_string(current_message()));
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

console::result::result(expression ast)
  : ast_{std::move(ast)}
{
}

void console::result::add(event e)
{
  auto i = std::lower_bound(events_.begin(), events_.end(), e);
  assert(i == events_.end() || e < *i);
  events_.insert(i, std::move(e));
}

size_t console::result::apply(size_t n, std::function<void(event const&)> f)
{
  size_t i = 0;
  while (i < n && pos_ < events_.size())
  {
    f(events_[pos_++]);
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

expression const& console::result::ast() const
{
  return ast_;
}

size_t console::result::size() const
{
  return events_.size();
}

void console::result::hits(uint64_t n)
{
  hits_ = n;
}

uint64_t console::result::hits() const
{
  return hits_;
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

void console::on_exit()
{
  connected_.clear();
  search_ = invalid_actor;
  keystroke_monitor_ = invalid_actor;
}

behavior console::make_behavior()
{
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

  keystroke_monitor_ = spawn<keystroke_monitor, detached+linked>(this);

  return
  {
    [=](down_msg const& msg)
    {
      if (msg.source == search_.address())
      {
        print(fail) << "search terminated" << std::endl;
        quit(exit::error);
      }
      else
      {
        VAST_DEBUG(this, "got DOWN from query", msg.source);
        remove(msg.source);
      }
    },
    [=](error const& e)
    {
      print(fail) << e << std::endl;
      prompt();
    },
    [=](done_atom)
    {
      VAST_DEBUG(this, "got done notification from query", current_sender());
      remove(current_sender());
    },
    [=](prompt_atom)
    {
      prompt();
    },
    [=](progress_atom, double progress, uint64_t hits)
    {
      auto i = connected_.find(current_sender());
      assert(i != connected_.end());

      auto r = i->second.second;
      assert(r);
      r->hits(hits);

      if (r->progress() <= progress + 0.05 || progress == 1.0)
      {
        if (following_)
        {
          auto base = r->progress();
          if (! appending_)
          {
            print(query)
              << "progress "
              << util::color::blue << '|' << util::color::reset;

            base = 0.0;
            appending_ = true;
          }

          print(none) << util::color::green;
          for (auto d = base; d < progress; d += 0.05)
            print(none) << '*';
          print(none) << util::color::reset << std::flush;

          if (progress == 1.0)
          {
            print(none)
              << util::color::green << '*'
              << util::color::blue << '|'
              << util::color::reset << ' ' << hits << " hits" << std::endl;

            appending_ = false;

            if (hits == 0)
              unfollow();
          }
        }

        r->progress(progress);
      }
    },
    [=](event& e)
    {
      auto i = connected_.find(current_sender());
      assert(i != connected_.end());

      auto r = i->second.second;
      assert(r);

      if (following_ && r == active_)
      {
        if (appending_)
        {
          print(none) << std::endl;
          appending_ = false;
        }

        std::cout << e << std::endl;

        if (expected_ > 0 && --expected_ == 0)
          send(this, key_atom::value, 's');
      }

      r->add(std::move(e));
    },
    [=](key_atom, char key)
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

            print(fail)
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
              << "        j     seek one batch forward\n"
              << "        k     seek one batch backword\n"
              << "        s     show query status\n"
              << "        q     leave query control mode\n"
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
            assert(active_);
            auto n = active_->apply(
                opts_.batch_size,
                [](event const& e) { std::cout << e << std::endl; });

            if (n == 0)
              print(query) << "reached end of results" << std::endl;
          }
          break;
        case 'e':
          {
            bool found = false;
            for (auto& p : connected_)
              if (p.second.second == active_)
              {
                found = true;
                send(p.second.first, extract_atom::value, opts_.batch_size);
                print(query)
                  << "asks for " << opts_.batch_size
                  << " more results" << std::endl;

                expected_ += opts_.batch_size;
              }

            if (! found)
              print(query) << "not connected to query" << std::endl;
          }
          break;
        case 'j':
          {
            assert(active_);
            auto n = active_->seek_forward(opts_.batch_size);
            print(query) << "seeked +" << n << " events" << std::endl;
          }
          break;
        case 'k':
          {
            assert(active_);
            auto n = active_->seek_backward(opts_.batch_size);
            print(query) << "seeked -" << n << " events" << std::endl;
          }
          break;
        case EOF:
        case '':
        case 'q':
          {
            unfollow();
          }
          return;
        case 's':
          {
            assert(active_);

            print(query)
              << "status: "
              << active_->size() << '/' << active_->hits() <<  " hits, "
              << active_->percent() << "% ";

            print(none)
              << util::color::blue << '|' << util::color::green;

            auto p = static_cast<int>(active_->percent(0) / 5);
            for (int i = 0; i < p; ++i)
              print(none) << '*';
            for (int i = 0; i < 20 - p; ++i)
              print(none) << ' ';

            print(none)
              << util::color::blue << '|' << util::color::reset << ' ' << std::endl;
          }
          break;
      }

      send(keystroke_monitor_, get_atom::value);
    }
  };
}

std::ostream& console::print(print_mode mode)
{
  if (mode == none)
    return std::cerr;

  if (appending_)
  {
    std::cerr << std::endl;
    appending_ = false;
  }

  switch (mode)
  {
    default:
      std::cerr << util::color::red << "[???] ";
      break;
    case fail:
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
        << util::color::cyan << "[" << active_->id() << "] ";
      break;
  }

  std::cerr << util::color::reset;

  return std::cerr;
}

void console::prompt(size_t ms)
{
  if (ms > 0)
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));

  std::string line;
  auto t = cmdline_.get(line);
  if (! t)
  {
    VAST_ERROR(this, "failed to retrieve command line:", t.error());
    quit(exit::error);
    return;
  }

  // Check for CTRL+D.
  if (! *t)
  {
    print(none) << std::endl;
    if (cmdline_.mode_pop() > 0)
      prompt();
    else
      send_exit(address(), exit::stop);
    return;
  }

  if (line.empty())
  {
    prompt();
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
    prompt();
  }
  else if (r.failed())
  {
    print(fail) << r.error() << std::endl;
    prompt();
  }
}

void console::follow()
{
  following_ = true;
  send(keystroke_monitor_, start_atom::value);
}

void console::unfollow()
{
  following_ = false;
  send(keystroke_monitor_, stop_atom::value);
  prompt();
}

void console::remove(actor_addr const& doomed)
{
  if (! connected_.count(doomed))
    return;
  if (active_->size() == 0)
    unfollow();
  connected_.erase(doomed);
}

} // namespace vast
