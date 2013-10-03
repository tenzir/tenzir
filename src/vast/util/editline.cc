#include "vast/util/editline.h"

#include <cassert>
#include <iostream>
#include <map>
#include <vector>
#include <histedit.h>
#include "vast/logger.h"

namespace vast {
namespace util {

struct editline::impl
{
  static char* prompt_function(EditLine* el)
  {
    impl* instance;
    el_get(el, EL_CLIENTDATA, &instance);
    return const_cast<char*>(instance->prompt_str.c_str());
  }

  static unsigned char handle_completion(EditLine* el, int)
  {
    impl* instance;
    el_get(el, EL_CLIENTDATA, &instance);
    auto snippet = instance->cursor_line();
    std::vector<std::string> matches;
    for (auto& p : instance->completions)
    {
      auto& full = p.first;
      if (snippet.size() >= full.size())
        continue;
      auto result = std::mismatch(snippet.begin(), snippet.end(), full.begin());
      if (result.first == snippet.end())
        matches.push_back(full);
    }
    if (matches.empty())
      return CC_REFRESH_BEEP;
    if (matches.size() == 1)
    {
      instance->insert(matches.front().substr(snippet.size()));
    }
    else
    {
      std::cout << '\n';
      for (auto& match : matches)
        std::cout << match << std::endl;
    }
    return CC_REDISPLAY;
  }

  impl(char const* name, char const* comp_key = "\t")
    : completion_key{comp_key}
  {
    // Bring editline to life.
    el = el_init(name, stdin, stdout, stderr);
    hist = history_init();
    assert(el != nullptr);
    assert(hist != nullptr);
    el_set(el, EL_PREP_TERM, 1);
    //el_set(el, EL_UNBUFFERED, 1);

    // Sane defaults.
    el_set(el, EL_EDITOR, "vi");

    // Make ourselves available in callbacks.
    el_set(el, EL_CLIENTDATA, this);

    // Setup history.
    history(hist, &hist_event, H_SETSIZE, 1000);
    history(hist, &hist_event, H_SETUNIQUE, 1);
    el_set(el, EL_HIST, history, hist);

    // Setup completion.
    el_set(el, EL_ADDFN, "vast-complete", "VAST complete", &handle_completion);
    el_set(el, EL_BIND, completion_key, "vast-complete", NULL);

    // Setup prompt.
    el_set(el, EL_PROMPT, &prompt_function);
  }

  ~impl()
  {
    el_set(el, EL_PREP_TERM, 0);
    history_end(hist);
    el_end(el);
  }

  bool source(char const* filename)
  {
    return el_source(el, filename) != -1;
  }

  bool complete(std::string cmd, std::string desc)
  {
    if (completions.count(cmd))
      return false;
    el_set(el, EL_ADDFN, cmd.c_str(), desc.c_str(), &handle_completion);
    completions.emplace(std::move(cmd), std::move(desc));
    return true;
  }

  void prompt(std::string str)
  {
    prompt_str = std::move(str);
  }

  bool get(char& c)
  {
    return el_getc(el, &c) == 1;
  }

  bool get(std::string& line)
  {
    int n;
    auto str = el_gets(el, &n);
    if (n == -1)
      return false;
    if (str == nullptr)
      line.clear();
    else
      line.assign(str);
    if (! line.empty() && (line.back() == '\n' || line.back() == '\r'))
      line.pop_back();
    return true;
  }

  void insert(std::string const& str)
  {
    el_insertstr(el, str.c_str());
  }

  size_t cursor()
  {
    auto info = el_line(el);
    return info->cursor - info->buffer;
  }

  std::string line()
  {
    auto info = el_line(el);
    return {info->buffer, info->lastchar - info->buffer};
  }

  std::string cursor_line()
  {
    auto info = el_line(el);
    return {info->buffer, info->cursor - info->buffer};
  }

  void reset()
  {
    el_reset(el);
  }

  void resize()
  {
    el_resize(el);
  }

  void beep()
  {
    el_beep(el);
  }

  void history_add(std::string const& str)
  {
    history(hist, &hist_event, H_ADD, str.c_str());
  }

  void history_append(std::string const& str)
  {
    history(hist, &hist_event, H_APPEND, str.c_str());
  }

  void history_enter(std::string const& str)
  {
    history(hist, &hist_event, H_ENTER, str.c_str());
  }

  EditLine* el;
  History* hist;
  HistEvent hist_event;
  char const* completion_key;
  std::map<std::string, std::string> completions;
  std::string prompt_str{">> "};
};

editline::editline(char const* name)
  : impl_{new impl{name}}
{
}

editline::~editline()
{
}

bool editline::source(char const* filename)
{
  return impl_->source(filename);
}

bool editline::complete(std::string cmd, std::string desc)
{
  return impl_->complete(std::move(cmd), std::move(desc));
}

void editline::prompt(std::string str)
{
  impl_->prompt(std::move(str));
}

bool editline::get(char& c)
{
  return impl_->get(c);
}

bool editline::get(std::string& line)
{
  return impl_->get(line);
}

void editline::put(std::string const& str)
{
  impl_->insert(str);
}

std::string editline::line()
{
  return impl_->line();
}

size_t editline::cursor()
{
  return impl_->cursor();
}

void editline::history_add(std::string const& str)
{
  impl_->history_add(str);
}

void editline::history_append(std::string const& str)
{
  impl_->history_append(str);
}

void editline::history_enter(std::string const& str)
{
  impl_->history_enter(str);
}

} // namespace util
} // namespace vast
