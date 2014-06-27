#include "vast/util/editline.h"

#include <cassert>
#include <cerrno>
#include <cstring>
#include <histedit.h>
#include "vast/print.h"
#include "vast/util/color.h"

namespace vast {
namespace util {

struct editline::history::impl
{
  impl(int size, bool unique, std::string filename)
    : filename_{std::move(filename)}
  {
    hist = ::history_init();
    assert(hist != nullptr);
    ::history(hist, &hist_event, H_SETSIZE, size);
    ::history(hist, &hist_event, H_SETUNIQUE, unique ? 1 : 0);
    load();
  }

  ~impl()
  {
    save();
    ::history_end(hist);
  }

  void save()
  {
    if (! filename_.empty())
      ::history(hist, &hist_event, H_SAVE, filename_.c_str());
  }

  void load()
  {
    if (! filename_.empty())
      ::history(hist, &hist_event, H_LOAD, filename_.c_str());
  }

  void add(std::string const& str)
  {
    ::history(hist, &hist_event, H_ADD, str.c_str());
    save();
  }

  void append(std::string const& str)
  {
    ::history(hist, &hist_event, H_APPEND, str.c_str());
  }

  void enter(std::string const& str)
  {
    ::history(hist, &hist_event, H_ENTER, str.c_str());
  }

  History* hist;
  HistEvent hist_event;
  std::string filename_;
};


editline::history::history(int size, bool unique, std::string filename)
  : impl_{new impl{size, unique, std::move(filename)}}
{
}

editline::history::~history()
{
}

void editline::history::save()
{
  impl_->save();
}

void editline::history::load()
{
  impl_->load();
}

void editline::history::add(std::string const& str)
{
  impl_->add(str);
}

void editline::history::append(std::string const& str)
{
  impl_->append(str);
}

void editline::history::enter(std::string const& str)
{
  impl_->enter(str);
}

editline::prompt::prompt(std::string str, char const* color)
{
  push(std::move(str), color);
}

void editline::prompt::push(std::string str, char const* color)
{
  if (str.empty())
    return;

  if (color)
    str_ += color;

  str_ += std::move(str);

  if (color)
    str_ += util::color::reset;
}

char const* editline::prompt::display() const
{
  return str_.c_str();
}

bool editline::completer::add(std::string str)
{
  if (std::find(strings_.begin(), strings_.end(), str) != strings_.end())
    return false;

  strings_.push_back(std::move(str));
  return true;
}

bool editline::completer::remove(std::string const& str)
{
  if (std::find(strings_.begin(), strings_.end(), str) == strings_.end())
    return false;

  strings_.erase(
      std::remove(strings_.begin(), strings_.end(), str),
      strings_.end());

  return true;
}

void editline::completer::replace(std::vector<std::string> completions)
{
  strings_ = std::move(completions);
}

void editline::completer::on(callback f)
{
  callback_ = f;
};

trial<std::string> editline::completer::complete(std::string const& prefix) const
{
  if (! callback_)
    return error{"no completion handler registered"};

  if (strings_.empty())
    return error{"no completions registered"};

  std::vector<std::string> matches;
  for (auto& str : strings_)
  {
    if (prefix.size() >= str.size())
      continue;

    auto result = std::mismatch(prefix.begin(), prefix.end(), str.begin());
    if (result.first == prefix.end())
      matches.push_back(str);
  }

  return callback_(prefix, std::move(matches));
};

namespace {

// RAII enabling of editline settings.
struct scope_setter
{
  scope_setter(EditLine* el, int flag)
    : el{el}, flag{flag}
  {
    el_set(el, flag, 1);
  }

  ~scope_setter()
  {
    assert(el);
    el_set(el, flag, 0);
  }

  EditLine* el;
  int flag;
};


} // namespace <anonymous>

struct editline::impl
{
  static char* prompt_function(EditLine* el)
  {
    impl* instance;
    el_get(el, EL_CLIENTDATA, &instance);
    assert(instance);

    return const_cast<char*>(instance->prompt_.display());
  }

  static unsigned char handle_complete(EditLine* el, int)
  {
    impl* instance;
    el_get(el, EL_CLIENTDATA, &instance);
    assert(instance);

    auto line = instance->cursor_line();
    auto line_size = line.size();
    auto t = instance->completer_.complete(std::move(line));

    if (! t || t->empty())
      return CC_REFRESH_BEEP;

    instance->insert(t->substr(line_size));
    return CC_REDISPLAY;
  }

  static int handle_char_read(EditLine* el, char* c)
  {
    impl* instance;
    el_get(el, EL_CLIENTDATA, &instance);
    assert(instance);
    assert(instance->on_char_read_);

    return instance->on_char_read_(c);
  }

  impl(char const* name, char const* comp_key = "\t")
    : el_{el_init(name, stdin, stdout, stderr)},
      completion_key_{comp_key},
      on_char_read_{[&](char *c) { return read_char_from_input_stream(c); }}
  {
    assert(el_ != nullptr);

    // Make ourselves available in callbacks.
    el_set(el_, EL_CLIENTDATA, this);

    // Keyboard defaults.
    el_set(el_, EL_EDITOR, "vi");
    el_set(el_, EL_BIND, "^r", "em-inc-search-prev", NULL);
    el_set(el_, EL_BIND, "^w", "ed-delete-prev-word", NULL);

    // Setup completion.
    el_set(el_, EL_ADDFN, "vast-complete", "VAST complete", &handle_complete);
    el_set(el_, EL_BIND, completion_key_, "vast-complete", NULL);

    // FIXME: this is a fix for folks that have "bind -v" in their .editrc.
    // Most of these also have "bind ^I rl_complete" in there to re-enable tab
    // completion, which "bind -v" somehow disabled. A better solution to
    // handle this problem would be desirable.
    el_set(el_, EL_ADDFN, "rl_complete", "default complete", &handle_complete);

    // Let all character reads go through our custom handler so that we can
    // figure out when we receive EOF.
    el_set(el_, EL_GETCFN, &handle_char_read);
  }

  ~impl()
  {
    el_end(el_);
  }

  void on_char_read(std::function<int(char*)> handler)
  {
    assert(handler);
    on_char_read_ = handler;
  }

  int read_char_from_input_stream(char *c)
  {
    while (true)
    {
      errno = 0;
      char ch = ::fgetc(input_file_ptr());

      if (ch == '\x04' && empty_line())
      {
        errno = 0;
        ch = EOF;
      }

      if (ch == EOF)
      {
        if (errno == EINTR)
        {
          continue;
        }
        else
        {
          eof_ = true;
          break;
        }
      }
      else
      {
        *c = ch;
        return 1;
      }
    }

    return 0;
  }

  bool source(char const* filename)
  {
    return el_source(el_, filename) != -1;
  }

  void set(prompt p)
  {
    prompt_ = std::move(p);
    el_set(el_, EL_PROMPT, &prompt_function);
  }

  void set(history& hist)
  {
    el_set(el_, EL_HIST, ::history, hist.impl_->hist);
  }

  void set(completer comp)
  {
    completer_ = std::move(comp);
  }

  bool get(char& c)
  {
    return ! eof() && el_getc(el_, &c) == 1;
  }

  bool get(std::string& line)
  {
    if (eof())
      return false;

    line.clear();
    scope_setter ss{el_, EL_PREP_TERM};

    int n;
    auto str = el_gets(el_, &n);
    if (n == -1 || eof())
      return false;

    if (str == nullptr)
    {
      line.clear();
      return true;
    }

    while (n > 0 && (str[n - 1] == '\n' || str[n - 1] == '\r'))
      --n;

    line.assign(str, n);
    return true;
  }

  void push(char const* str)
  {
    el_push(el_, str);
  }

  void insert(std::string const& str)
  {
    el_insertstr(el_, str.c_str());
  }

  size_t cursor()
  {
    auto info = el_line(el_);
    return info->cursor - info->buffer;
  }

  std::string line()
  {
    auto info = el_line(el_);
    return {info->buffer,
            static_cast<std::string::size_type>(info->lastchar - info->buffer)};
  }

  std::string cursor_line()
  {
    auto info = el_line(el_);
    return {info->buffer,
            static_cast<std::string::size_type>(info->cursor - info->buffer)};
  }

  bool empty_line()
  {
    auto info = el_line(el_);
    return info->buffer == info->cursor && info->buffer == info->lastchar;
  }

  void reset()
  {
    el_reset(el_);
  }

  void resize()
  {
    el_resize(el_);
  }

  void beep()
  {
    el_beep(el_);
  }

  FILE* file_ptr(int fd)
  {
    FILE* f = nullptr;
    return el_get(el_, EL_GETFP, fd, &f) == 0 ? f : nullptr;
  }

  FILE* input_file_ptr()
  {
    return file_ptr(0);
  }

  FILE* output_file_ptr()
  {
    return file_ptr(1);
  }

  FILE* error_file_ptr()
  {
    return file_ptr(2);
  }

  bool eof()
  {
    return eof_;
  }

  void unset_eof()
  {
    eof_ = false;
  }

  EditLine* el_;
  char const* completion_key_;
  prompt prompt_;
  completer completer_;
  std::function<int(char*)> on_char_read_;
  bool eof_ = false;
};

editline::editline(char const* name)
  : impl_{new impl{name}}
{
}

editline::~editline()
{
}

void editline::on_char_read(std::function<int(char*)> handler)
{
  return impl_->on_char_read(handler);
}

bool editline::source(char const* filename)
{
  return impl_->source(filename);
}

void editline::set(prompt p)
{
  impl_->set(std::move(p));
}

void editline::set(history& hist)
{
  impl_->set(hist);
}

void editline::set(completer comp)
{
  impl_->set(comp);
}

trial<bool> editline::get(char& c)
{
  auto success = impl_->get(c);
  if (success)
    return true;

  if (eof())
  {
    impl_->unset_eof();
    return false;
  }

  return error{std::strerror(errno)};
}

trial<bool> editline::get(std::string& line)
{
  auto success = impl_->get(line);
  if (success)
    return true;

  if (eof())
  {
    impl_->unset_eof();
    return false;
  }

  return error{std::strerror(errno)};
}

void editline::push(char const* str)
{
  return impl_->push(str);
}

void editline::put(std::string const& str)
{
  impl_->insert(str);
}

bool editline::eof()
{
  return impl_->eof();
}

std::string editline::line()
{
  return impl_->line();
}

size_t editline::cursor()
{
  return impl_->cursor();
}

void editline::reset()
{
  impl_->reset();
}

editline::completer& editline::completion()
{
  return impl_->completer_;
}

} // namespace util
} // namespace vast
