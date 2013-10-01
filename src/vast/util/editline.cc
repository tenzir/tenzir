#include "vast/util/editline.h"

#include <cassert>
#include <histedit.h>

namespace vast {
namespace util {

struct editline::impl
{
  char* prompt_function(EditLine*)
  {
    return const_cast<char*>(prompt.c_str());
  }

  EditLine* el;
  History* hist;
  HistEvent hist_event;
  std::string prompt{">>"};
};

editline::editline(char const* name)
  : impl_{new impl}
{
  impl_->el = el_init(name, stdin, stdout, stderr);
  impl_->hist = history_init();
  assert(impl_->el != nullptr);
  assert(impl_->hist != nullptr);

  // Setup history.
  history(impl_->hist, &impl_->hist_event, H_SETSIZE, 1000);
  el_set(impl_->el, EL_HIST, history, impl_->hist);

  // Setup prompt.
  el_set(impl_->el, EL_PROMPT, &impl::prompt_function);
}

editline::~editline()
{
  history_end(impl_->hist);
  el_end(impl_->el);
}

bool editline::source(char const* filename)
{
  return el_source(impl_->el, filename) != -1;
}

void editline::prompt(std::string str)
{
  impl_->prompt = std::move(str);
}

bool editline::get(char& c)
{
  return el_getc(impl_->el, &c) == 1;
}

bool editline::get(std::string& line)
{
  int n;
  auto str = el_gets(impl_->el, &n);
  if (n == -1)
    return false;
  if (str == nullptr)
    line.clear();
  else
    line.assign(str);
  return true;
}

void editline::put(char const* str)
{
  if (! str)
    return;
  el_push(impl_->el, str);
}

void editline::reset()
{
  el_reset(impl_->el);
}

void editline::resize()
{
  el_resize(impl_->el);
}

void editline::beep()
{
  el_beep(impl_->el);
}

void editline::history_add(char const* line)
{
  history(impl_->hist, &impl_->hist_event, H_ENTER, line);
}

} // namespace util
} // namespace vast
