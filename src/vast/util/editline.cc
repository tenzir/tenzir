#include "vast/util/editline.h"

#include <histedit.h>

namespace vast {
namespace util {

struct editline::impl
{
  EditLine* el;
};

editline::editline(char const* name)
  : impl_{new impl}
{
  impl_->el = el_init(name, stdin, stdout, stderr);
}

editline::~editline()
{
  el_end(impl_->el);
}

bool editline::source(char const* filename)
{
  return el_source(impl_->el, filename) != -1;
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

void editline::put(std::string const& str)
{
  if (str.empty())
    return;
  el_push(impl_->el, str.c_str());
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

} // namespace util
} // namespace vast
