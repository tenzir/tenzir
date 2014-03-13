#include "vast/util/command_line.h"
#include "vast/util/color.h"

namespace vast {
namespace util {

command_line::command::command(intrusive_ptr<mode> mode,
                               intrusive_ptr<command> parent,
                               std::string name,
                               std::string desc)
  : mode_{std::move(mode)},
    parent_{std::move(parent)},
    name_{std::move(name)},
    description_{std::move(desc)}
{
  if (parent_)
    mode_->complete(absolute_name() + ' ');
}

intrusive_ptr<command_line::command>
command_line::command::add(std::string name, std::string desc)
{
  if (name.empty())
    return {};

  for (auto& cmd : commands_)
    if (cmd->name_ == name)
      return {};

  commands_.push_back(
      make_intrusive<command>(mode_, this, std::move(name), std::move(desc)));

  return commands_.back();
}


void command_line::command::on(callback f)
{
  handler_ = f;
}

result<bool> command_line::command::execute(std::string args) const
{
  auto delim = args.find(' ');
  auto name = args.substr(0, delim);
  auto rest = delim == std::string::npos ? "" : args.substr(++delim);
  for (auto& cmd : commands_)
    if (cmd->name_ == name)
      return cmd->execute(std::move(rest));

  if (! handler_)
    return error{"no handler registered for command: " + args};

  return handler_(std::move(args));
}

std::string const& command_line::command::name() const
{
  return name_;
}

std::string command_line::command::absolute_name() const
{
  // The root command has no name.
  if (! parent_)
    return "";

  std::vector<std::string> names{name_};
  auto parent = parent_;
  while (parent->parent_)
  {
    names.push_back(parent->name_);
    parent = parent->parent_;
  }

  assert(! names.empty());

  std::string fqn;
  for (auto i = names.rbegin(); i != names.rend(); ++i)
    fqn += *i + ' ';

  fqn.pop_back();

  return fqn;
}

std::string command_line::command::help(size_t indent) const
{
  std::string str;

  if (commands_.empty())
    return str;

  size_t max_len = 0;
  for (auto& cmd : commands_)
    if (cmd->name_.size() > max_len)
      max_len = cmd->name_.size();

  for (auto& cmd : commands_)
    str += (indent == 0 ? "" : std::string(indent, ' '))
      + cmd->name_
      + std::string(max_len - cmd->name_.size() + 2, ' ')
      + cmd->description_
      + '\n';

  return str;
}


command_line::mode::mode(std::string name,
                         std::string prompt,
                         char const* prompt_color,
                         std::string history_file)
  : history_{1000, true, std::move(history_file)}
{
  el_.source();
  el_.set(history_);
  if (! prompt.empty())
    el_.set(editline::prompt{std::move(prompt), prompt_color});

  root_ = make_intrusive<command>(this, nullptr, std::move(name), "");
}

intrusive_ptr<command_line::command>
command_line::mode::add(std::string name, std::string desc)
{
  return root_->add(std::move(name), std::move(desc));
}

void command_line::mode::on_unknown_command(callback f)
{
  root_->on(f);
}

void command_line::mode::on_complete(editline::completer::callback f)
{
  el_.completion().on(f);
}

void command_line::mode::complete(std::string str)
{
  el_.completion().add(std::move(str));
}

void command_line::mode::complete(std::vector<std::string> completions)
{
  el_.completion().replace(std::move(completions));
}

result<bool> command_line::mode::execute(std::string args) const
{
  return root_->execute(std::move(args));
}

std::string const& command_line::mode::name() const
{
  return root_->name();
}

std::string command_line::mode::help(size_t indent) const
{
  return root_->help(indent);
};


intrusive_ptr<command_line::mode>
command_line::mode_add(std::string name,
                       std::string prompt,
                       char const* prompt_color,
                       std::string history_file)
{
  if (modes_.count(name))
    return {};

  auto m = make_intrusive<mode>(std::move(name),
                                std::move(prompt),
                                prompt_color,
                                std::move(history_file));

  return modes_.emplace(m->root_->name(), m).first->second;
}

bool command_line::mode_rm(std::string const& name)
{
  if (! modes_.count(name))
    return false;

  modes_.erase(name);

  return true;
}

bool command_line::mode_push(std::string const& mode)
{
  if (! modes_.count(mode))
    return false;

  mode_stack_.push_back(modes_[mode]);

  return true;
}

size_t command_line::mode_pop()
{
  mode_stack_.pop_back();
  return mode_stack_.size();
}

bool command_line::append_to_history(std::string const& entry)
{
  if (mode_stack_.empty())
    return false;

  mode_stack_.back()->history_.enter(entry);
  mode_stack_.back()->history_.save();

  return true;
}

result<bool> command_line::process(std::string cmd)
{
  if (mode_stack_.empty())
    return error{"mode stack empty"};

  return mode_stack_.back()->execute(std::move(cmd));
}

trial<bool> command_line::get(char& c)
{
  if (mode_stack_.empty())
    return error{"mode stack empty"};

  return mode_stack_.back()->el_.get(c);
}

trial<bool> command_line::get(std::string& line)
{
  if (mode_stack_.empty())
    return error{"mode stack empty"};

  // Fixes TTY weirdness which may occur when switching between modes.
  mode_stack_.back()->el_.reset();

  auto t = mode_stack_.back()->el_.get(line);
  if (! t || ! *t)
    return t;

  // Trim line from leading/trailing whitespace.
  auto first_non_ws = line.find_first_not_of(" \t");
  auto last_non_ws = line.find_last_not_of(" \t");
  if (first_non_ws != std::string::npos)
    line = line.substr(first_non_ws, last_non_ws - first_non_ws + 1);

  return t;
}

} // namespace util
} // namespace vast
