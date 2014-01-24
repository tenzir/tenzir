#ifndef VAST_UTIL_CONFIGURATION_H
#define VAST_UTIL_CONFIGURATION_H

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include "vast/util/trial.h"

namespace vast {
namespace util {

/// A command line parser and program option utility.
template <typename Derived>
class configuration
{
public:
  struct error : util::error
  {
    using util::error::error;

    error(std::string msg, char c)
      : util::error{msg + " (-" + c + ')'}
    {
    }
    error(std::string msg, std::string opt)
      : util::error{msg + " (--" + opt + ')'}
    {
    }
  };

  /// Initializes the configuration from a configuration file.
  /// @param filename The name of the configuration file.
  /// @returns An engaged trial on success.
  static trial<Derived> parse(std::string const& /* filename */)
  {
    return error{"function not yet implemented"};
  }

  /// Initializes the configuration from command line parameters.
  /// @argc The argc parameter from main.
  /// @param argv The argv parameter from main.
  static trial<Derived> parse(int argc, char *argv[])
  {
    Derived cfg;

    // Although we don't like to use exceptions, for the "configuration DSL" we
    // prefer a monadic style to declare our program and hence have to fall
    // back to exceptions.
    try
    {
      cfg.initialize();
    }
    catch (std::logic_error const& e)
    {
      return error{e.what()};
    }

    for (int i = 1; i < argc; ++i)
    {
      std::vector<std::string> values;
      std::string arg(argv[i]);
      if (arg.size() < 2)
      {
        // We need at least a '-' followed by one character.
        return error{"ill-formed option specificiation", argv[i]};
      }
      else if (arg[0] == '-' && arg[1] == '-')
      {
        // Argument must begin with '--'.
        if (arg.size() == 2)
          return error{"ill-formed option specification", argv[i]};
        arg = arg.substr(2);
      }
      else if (arg[0] == '-')
      {
        if (arg.size() > 2)
          // The short option comes with a value.
          values.push_back(arg.substr(2));
        arg = arg[1];
        auto s = cfg.shortcuts_.find(arg);
        if (s == cfg.shortcuts_.end())
          return error{"unknown short option", arg[0]};
        arg = s->second;
      }

      auto o = cfg.find_option(arg);
      if (! o)
        return error{"unknown option", arg};
      o->defaulted_ = false;

      while (i+1 < argc && std::strlen(argv[i+1]) > 0 && argv[i+1][0] != '-')
        values.emplace_back(argv[++i]);
      if (values.size() > o->max_vals_)
        return error{"too many values", arg};
      if (o->max_vals_ == 1 && values.size() != 1)
        return error{"option value required", arg};
      if (! values.empty())
        o->values_ = std::move(values);
    }

    if (! cfg.verify())
      return error{"configuration verification failed"};

    return {std::move(cfg)};
  }

  /// Checks whether the given option is set.
  /// @param opt Name of the option to check.
  /// @returns `true` if *opt* is set.
  bool check(std::string const& opt) const
  {
    auto o = find_option(opt);
    return o && ! o->defaulted_;
  }

  /// Returns the value of the given option.
  /// @param opt The name of the option.
  /// @returns The option value.
  trial<std::string> get(std::string const& opt) const
  {
    auto o = find_option(opt);
    if (! o)
      return error{"option does not exist"};
    if (o->values_.empty())
      return error{"option has no value"};
    if (o->max_vals_ > 1)
      return error{"cannot get multi-value option"};

    assert(o->values_.size() == 1);
    return o->values_.front();
  }

  /// Retrieves an option as a specific type.
  /// @tparam T The type to convert the option to.
  /// @param opt The name of the option.
  /// @returns The converted option value.
  template <typename T>
  trial<T> as(std::string const& opt) const
  {
    auto o = find_option(opt);
    if (! o)
      return error{"unknown option", opt};

    return dispatch<T>(*o, std::is_same<T, std::vector<std::string>>());
  }

  /// Prints the usage into a given output stream.
  /// @param sink The output stream to receive the configuration.
  /// @param show_all Whether to also print invisible options.
  void usage(std::ostream& sink, bool show_all = false)
  {
    sink << derived()->banner() << "\n";

    for (auto& b : blocks_)
    {
      if (! show_all && ! b.visible_)
        continue;

      sink << "\n " << b.name_ << ":\n";

      auto has_shortcut = std::any_of(
          b.options_.begin(),
          b.options_.end(),
          [](option const& o) { return o.shortcut_ != '\0'; });

      auto max = std::max_element(
          b.options_.begin(),
          b.options_.end(),
          [](option const& o1, option const& o2)
          {
            return o1.name_.size() < o2.name_.size();
          });

      auto max_len = max->name_.size();
      for (auto& opt : b.options_)
      {
        sink << "   --" << opt.name_;
        sink << std::string(max_len - opt.name_.size(), ' ');
        if (has_shortcut)
          sink << (opt.shortcut_
                   ? std::string(" | -") + opt.shortcut_
                   : "     ");

        sink << "   " << opt.description_ << "\n";
      }
    }

    sink << std::endl;
  }
  
protected:
  class option
  {
    friend configuration;
  public:
    option(std::string name, std::string desc, char shortcut = '\0')
      : name_{std::move(name)},
        description_{std::move(desc)},
        shortcut_{shortcut}
    {
    }

    template <typename T>
    option& init(T const& x)
    {
      std::ostringstream ss;
      ss << x;
      values_.push_back(ss.str());
      max_vals_ = (values_.size() == 1) ? 1 : -1;
      return *this;
    }

    template<class T, typename... Args>
    option& init(T const& head, Args... tail)
    {
      init(head);
      init(tail...);
      return *this;
    }

    option& multi(size_t n = -1)
    {
      max_vals_ = n;
      return *this;
    }

    option& single()
    {
      return multi(1);
    }

  private:
    std::string name_;
    std::vector<std::string> values_;
    std::string description_;
    size_t max_vals_ = 0;
    bool defaulted_ = true;
    char shortcut_ = '\0';
  };

  /// A proxy class to add options to the configuration.
  class block
  {
    friend class configuration;
    block(block const&) = delete;
    block& operator=(block other) = delete;

  public:
    /// Separates hierarchical options.
    static constexpr char const* separator = ".";

    /// Move-constructs a block.
    /// @param other The block to move.
    block(block&& other)
      : visible_{other.visible_},
        name_{std::move(other.name_)},
        prefix_{std::move(other.prefix_)},
        options_{std::move(other.options_)},
        config_{other.config_}
    {
      other.visible_ = true;
      other.config_ = nullptr;
    }

    /// Adds a new option.
    /// @param name The option name.
    /// @param desc The option description.
    option& add(std::string const& name, std::string desc)
    {
      std::string fqn = qualify(name);
      if (config_->find_option(fqn))
        throw std::logic_error{"duplicate option"};
      options_.emplace_back(std::move(fqn), std::move(desc));
      return options_.back();
    }

    /// Adds a new option with shortcut.
    /// @param shortcut The shortcut of the option (single character).
    /// @param name The option name.
    /// @param desc The option description.
    option& add(char shortcut, std::string const& name, std::string desc)
    {
      if (config_->shortcuts_.count({shortcut}))
        throw std::logic_error{"duplicate shortcut"};
      std::string fqn = qualify(name);
      config_->shortcuts_.insert({{shortcut}, fqn});
      if (config_->find_option(fqn))
        throw std::logic_error{"duplicate option"};
      options_.emplace_back(std::move(fqn), std::move(desc), shortcut);
      return options_.back();
    }


    /// Sets the visibility of this block when displaying the usage.
    bool visible() const
    {
      return visible_;
    }

    /// Sets the visibility of this block when displaying the usage.
    void visible(bool flag)
    {
      visible_ = flag;
    }

  private:
    block(std::string name, std::string prefix, configuration* config)
      : name_{std::move(name)},
        prefix_{std::move(prefix)},
        config_{config}
    {
    }

    std::string qualify(std::string const& name) const
    {
      return prefix_.empty() ? name : prefix_ + separator + name;
    }

    bool visible_ = true;
    std::string name_;
    std::string prefix_;
    std::vector<option> options_;
    configuration* config_;
  };

  /// Default-constructs a configuration.
  configuration() = default;

  /// Creates a new option block.
  /// @param name The name of the option block.
  /// @param prefix The prefix of the block.
  /// @returns The option block.
  block& create_block(std::string name, std::string prefix = "")
  {
    block b{std::move(name), std::move(prefix), this};
    blocks_.push_back(std::move(b));
    return blocks_.back();
  }

  /// Verifies that two given options are not specified at the same time.
  /// @param opt1 The first option.
  /// @param opt2 The second option.
  void add_conflict(std::string opt1, std::string opt2)
  {
    conflicts_.emplace(std::move(opt1), std::move(opt2));
  }

  /// Adds a disjunction of option dependencies.
  ///
  /// @param needy The option that depends on *required*.
  ///
  /// @param required A set of options where at least one must exist with
  /// when needy is given.
  ///
  /// @returns `true` iff *needy* and one or more optiosn in *required* exist.
  void add_dependencies(std::string needy, std::vector<std::string> required)
  {
    dependencies_.emplace(std::move(needy), std::move(required));
  }

  /// Adds an option dependency.
  /// @param needy The option that depends on *required*.
  /// @param required The option that must exist when *needy* exists.
  /// @returns `true` iff *required* and *needy* exists.
  void add_dependency(std::string needy, std::string required)
  {
    add_dependencies(std::move(needy), {std::move(required)});
  }

private:
  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  bool verify() const
  {
    for (auto& p : conflicts_)
      if (check(p.first) && check(p.second))
        return false;

    for (auto& p : dependencies_)
    {
      auto any = std::any_of(
          p.second.begin(),
          p.second.end(),
          [&](std::string const& dep) { return check(dep); });

      if (check(p.first) && ! any)
        return false;
    }

    return true;
  }

  option* find_option(std::string const& opt)
  {
    for (auto& b : blocks_)
      for (size_t i = 0; i < b.options_.size(); ++i)
        if (b.options_[i].name_ == opt)
          return &b.options_[i];

    return nullptr;
  }

  option const* find_option(std::string const& opt) const
  {
    for (auto& b : blocks_)
      for (size_t i = 0; i < b.options_.size(); ++i)
        if (b.options_[i].name_ == opt)
          return &b.options_[i];

    return nullptr;
  }

  template <typename T>
  trial<T> dispatch(option const& opt, std::true_type) const
  {
    return {opt.values_};
  }

  template <typename T>
  trial<T> dispatch(option const& opt, std::false_type) const
  {
    if (opt.values_.empty())
      return error{"option has no value", opt.name_};

    if (opt.max_vals_ > 1)
      return error{"cannot cast multi-value option", opt.name_};

    T x;
    std::istringstream ss(opt.values_.front());
    ss >> x;

    return {std::move(x)};
  }

  std::vector<block> blocks_;
  std::map<std::string, std::string> shortcuts_;
  std::multimap<std::string, std::string> conflicts_;
  std::multimap<std::string, std::vector<std::string>> dependencies_;
};

} // namespace util
} // namespace vast

#endif
