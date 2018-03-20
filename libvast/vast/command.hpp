/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_COMMAND_HPP
#define VAST_COMMAND_HPP

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <caf/actor_system_config.hpp>
#include <caf/fwd.hpp>
#include <caf/message.hpp>

#include <caf/detail/unordered_flat_map.hpp>

#include "vast/error.hpp"
#include "vast/data.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#include "vast/detail/steady_map.hpp"
#include "vast/detail/string.hpp"

namespace vast {

/// A top-level command.
class command {
public:
  // -- member types -----------------------------------------------------------

  /// An option of a command.
  struct option {
    template <class T = bool>
    static std::pair<std::string, option>
    make(const std::string& tag, std::string desc, T x = {}) {
      auto s = detail::split_to_str(tag, ",");
      std::string shortcut;
      if (s.size() >= 2)
        shortcut = s[1][0];
      return {s[0], {std::move(shortcut), std::move(desc),
                    data{std::forward<T>(x)}}};
    }

    std::string shortcut;
    std::string description;
    data value;
  };

  /// Owning pointer to a command.
  using unique_ptr = std::unique_ptr<command>;

  /// Group of configuration parameters.
  using opt_group = caf::actor_system_config::opt_group;

  /// Maps names of config parameters to their value.
  using opt_map = std::map<std::string, caf::config_value>;

  /// Returns a CLI option as name/value pair.
  using get_opt = std::function<std::pair<std::string, caf::config_value>()>;

  /// Wraps the result of proceed.
  enum proceed_result {
    proceed_ok,
    stop_successful,
    stop_with_error
  };

  // -- constructors, destructors, and assignment operators --------------------

  command();

  command(command* parent, std::string_view name);

  virtual ~command();

  /// Runs the command and blocks until execution completes.
  /// @returns An exit code suitable for returning from main.
  int run(caf::actor_system& sys, caf::message args);

  /// Runs the command and blocks until execution completes.
  /// @returns An exit code suitable for returning from main.
  int run(caf::actor_system& sys, opt_map& options, caf::message args);

  /// Prints usage to `std::cerr`.
  void usage();

  /// Defines a sub-command.
  /// @param name The name of the command.
  /// @param desc The description of the command.
  command& cmd(const std::string& name, std::string desc = "");

  /// Parses command line arguments and dispatches the contained command to the
  /// registered sub-command.
  /// @param args The command line arguments.
  void dispatch(const std::vector<std::string>& args) const;

  /// Retrieves an option value.
  /// @param x The name of the option.
  /// @returns The value for *x* or `nullptr` if `x` is not a valid option.
  const data* get(const std::string& x) const;

  std::string description;
  detail::steady_map<std::string, option> options;

  /// Returns the full name for this command.
  std::string full_name();

  /// Queries whether this command has no parent.
  bool is_root() const noexcept;

  /// Queries whether this command has no parent.
  command& root() noexcept {
    return is_root() ? *this : parent_->root();
  }

  inline const std::string_view& name() const noexcept {
    return name_;
  }

  template <class T, class... Ts>
  T* add(std::string_view name, Ts&&... xs) {
    auto ptr = std::make_unique<T>(this, name, std::forward<Ts>(xs)...);
    auto result = ptr.get();
    if (!nested_.emplace(name, std::move(ptr)).second) {
      throw std::invalid_argument("name already exists");
    }
    return result;
  }

  template <class T>
  caf::optional<T> get(const opt_map& xs, const std::string& name) {
    // Map T to the clostest type in config_value.
    using cfg_type =
      typename std::conditional<
        std::is_integral<T>::value && !std::is_same<bool, T>::value,
        int64_t,
        typename std::conditional<
          std::is_floating_point<T>::value,
          double,
          T
          >::type
        >::type;
    auto i = xs.find(name);
    if (i == xs.end())
      return caf::none;
    auto result = caf::get_if<cfg_type>(&i->second);
    if (!result)
      return caf::none;
    return static_cast<T>(*result);
  }

  template <class T>
  T get_or(const opt_map& xs, const std::string& name, T fallback) {
    auto result = get<T>(xs, name);
    if (!result)
      return fallback;
    return *result;
  }

protected:
  /// Checks whether a command is ready to proceed, i.e., whether the
  /// configuration allows for calling `run_impl` or `run` on a nested command.
  virtual proceed_result proceed(caf::actor_system& sys, opt_map& options,
                                 caf::message args);

  virtual int run_impl(caf::actor_system& sys, opt_map& options,
                       caf::message args);

  template <class T>
  void add_opt(std::string name, std::string descr, T& ref) {
    opts_.emplace_back(name, std::move(descr), ref);
    // Extract the long name from the full name (format: "long,l").
    auto pos = name.find_first_of(',');
    if (pos < name.size())
      name.resize(pos);
    get_opts_.emplace_back([name = std::move(name), &ref] {
      // Map T to the clostest type in config_value.
      using cfg_type =
        typename std::conditional<
          std::is_integral<T>::value && !std::is_same<bool, T>::value,
          int64_t,
          typename std::conditional<
            std::is_floating_point<T>::value,
            double,
            T
            >::type
          >::type;
      cfg_type copy = ref;
      return std::make_pair(name, caf::config_value{std::move(copy)});
    });
  }

private:
  /// Separates arguments into the arguments for the current command, the name
  /// of the subcommand, and the arguments for the subcommand.
  std::tuple<caf::message, std::string, caf::message>
  separate_args(const caf::message& args);

  caf::detail::unordered_flat_map<std::string_view, unique_ptr> nested_;
  command* parent_;
  std::string_view name_;
  std::vector<caf::message::cli_arg> opts_;
  std::vector<get_opt> get_opts_;
};

} // namespace vast

#endif
