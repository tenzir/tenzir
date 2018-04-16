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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <caf/actor_system_config.hpp>
#include <caf/fwd.hpp>
#include <caf/message.hpp>

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

  /// Maps names of config parameters to their value.
  using option_map = std::map<std::string, caf::config_value>;

  /// Iterates over CLI arguments.
  using const_iterator = std::vector<std::string>::const_iterator;

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
  int run(caf::actor_system& sys, const_iterator args_begin,
          const_iterator args_end);

  /// Runs the command and blocks until execution completes.
  /// @returns An exit code suitable for returning from main.
  int run(caf::actor_system& sys, option_map& options,
          const_iterator args_begin, const_iterator args_end);

  /// Prints usage to `std::cerr`.
  void usage();

  /// Returns the full name for this command.
  std::string full_name();

  /// Returns the name for this command.
  std::string name();

  /// Queries whether this command has no parent.
  bool is_root() const noexcept;

  /// Queries whether this command has no parent.
  command& root() noexcept {
    return is_root() ? *this : parent_->root();
  }

  inline const std::string_view& name() const noexcept {
    return name_;
  }

  /// Defines a sub-command.
  /// @param name The name of the command.
  /// @param xs The parameters required to construct the command.
  template <class T, class... Ts>
  T* add(std::string_view name, Ts&&... xs) {
    auto ptr = std::make_unique<T>(this, name, std::forward<Ts>(xs)...);
    auto result = ptr.get();
    if (!nested_.emplace(name, std::move(ptr)).second) {
      // FIXME: do not use exceptions.
      throw std::invalid_argument("name already exists");
    }
    return result;
  }

  template <class T>
  caf::optional<T> get(const option_map& xs, const std::string& name) {
    // Map T to the clostest type in config_value.
    using cfg_type =
      typename std::conditional<
        std::is_integral_v<T> && !std::is_same_v<bool, T>,
        int64_t,
        typename std::conditional<
          std::is_floating_point_v<T>,
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
  T get_or(const option_map& xs, const std::string& name, T fallback) {
    auto result = get<T>(xs, name);
    if (!result)
      return fallback;
    return *result;
  }

protected:
  /// Checks whether a command is ready to proceed, i.e., whether the
  /// configuration allows for calling `run_impl` or `run` on a nested command.
  virtual proceed_result proceed(caf::actor_system& sys, option_map& options,
                                 const_iterator args_begin,
                                 const_iterator args_end);

  virtual int run_impl(caf::actor_system& sys, option_map& options,
                       caf::message args);

  template <class T>
  void add_opt(std::string name, std::string descr, T& ref) {
    opts_.emplace_back(name, std::move(descr), ref);
    // Extract the long name from the full name (format: "long,l").
    auto pos = name.find_first_of(',');
    if (pos < name.size())
      name.resize(pos);
    kvps_.emplace_back([name = std::move(name), &ref] {
      // Map T to the clostest type in config_value.
      using cfg_type =
        typename std::conditional<
          std::is_integral_v<T> && !std::is_same_v<bool, T>,
          int64_t,
          typename std::conditional<
            std::is_floating_point_v<T>,
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
  std::tuple<caf::message, std::string, caf::message, size_t>
  separate_args(const caf::message& args);

  std::map<std::string_view, std::unique_ptr<command>> nested_;
  command* parent_;

  /// The user-provided name.
  std::string_view name_;

  /// List of all accepted options.
  std::vector<caf::message::cli_arg> opts_;

  /// List of function objects that return CLI options as name/value pairs.
  std::vector<std::function<std::pair<std::string, caf::config_value>()>> kvps_;
};

} // namespace vast

