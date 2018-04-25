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

#include <string>
#include <string_view>
#include <vector>

#include "vast/optional.hpp"
#include "vast/expected.hpp"
#include "vast/data.hpp"

#include "vast/detail/steady_map.hpp"

namespace vast {

class option_map;

// FIXME: Use string_view instead of const std::string& where apropiate.
// The Steady_map currently does not allow to use string_views to search for
// strings

/// A set of `option_declarations` that can fill an `option_map` from a CLI
/// string.
class option_declaration_set {
public:
  /// A declaration of a CLI argument option.
  class option_declaration {
  public:
    /// Constructs a declation of an option.
    /// @param long_name A long name that identifies this option.
    /// @param short_names A vector of short name that identifies this option.
    /// @param description A decprition to this option.
    /// @param has_argument A flag that describes whether this option has an
    /// argument.
    /// @param default_value A value that is used when the option is not set by
    /// a user.
    option_declaration(std::string long_name, std::vector<char> short_names,
                       std::string description, bool has_argument,
                       data default_value);

    /// Returns the long name.
    inline const auto& long_name() const {
      return long_name_;
    }

    /// Returns a vector short names.
    inline const auto& short_names() const {
      return short_names_;
    }

    /// Returns a description.
    inline const auto& description() const {
      return description_;
    }

    /// Checks whether this option requires an argument.
    inline auto has_argument() const {
      return has_argument_;
    }

    /// Returns the default value.
    inline const auto& default_value() const {
      return default_value_;
    }

    /// Creates a `data` with the type of `default_value` from a string.
    /// @param value The string from that the `data` is created.
    /// @returns either a data with the parsed value or an `error`.
    expected<data> parse(const std::string& value) const;
  private:
    std::string long_name_;
    std::vector<char> short_names_;
    std::string description_;
    bool has_argument_;
    data default_value_;
  };

  using argument_iterator = std::vector<std::string>::const_iterator;

  /// Wraps the parse state.
  enum class parse_state {
    successful,
    option_already_exists,
    begin_is_not_an_option,
    name_not_declartion,
    arg_passed_but_not_declared,
    arg_declared_but_not_passed,
    faild_to_parse_argument,
    in_progress
  };

  /// Creates an a set of `option_declaration`.
  option_declaration_set();

  /// Adds an `option_declation` to the set.
  /// @param name The Long name and optional short option names in the format
  ///             "<long name>,[<short names 1><short name 2><...>]", where a
  ///             short name consists of exact one char.
  /// @returns An error if a) no long option name exists, b) long option is name
  ///          taken, c) short option name is taken
  expected<void> add(const std::string& name, const std::string& desciption,
                     data default_value);

  /// Creates a summary of all option declarations.
  std::string usage() const;

  /// Determines the number of added `options_declarations's.
  inline size_t size() const {
    return long_opts_.size();
  }

  /// Searches for an `option_declaration` by its long name.
  optional<const option_declaration&> find(const std::string& name) const;

  /// Fills an `option_map` from parsed CLI arguments.
  /// @param option_map The map of options that shall be filled.
  /// @param begin The iterator to the first argument that shall being parsed.
  /// @param end The *past-the-end* iterator of the last argument.
  /// @returns a pair constisting of a 'parser_state' and an iterator.
  ///          The `state` is *successful* when all arguments are successfully
  ///          parsed. Otherwise, it contains a value specific to the occurred
  ///          error. The 'iterator' points to the argument where the parser
  ///          encountered an error otherwise it points to the `end`.
  std::pair<parse_state, argument_iterator>
  parse(option_map& xs, argument_iterator begin, argument_iterator end) const;

private:
  using option_ptr = std::shared_ptr<option_declaration>;
  detail::steady_map<std::string, option_ptr> long_opts_;
  detail::steady_map<char, option_ptr> short_opts_;
};


} // namespace vast
