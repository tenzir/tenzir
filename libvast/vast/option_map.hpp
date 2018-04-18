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

#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "vast/detail/steady_map.hpp"
#include "vast/detail/string.hpp"

#include "vast/optional.hpp"
#include "vast/expected.hpp"

#include "vast/error.hpp"
#include "vast/data.hpp"




namespace vast {

// FIXME: const std::string& to string_view
// FIXME: split header for option_map and option_delcation set

/// A map for CLI options.
class option_map {
public:
  // FIXME: the map api is not complete
  using map_type = detail::steady_map<std::string, data>;
  using iterator = map_type::iterator;
  using const_iterator = map_type::const_iterator;
  using reverse_iterator = map_type::reverse_iterator;
  using const_reverse_iterator = map_type::const_reverse_iterator;
  using size_type = map_type::size_type;

  optional<data> get(const std::string& name) const;

  data get_or(const std::string& name, const data& default_value) const;

  inline optional<data> operator[](const std::string& name) const {
    return get(name);
  }

  void set(const std::string& name, const data& x);

  expected<void> add(const std::string& name, const data& x);

  void clear() {
    xs_.clear();
  }

  // -- iterators ------------------------------------------------------------

  inline auto begin() {
    return xs_.begin();
  }

  inline auto begin() const {
    return xs_.begin();
  }

  inline auto end() {
    return xs_.end();
  }

  inline auto end() const {
    return xs_.end();
  }

  inline auto rbegin() {
    return xs_.rbegin();
  }

  inline auto rbegin() const {
    return xs_.rbegin();
  }

  inline auto rend() {
    return xs_.rend();
  }

  inline auto rend() const {
    return xs_.rend();
  }

  // -- capacity -------------------------------------------------------------

  inline auto empty() const {
    return xs_.empty();
  }

  inline auto size() const {
    return xs_.size();
  }

private:
  map_type xs_;
};

/// A set of `option_declarations` that can fill an `option_map` from a CLI
/// string.
class option_declaration_set {
public:
  /// A declaration of a CLI argument option.
  class option_declaration {
  public:
    /// Constructs a declation of an option.
    /// @param long_name A long name that identifies this option.
    /// @param short_name A short name that identifies this option.
    /// @param description An info to this option.
    /// @param has_argument A flag that describes whether this option an
    /// argument.
    /// @param default_value A value thats is used when the option is not set by
    /// a user.
    option_declaration(std::string long_name, std::vector<char> short_names,
                       std::string description, bool has_argument,
                       data default_value);

    inline const auto& long_name() const {
      return long_name_;
    }

    inline const auto& short_names() const {
      return short_names_;
    }

    inline const auto& description() const {
      return description_;
    }

    /// Checks whether this option requires an argument.
    inline auto has_argument() const {
      return has_argument_;
    }

    inline const auto& default_value() const {
      return default_value_;
    }

    /// Creates a `data` from a parsed string.
    ///// if this option has no argument this function cannot be called
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

  /// Wraps the parse result.
  enum class parse_result {
    in_progress = 0,
    successful,
    option_already_exists,
    begin_is_not_an_option,
    name_not_declartion,
    arg_passed_but_not_declared,
    arg_declared_but_not_passed,
    faild_to_parse_argument,
    parsing_error // FIXME: remove me
  };

  option_declaration_set();

  /// Adds an `option_declation` to the set.
  /// @param name Long and (optional) short option names in the format
  ///             "<long name>,[<short names 1><short name 2><...>]", where a
  ///             short name consists of exact one char.
  /// @returns An error if a) no long option name exists, b) long option is name
  ///          taken, c) short option name is taken
  expected<void> add(const std::string& name, const std::string& desciption,
                     data default_value);

  /// Creates a pretty summary of all option declarations.
  std::string usage() const;

  inline size_t size() const {
    return long_opts_.size();
  }

  /// Searches for an `option_declaration` by its long name.
  optional<const option_declaration&> find(const std::string& name) const;


  /// Parses CLI arguments and fills a map of options with all found values.
  /// @param option_map The map of options that shall be filled.
  /// @param begin The first argument that shall being parsed.
  /// @param end The *past-the-end* pointer of the last argument.
  /// @returns a pair constisting of an 'error' and an iterator.
  ///          The `error` is empty when all arguments are successfully parsed.
  ///          Otherwise, it contains a description why an error occurred. The
  ///          'iterator' points to the argument where the parser encountered an
  ///          error or to the `end`.
  std::pair<parse_result, argument_iterator>
  parse(option_map& xs, argument_iterator begin, argument_iterator end) const;

private:
  using option_ptr = std::shared_ptr<option_declaration>;
  detail::steady_map<std::string, option_ptr> long_opts_;
  detail::steady_map<char, option_ptr> short_opts_;
};


} // namespace vast

