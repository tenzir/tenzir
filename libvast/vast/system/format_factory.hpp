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

#ifndef FORMAT_FACTORY_HPP 
#define FORMAT_FACTORY_HPP

#include <string>
#include <functional>

#include <caf/local_actor.hpp>

#include "vast/system/source.hpp"

namespace vast::system {


/// A factory for readers and writers.
class format_factory {
public:
  /// A type of a factory to create a reader or a writer.
  template <class Format>
  using format_factory_function = 
    std::function<expected<Format>(caf::message&)>;

  /// A type of a factory to spawn an actor configred by a message.
  using actor_factory_function = 
    std::function<expected<caf::actor>(caf::local_actor*, caf::message&)>;

  /// Default arguments which are provided by most readers.
  struct reader_default_args {
    caf::message::cli_res parse(caf::message& args);

    std::string input = "-"s;
    bool uds = false;
  };

  /// Default-constructs a format factory.
  format_factory();

  /// Stores a reader format.
  /// @param format The name of the format.
  /// @param make_reader A factory function to create a reader
  template <class Reader>
  bool add_reader(const std::string& format,
                  format_factory_function<Reader> make_reader) {
    auto factory = [=](caf::local_actor* self,
                       caf::message& args) -> expected<caf::actor> {
      if (auto reader = make_reader(args); reader)
        return self->spawn(source<Reader>, std::move(*reader));
      else
        return reader.error();
    };
    return readers_.try_emplace(format, std::move(factory)).second;
  }

  template <class Writer>
  bool add_writer(const std::string&, format_factory_function<Writer>) {
    // TODO: implement me
  }

  expected<actor_factory_function> reader(const std::string& format);
  expected<actor_factory_function> writer(const std::string& format);
private:
  std::unordered_map<std::string, actor_factory_function> readers_;
  std::unordered_map<std::string, actor_factory_function> writers_;
};


} // namespace vast::system::format

#endif
