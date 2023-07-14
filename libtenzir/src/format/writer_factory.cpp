//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/format/writer_factory.hpp"

#include "tenzir/config.hpp"
#include "tenzir/detail/make_io_stream.hpp"
#include "tenzir/format/arrow.hpp"
#include "tenzir/format/ascii.hpp"
#include "tenzir/format/csv.hpp"
#include "tenzir/format/json.hpp"
#include "tenzir/format/null.hpp"
#include "tenzir/format/writer.hpp"
#include "tenzir/format/zeek.hpp"
#include "tenzir/plugin.hpp"

namespace tenzir {

template <class Writer>
caf::expected<std::unique_ptr<format::writer>>
make_writer(const caf::settings& options) {
  using namespace std::string_literals;
  using ostream_ptr = std::unique_ptr<std::ostream>;
  if constexpr (std::is_constructible_v<Writer, ostream_ptr, caf::settings>) {
    auto out = detail::make_output_stream(options);
    if (!out)
      return out.error();
    return std::make_unique<Writer>(std::move(*out), options);
  } else {
    return std::make_unique<Writer>(options);
  }
}

void factory_traits<format::writer>::initialize() {
  using namespace format;
  using fac = factory<writer>;
  fac::add("ascii", make_writer<ascii::writer>);
  fac::add("csv", make_writer<csv::writer>);
  fac::add("json", make_writer<format::json::writer>);
  fac::add("null", make_writer<null::writer>);
  fac::add("zeek", make_writer<zeek::writer>);
  fac::add("arrow", make_writer<arrow::writer>);
  for (const auto& plugin : plugins::get()) {
    if (const auto* reader = plugin.as<writer_plugin>()) {
      fac::add(
        reader->writer_format(),
        [name = std::string{plugin->name()}](const caf::settings& options)
          -> caf::expected<std::unique_ptr<format::writer>> {
          for (const auto& plugin : plugins::get()) {
            if (plugin->name() != name)
              continue;
            const auto* writer = plugin.as<writer_plugin>();
            TENZIR_ASSERT(writer);
            return writer->make_writer(options);
          }
          return caf::make_error(ec::logic_error,
                                 fmt::format("writer plugin {} was used to "
                                             "initialize factory but unloaded "
                                             "at a later point in time",
                                             name));
        });
    }
  }
}

} // namespace tenzir
