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

#include <cstddef>
#include <iostream>
#include <string>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/exec_main.hpp>
#include <caf/scoped_actor.hpp>

#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/factory.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/source.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

using std::cerr;
using std::cout;
using std::endl;

using vast::table_slice_ptr;

using namespace caf;

namespace {

constexpr const char* vast_header
  = R"(/******************************************************************************
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
)";

using slices_vector = std::vector<vast::table_slice_ptr>;

using read_function = slices_vector (*)(actor_system&);

using print_function = void (*)(actor_system&, const slices_vector&);

// Our custom configuration with extra command line options for this tool.
class config : public vast::system::configuration {
public:
  config() {
    opt_group{custom_options_, "global"}
      .add<std::string>("input-format,f", "input log format, defaults to zeek")
      .add<std::string>("variable-name",
                        "optional name for generated C++ variables")
      .add<std::string>("namespace-name",
                        "optional namespace for generated C++ code")
      .add<std::string>("output-format", "output format, defaults to 'c++'")
      .add<std::string>("input,i",
                        "path to input file or '-' (default) for STDIN")
      .add<std::string>("output,o",
                        "path to output file or '-' (default) for STDOUT")
      .add<atom_value>("table-slice-type,t",
                       "implementation type for the generated slices")
      .add<size_t>("table-slice-size,s",
                   "maximum size of the generated slices");
  }

  using actor_system_config::parse;
};

void print_cpp(actor_system& sys, const slices_vector& slices) {
  std::vector<char> buf;
  binary_serializer sink{sys, buf};
  sink(slices);
  auto nn = get_or(sys.config(), "namespace-name", "log");
  auto vn = get_or(sys.config(), "variable-name", "buf");
  auto path = get_or(sys.config(), "output", "-");
  auto maybe_out = vast::detail::make_output_stream(path, false);
  if (!maybe_out) {
    cerr << "unable to open " << path << ": " << sys.render(maybe_out.error())
         << endl;
    return;
  }
  auto& out = **maybe_out;
  out << vast_header << endl
      << "#include <cstddef>" << endl
      << endl
      << "namespace " << nn << " {" << endl
      << endl
      << "char " << vn << "[] = {" << endl;
  for (auto c : buf)
    out << static_cast<int>(c) << "," << endl;
  out << "};" << endl
      << endl
      << "size_t " << vn << "_size = sizeof(" << vn << ");" << endl
      << endl
      << "} // namespace " << nn << endl;
}

slices_vector read_zeek(actor_system& sys) {
  slices_vector result;
  using reader_type = vast::format::zeek::reader;
  auto slice_size = get_or(sys.config(), "table-slice-size",
                           vast::defaults::system::table_slice_size);
  auto slice_type = get_or(sys.config(), "table-slice-type",
                           vast::defaults::system::table_slice_type);
  auto in = vast::detail::make_input_stream(get_or(sys.config(), "input", "-"),
                                            false);
  auto push_slice = [&](table_slice_ptr x) {
    result.emplace_back(std::move(x));
  };
  reader_type reader{slice_type, std::move(*in)};
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     slice_size, push_slice);
  if (err != vast::ec::end_of_input)
    std::cerr << "*** error: " << sys.render(err) << std::endl;
  VAST_INFO("reader", "produced", produced, "events");
  return result;
}

void caf_main(actor_system& sys, const config& cfg) {
  // Print funtions setup.
  std::unordered_map<std::string, print_function> printers{
    {"c++", print_cpp},
  };
  // Source factories setup.
  std::unordered_map<std::string, read_function> readers{
    {"zeek", read_zeek},
  };
  // Utility functions.
  auto dump_keys = [&](const auto& xs) {
    for (auto& kvp : xs)
      cerr << "- " << kvp.first << endl;
  };
  // Verify printer setup.
  print_function print = nullptr;
  if (auto i = printers.find(get_or(cfg, "output-format", "c++"));
      i == printers.end()) {
    std::cerr << "invalid printer; supported output formats:" << endl;
    dump_keys(printers);
    return;
  } else {
    print = i->second;
  }
  // Verify input format setup.
  read_function read = nullptr;
  auto input_format = get_or(cfg, "input-format", "zeek");
  if (auto i = readers.find(input_format); i == readers.end()) {
    cerr << "invalid input format; supported formats:" << endl;
    dump_keys(readers);
    return;
  } else {
    read = i->second;
  }
  // Dispatch to function pair.
  print(sys, read(sys));
}

} // namespace

CAF_MAIN()
