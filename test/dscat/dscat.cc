#include <iostream>

#include <caf/message_builder.hpp>

#include "vast/filesystem.h"
#include "vast/io/algorithm.h"
#include "vast/io/file_stream.h"
#include "vast/util/posix.h"

using namespace caf;
using namespace std;
using namespace std::string_literals;
using namespace vast;
using namespace vast::io;

int main(int argc, char** argv)
{
  auto usage = "usage: dscat [-l] <uds> [file]";
  auto r = message_builder{argv + 1, argv + argc}.extract_opts({
    {"listen,l", "listen on <uds> and serve <file>"}
  });
  if (r.remainder.size() > 2)
  {
    cerr << usage << endl;
    return 1;
  }
  if (r.remainder.empty())
  {
    cerr << usage << "\n\n" << r.helptext;
    return 1;
  }
  auto& uds_name = r.remainder.get_as<std::string>(0);
  if (r.opts.count("listen") > 0)
  {
    auto input =
      r.remainder.size() == 2 ? r.remainder.get_as<std::string>(1) : "-";
    cerr << "listening on " << uds_name << " to serve " << input << endl;
    auto uds = util::unix_domain_socket::accept(uds_name);
    if (! uds)
    {
      cerr << "failed to accept connection" << endl;
      return -1;
    }
    file f{input};
    if (! f.open(file::read_only))
    {
      cerr << "failed to open file " << input << endl;
      return 1;
    }
    cerr << "sending file descriptor " << f.handle() << endl;
    if (! uds.send_fd(f.handle()))
    {
      cerr << "failed to send file descriptor" << endl;
      return 1;
    }
  }
  else
  {
    cerr << "connecting to " << uds_name << endl;
    auto uds = util::unix_domain_socket::connect(uds_name);
    if (! uds)
    {
      cerr << "failed to connect" << endl;
      return 1;
    }
    cerr << "receiving file descriptor " << endl;
    auto fd = uds.recv_fd();
    if (fd < 0)
    {
      cerr << "failed to receive file descriptor" << endl;
      return 1;
    }
    cerr << "dumping contents\n" << endl;
    file_input_stream is{fd, close_on_destruction};
    file_output_stream os{"-"};
    copy(is, os);
  }
  return 0;
}
