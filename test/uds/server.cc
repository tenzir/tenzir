#include <iostream>

#include "vast/filesystem.h"
#include "vast/util/posix.h"

using namespace vast;

int main()
{
  auto filename = "/tmp/test.socket";
  // Block and wait for a client connection.
  std::cerr << "accepting connections on " << filename << std::endl;
  auto uds = util::unix_domain_socket::accept(filename);
  if (! uds)
  {
    std::cerr << "failed to accept connection" << std::endl;
    return -1;
  }
  std::cerr << "receiving file descriptor" << std::endl;
  auto fd = uds.recv_fd();
  if (fd < 0)
  {
    std::cerr << "invalid file descriptor: " << fd << std::endl;
    return -1;
  }
  file f{fd};
  auto response = "**********";
  std::cerr << "writing response: " << response << std::endl;
  return f.write(response, sizeof(response));
}
