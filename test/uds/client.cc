#include <iostream>
#include <thread>

#include "vast/util/posix.h"

using namespace vast;

int main()
{
  auto filename = "/tmp/test.socket";
  // Block and wait for a client connection.
  std::cerr << "connecting to " << filename << std::endl;
  auto uds = util::unix_domain_socket::connect(filename);
  if (! uds)
  {
    std::cerr << "failed to connect" << std::endl;
    return 1;
  }
  auto fd = 1;
  std::cerr << "sending file descriptor " << fd << std::endl;
  uds.send_fd(fd);
  std::cerr << "awaiting response" << std::endl;
  for (auto i = 0; i < 5; ++i)
  {
    std::cerr << '.' << std::flush;
    std::this_thread::sleep_for(std::chrono::seconds(i));
  }
  std::cerr << std::endl;
  return 0;
}
