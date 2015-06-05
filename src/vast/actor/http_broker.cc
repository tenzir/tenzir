#include <iostream>
#include <chrono>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "vast/logger.h"
#include "vast/actor/http_broker.h"

// FIXME: remove after debugging
using std::cout;
using std::cerr;
using std::endl;

using namespace caf;
using namespace caf::io;
using namespace std::string_literals;

namespace vast {

//using tick_atom = atom_constant<atom("tick")>;

constexpr const char http_ok[] = R"__(HTTP/1.1 200 OK
Content-Type: text/plain
Connection: keep-alive
Transfer-Encoding: chunked

d
Hi there! :)

0


)__";

constexpr const char http_header[] = R"__(HTTP/1.1 200 OK
Content-Type: application/json
Connection: keep-alive

)__";

template <size_t Size>
constexpr size_t cstr_size(const char (&)[Size])
{
  return Size;
}


std::string parse_url(new_data_msg msg)
{
  std::string bufstr(msg.buf.begin(), msg.buf.end());
  //aout(self) << bufstr << endl;
  auto pos = bufstr.find(' ');
  auto url = bufstr.substr(pos + 1, bufstr.find(' ', pos+1)-4);
  //aout(self) << "url:'" << url << "'" << endl;
  return url;
}

std::string create_response(std::string const& content)
{
  auto response = ""s;
  response += "HTTP/1.1 200 OK\r\n";
  response += "Content-Type: application/json\r\n";
  response += "\r\n";
  response += content;
  response += "\r\n";
  return response;
}

behavior connection_worker(broker* self, connection_handle hdl)
{
  self->configure_read(hdl, receive_policy::at_most(1024));
  return
  {
    [=](new_data_msg const& msg)
    {
      VAST_DEBUG(self, "got", msg.buf.size(), "bytes");
      auto url = parse_url(msg);

      auto query = url.substr(url.find("query=") + 6, url.size());
      aout(self) << "query:'" << query << "'" << endl;

      auto content ="{query : \""s;
      content.append(query);
      content.append("\"}");

      auto ans = create_response(content);
      aout(self) << "response:" << ans << endl;

      self->write(msg.handle, ans.size(), ans.c_str());
      self->quit();
    },
    [=](connection_closed_msg const&)
    {
      self->quit();
    }
  };
}

behavior http_broker_function(broker* self)
{
  VAST_VERBOSE("http_broker_function called");
  return
  {
    [=](new_connection_msg const& ncm)
    {
      VAST_DEBUG(self, "got new connection");
      auto worker = self->fork(connection_worker, ncm.handle);
      self->monitor(worker);
      self->link_to(worker);
    },
    others >> [=]
    {
      auto msg = to_string(self->current_message());
      VAST_WARN(self, "got unexpected msg:", msg);
      aout(self) << "unexpected: " << msg << endl;
    }
  };
}

optional<uint16_t> as_u16(std::string const& str)
{
  return static_cast<uint16_t>(stoul(str));
}

//int main(int argc, const char** argv) {
//  uint16_t port = 0;
//  auto res = message_builder(argv + 1, argv + argc).extract_opts({
//    {"port,p", "set port", port},
//  });
//  if (!res.error.empty()) {
//    cerr << res.error << endl;
//    return 1;
//  }
//  if (res.opts.count("help") > 0) {
//    cout << res.helptext << endl;
//    return 0;
//  }
//  if (!res.remainder.empty()) {
//    // not all CLI arguments could be consumed
//    cerr << "*** too many arguments" << endl << res.helptext << endl;
//    return 1;
//  }
//  if (res.opts.count("port") == 0) {
//    cerr << "*** no port given" << endl << res.helptext << endl;
//    return 1;
//  }
//  cout << "*** run in server mode listen on: " << port << endl;
//  cout << "*** to quit the program, simply press <enter>" << endl;
//  auto server_actor = spawn_io_server(http_broker_function, port);
//  // wait for any input
//  std::string dummy;
//  std::getline(std::cin, dummy);
//  // kill server
//  anon_send_exit(server_actor, exit_reason::user_shutdown);
//  await_all_actors_done();
//  shutdown();
//}

} // namespace vast
