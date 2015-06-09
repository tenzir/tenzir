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

behavior connection_worker(broker* self, connection_handle hdl, actor const& node)
{
  self->configure_read(hdl, receive_policy::at_most(1024));
  caf::message_builder mb;
  mb.append("spawn");
  mb.append("exporter");
  self->send(node, mb.to_message());

  return
  {
    [=](new_data_msg const& msg)
    {
      VAST_DEBUG(self, "got", msg.buf.size(), "bytes");
      auto url = parse_url(msg);
      auto query = url.substr(url.find("query=") + 6, url.size());
      VAST_DEBUG(self, "got", query, "as query");

      auto content ="{query : \""s;
      content.append(query);
      content.append("\"}");

      auto ans = create_response(content);
      VAST_DEBUG(self, "responding with", ans);
      self->write(msg.handle, ans.size(), ans.c_str());

      auto host = "127.0.0.1"s;
      auto port = uint16_t{42000};
      VAST_VERBOSE("connecting to", host << ':' << port);
      try
      {
        auto node = caf::io::remote_actor(host.c_str(), port);
        VAST_VERBOSE("connected to", host << ':' << port);
      }
      catch (caf::caf_exception const& e)
      {
        VAST_ERROR("failed to connect to", host << ':' << port);
      }
      self->quit();
    },
    [=](connection_closed_msg const&)
    {
      self->quit();
    }
  };
}

behavior http_broker_function(broker* self, actor const& node)
{
  VAST_VERBOSE("http_broker_function called");
  return
  {
    [=](new_connection_msg const& ncm)
    {
      VAST_DEBUG(self, "got new connection");
      auto worker = self->fork(connection_worker, ncm.handle, node);
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

} // namespace vast
