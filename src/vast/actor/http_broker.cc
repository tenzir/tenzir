#include <iostream>
#include <chrono>

#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "vast/logger.h"
#include "vast/actor/http_broker.h"

#include "vast/actor/actor.h"
#include "vast/event.h"
#include "vast/time.h"
#include "vast/uuid.h"
#include "vast/util/json.h"
#include "vast/concept/parseable/vast/http.h"
#include "vast/util/http.h"

using namespace caf;
using namespace caf::io;
using namespace std::string_literals;

namespace vast {

uint64_t event_counter = 0;

std::string create_response_header()
{
  auto response = "HTTP/1.1 200 OK\r\n"s;
  response += "Content-Type: application/json\r\n";
  response += "Access-Control-Allow-Origin: *\r\n";
  response += "\r\n";
  return response;
}

void setup_exporter(broker* self, util::http_url url, actor const& node, std::string exporter_label){
  caf::message_builder mb;
  mb.append("spawn");
  mb.append("-l" + exporter_label);
  mb.append("exporter");
  if (url.contains_option("continuous"))
  {
    mb.append("-c");
    VAST_DEBUG("add continuous option");
  }
  if (url.contains_option("historical"))
  {
    mb.append("-h");
    VAST_DEBUG("add historical option");
  }
  if (url.contains_option("unified"))
  {
    mb.append("-u");
    VAST_DEBUG("add unified option");
  }
  if (url.contains_option("limit"))
  {
    mb.append("-l " + url.Options("limit"));
    VAST_DEBUG("add limit ", url.Options("limit"));
  }
  mb.append(url.Options("query"));
  self->send(node, mb.to_message());

  mb.clear();
  mb.append("connect");
  mb.append(exporter_label);
  //TODO: Actually get the archive(s) actor and use proper labels
  mb.append("archive");
  self->send(node, mb.to_message());

  mb.clear();
  mb.append("connect");
  mb.append(exporter_label);
  //TODO: Actually get the index(es) actor and use proper labels
  mb.append("index");
  self->send(node, mb.to_message());

  mb.clear();
  mb.append(get_atom::value);
  mb.append(exporter_label);
  self->send(node, mb.to_message());
}

bool handle(event const& e, broker* self, connection_handle hdl)
{
  auto j = to<util::json>(e);
  if (! j)
    return false;
  event_counter++;
  auto content = to_string(*j, true);
  content += "\r\n";
  self->write(hdl, content.size(), content.c_str());
  self->flush(hdl);
  return true;
}

behavior connection_worker(broker* self, connection_handle hdl, actor const& node)
{
  self->configure_read(hdl, receive_policy::at_most(1024));

  auto exporter_label = "exporter" + std::to_string(self->id());
  VAST_DEBUG(self, "exporter_label=", exporter_label);

  return
  {
    [=](new_data_msg const& msg)
    {
      std::string bufstr(msg.buf.begin(), msg.buf.end());
      auto http_parser = make_parser<util::http_request>{};
      util::http_request request;
      auto f = bufstr.begin();
      auto l = bufstr.end();
      http_parser.parse(f, l, request);
      auto url_str = request.URL();
      VAST_DEBUG(self, "got", url_str, "as URL");
      auto url_parser = make_parser<util::http_url>{};
      util::http_url url;
      f = url_str.begin();
      l = url_str.end();
      url_parser.parse(f, l, url);
      auto query = url.Options("query");
      VAST_DEBUG(self, "got", query, "as query");
      setup_exporter(self, url, node, exporter_label);
    },
    [=](connection_closed_msg const&)
    {
      self->quit();
    },
    [=](actor const& act, std::string const& fqn, std::string const& type)
    {

      VAST_DEBUG("got actor", act, "with fqn", fqn, " and type", type);
      //Register at exporter
      caf::message_builder mb;
      mb.append(put_atom::value);
      mb.append(sink_atom::value);
      mb.append(self);
      self->send(act, mb.to_message());

      //Run exporter, maybe delay here... if exporter gets run signal before we register we might
      //not get some events
      mb.clear();
      mb.append("send");
      mb.append(exporter_label);
      mb.append("run");
      self->send(node, mb.to_message());

      //send response header
      auto header = create_response_header();
      self->write(hdl, header.size(), header.c_str());
      self->flush(hdl);
      VAST_DEBUG("send response header", header);
    },
    // handle sink messages
    [=](exit_msg const& msg)
    {
      self->quit(msg.reason);
    },
    [=](uuid const&, event const& e)
    {
      handle(e, self, hdl);
    },
    [=](uuid const&, std::vector<event> const& v)
    {
      assert(! v.empty());
      for (auto& e : v)
        if (! handle(e, self, hdl))
          return;
    },
    [=](uuid const& id, progress_atom, double progress, uint64_t total_hits)
    {
      VAST_VERBOSE(self, "got progress from query ", id << ':',
                   total_hits, "hits (" << size_t(progress * 100) << "%)");
      auto progress_json = "{\n  \"progress\": "s;
      progress_json += std::to_string(progress);
      progress_json += "\n  \"event_counter\": ";
      progress_json += std::to_string(event_counter);
      progress_json += "\n  \"state\": \"PROGRESS\"";
      progress_json += "\n}\n";
      self->write(hdl, progress_json.size(), progress_json.c_str());
      self->flush(hdl);

    },
    [=](uuid const& id, done_atom, time::extent runtime)
    {
      VAST_VERBOSE(self, "got DONE from query", id << ", took", runtime);
      auto progress_json = "{\n  \"state\": \"DONE\""s;
      progress_json += "\n  \"progress\": 1.0";
      progress_json += "\n  \"event_counter\": ";
      progress_json += std::to_string(event_counter);
      progress_json += "\n}\n";
      self->write(hdl, progress_json.size(), progress_json.c_str());
      self->flush(hdl);
      event_counter = 0;
      self->quit(exit::done);
    }
  };
}

behavior http_broker_function(broker* self, actor const& node)
{
  VAST_DEBUG(self, "http_broker_function called");
  return
  {
    [=](new_connection_msg const& ncm)
    {
      VAST_DEBUG(self, "got new connection");
      auto worker = self->fork(connection_worker, ncm.handle, node);
      self->monitor(worker);
    },
    others >> [=]
    {
      auto msg = to_string(self->current_message());
      VAST_WARN(self, "got unexpected msg:", msg);
    }
  };
}

} // namespace vast
