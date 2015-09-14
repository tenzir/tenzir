#include <iostream>
#include <chrono>

#include <caf/io/all.hpp>

#include "vast/caf.h"
#include "vast/event.h"
#include "vast/uuid.h"
#include "vast/http.h"
#include "vast/actor/atoms.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/http.h"
#include "vast/concept/parseable/vast/uri.h"
#include "vast/concept/convertible/vast/event.h"
#include "vast/concept/convertible/to.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/json.h"
#include "vast/concept/printable/vast/uuid.h"
#include "vast/concept/printable/vast/http.h"
#include "vast/concept/printable/vast/uri.h"

using namespace std::string_literals;

namespace vast {
namespace {

using caf::io::broker;
using caf::io::connection_closed_msg;
using caf::io::connection_handle;
using caf::io::receive_policy;
using caf::io::new_connection_msg;
using caf::io::new_data_msg;

std::string make_response_header() {
  return "HTTP/1.1 200 OK\r\n"
         "Content-Type: application/json\r\n"
         "Access-Control-Allow-Origin: *\r\n"
         "\r\n";
}

// Handles a HTTP request.
void dispatch(http::request& request, broker* self,
              connection_handle const& conn, actor const& node) {
  if (request.method == "GET") {
    VAST_DEBUG_AT(self, "got GET request for", request.uri);
    // Construct message to spawn an EXPORTER via NODE.
    message_builder mb;
    mb.append("spawn");
    mb.append("exporter");
    mb.append("-l");
    mb.append("exporter-" + to_string(uuid::random()).substr(0, 7));
    mb.append("-a");  // enable auto-connect
    if (request.uri.query.count("continuous") > 0)
      mb.append("-c");
    else if (request.uri.query.count("historical") > 0)
      mb.append("-h");
    else if (request.uri.query.count("unified") > 0)
      mb.append("-u");
    if (request.uri.query.count("limit") > 0) {
      mb.append("-e") ;
      mb.append(request.uri.query["limit"]);
    }
    if (request.uri.query["query"].empty()) {
      json::object body;
      body["error"] = "empty query";
      auto response = make_response_header() + to_string(body);
      self->write(conn, response.size(), response.c_str());
      self->flush(conn);
      self->quit(exit::done);
      return;
    }
    mb.append(request.uri.query["query"]);
    self->send(node, mb.to_message());
    self->become(
      keep_behavior,
      [=](actor const& exporter) {
        VAST_DEBUG_AT(self, "got EXPORTER from NODE");
        // Register ourselves as SINK and get going.
        self->send(exporter, put_atom::value, sink_atom::value, self);
        self->send(exporter, run_atom::value);
        // For now, we terminate the EXPORTER as we go down. In the future, we
        // should keep it running so that one can steer the EXPORTER from
        // different HTTP sessions.
        self->attach_functor([=](uint32_t reason) {
          anon_send_exit(exporter, reason);
        });
        auto header = make_response_header();
        self->write(conn, header.size(), header.c_str());
        self->unbecome();
      },
      [=](error const& e) {
        VAST_ERROR_AT(self, "failed to spawn EXPORTER");
        json::object body;
        body["error"] = "failed to spawn EXPORTER: " + e.msg();
        auto response = make_response_header() + to_string(body);
        self->write(conn, response.size(), response.c_str());
        self->flush(conn);
        self->quit(exit::error);
      },
      quit_on_others(self)
    );
  } else {
    VAST_WARN_AT(self, "invalid HTTP request method:", request.method);
  }
}

// When the HTTP broker accepts a connection, it spawns this worker which is in
// charge of the HTTP session.
behavior http_worker(broker* self, connection_handle conn, actor const& node) {
  self->configure_read(conn, receive_policy::at_most(4096));
  auto deliver = [=](event const& e) {
    auto j = to<vast::json>(e);
    if (j) {
      auto str = to_string(*j) + "\r\n";
      self->write(conn, str.size(), str.c_str());
      self->flush(conn);
    }
  };
  return {
    [=](new_data_msg const& msg) {
      auto request = to<http::request>(msg.buf);
      if (request) {
        dispatch(*request, self, conn, node);
      } else {
        VAST_ERROR_AT(self, "received invalid HTTP request");
        self->quit(exit::error);
      }
    },
    [=](connection_closed_msg const&) {
      VAST_DEBUG_AT(self, "terminates after remote connection closed");
      self->quit(exit::done);
    },
    [=](exit_msg const& msg) {
      self->quit(msg.reason);
    },
    [=](uuid const&, event const& e) {
      deliver(e);
    },
    [=](uuid const&, std::vector<event> const& v) {
      VAST_ASSERT(!v.empty());
      for (auto& e : v)
        deliver(e);
    },
    [=](uuid const& id, progress_atom, double progress, uint64_t total_hits) {
      VAST_VERBOSE_AT(self, "got progress from query ", id << ':',
                      total_hits, "hits (" << size_t(progress * 100) << "%)");
      json::object status;
      status["progress"] = progress;
      auto body = to_string(status) + "\r\n";
      self->write(conn, body.size(), body.c_str());
      self->flush(conn);
    },
    [=](uuid const& id, done_atom, time::extent runtime) {
      VAST_VERBOSE(self, "got DONE from query", id << ", took", runtime);
      json::object status;
      status["progress"] = 1.0;
      status["runtime"] = to_string(runtime);
      auto body = to_string(status) + "\r\n";
      self->write(conn, body.size(), body.c_str());
      self->flush(conn);
      // TODO: Even if the query has delivered all results, the user may still
      // want to seek back (#87), so we should not terminate at this point and
      // either wait for a separate signal or terminate as the connection ends.
      self->quit(exit::done);
    }
  };
}

} // namespace <anonymous>

behavior http_broker(broker* self, actor const& node) {
  VAST_DEBUG_AT("http_broker#" << self->id(), "spawned");
  return {
    [=](new_connection_msg const& msg) {
      VAST_DEBUG_AT(self, "got new HTTP connection");
      auto worker = self->fork(http_worker, msg.handle, node);
      self->attach_functor([=](uint32_t reason) {
        anon_send_exit(worker, reason);
      });
    },
    quit_on_others(self)
  };
}

} // namespace vast
