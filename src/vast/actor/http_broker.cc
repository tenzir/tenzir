#include <iostream>
#include <chrono>
#include <map>

#include <caf/io/all.hpp>

#include "vast/caf.h"
#include "vast/event.h"
#include "vast/uuid.h"
#include "vast/http.h"
#include "vast/time.h"
#include "vast/actor/atoms.h"
#include "vast/actor/basic_state.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/http.h"
#include "vast/concept/parseable/vast/uri.h"
#include "vast/concept/parseable/vast/uuid.h"
#include "vast/concept/convertible/vast/event.h"
#include "vast/concept/convertible/to.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/std/vector.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/json.h"
#include "vast/concept/printable/vast/uuid.h"
#include "vast/concept/printable/vast/http.h"
#include "vast/concept/printable/vast/uri.h"
#include "vast/util/coding.h"

using namespace std::string_literals;

using caf::io::broker;
using caf::io::broker_ptr;
using caf::io::connection_closed_msg;
using caf::io::connection_handle;
using caf::io::receive_policy;
using caf::io::new_connection_msg;
using caf::io::new_data_msg;

namespace vast {
namespace {

std::string make_http_response(uint32_t code, std::string body) {
  http::response resp;
  resp.protocol = "HTTP";
  resp.version = 1.1;
  resp.status_code = code;
  resp.headers.push_back({"Access-Control-Allow-Origin", "*"});
  resp.headers.push_back({"Content-Type", "application/json"});
  resp.headers.push_back({"Content-Length", to_string(body.size())});
  resp.body = std::move(body);
  return to_string(resp);
}

std::string make_http_chunked_response_header(uint32_t code) {
  http::response resp;
  resp.protocol = "HTTP";
  resp.version = 1.1;
  resp.status_code = code;
  resp.headers.push_back({"Access-Control-Allow-Origin", "*"});
  resp.headers.push_back({"Content-Type", "application/json"});
  resp.headers.push_back({"Transfer-Encoding", "chunked"});
  return to_string(resp);
}

std::string make_http_response(uint32_t code, json::object const& j) {
  return make_http_response(code, to_string(j));
}

template <typename T>
message make_http_response_msg(uint32_t code, T const& x) {
  return make_message(make_http_response(code, x), false);
}

// Constructs a filter projection for a specific URI path and request method.
// The path can have "*" wildcards to match unconditionally.
auto filter(uri::path_type const& path, std::string const& method) {
  return [=](http::request const& req) -> maybe<http::request> {
    if (!(method == "*" || method == req.method))
      return nil;
    if (path.size() != req.uri.path.size())
      return nil;
    for (auto i = 0u; i < path.size(); ++i)
      if (!(path[i] == "*" || path[i] == req.uri.path[i]))
        return nil;
    return req;
  };
}

// Manages a single query.
struct shepherd {
  struct state : basic_state {
    state(local_actor* self) : basic_state{self, "shepherd"} {}

    actor sink;
    actor exporter;
    uint64_t requested = 0;
    uint64_t extracted = 0;
  };

  static behavior make(stateful_actor<state>* self) {
    auto send_data_chunk = [=](actor const& sink, std::string const& data) {
      auto chunk = util::to_hex(data.size()) + "\r\n" + data + "\r\n";
      self->send(sink, std::move(chunk), true);
    };
    auto send_last_chunk = [=](actor const& sink) {
      self->send(sink, "0\r\n\r\n", false);
    };
    self->trap_exit(true);
    auto on_exit = [=](exit_msg const& msg) {
      if (self->state.sink)
        self->send_exit(self->state.sink, msg.reason);
      if (self->state.exporter)
        self->send_exit(self->state.exporter, msg.reason);
      self->quit(msg.reason);
    };
    return {
      on_exit,
      [=](down_msg const& msg) {
        VAST_ASSERT(self->state.exporter.address() == msg.source);
        self->state.exporter = invalid_actor;
      },
      [=](put_atom, actor const& exporter) {
        // Currently, there can only be one EXPORTER per query.
        VAST_ASSERT(self->state.exporter == invalid_actor);
        VAST_DEBUG_AT(self, "got exporter#" << exporter->id());
        self->state.exporter = exporter;
        self->monitor(exporter);
      },
      [=](actor const& sink, extract_atom, uint64_t n) {
        VAST_DEBUG_AT(self, "got request to extract", n, "results");
        VAST_ASSERT(self->state.sink == invalid_actor);
        self->state.sink = sink;
        // If the EXPORTER is no longer alive, we cannot extract events.
        if (self->state.exporter == invalid_actor) {
          VAST_WARN_AT(self, "has no valid exporter");
          json::object o{{"error", "query already terminated"}};
          self->send(sink, make_http_response_msg(404, o));
          return;
        }
        self->state.requested += n;
        // We currently have only one EXPORTER which we have to relay the
        // extract request to. In the future, we need to figure out a strategy
        // to split the request over multiple EXPORTERS. Either by selecting a
        // specific one (e.g., the one with the newest data), or by simply
        // splitting the number of requested events uniformly.
        self->send(self->state.exporter, extract_atom::value, n);
        self->send(sink, make_http_chunked_response_header(200), true);
        // FIXME: instead of buffering the EXPORTER messages in a queue, figure
        // out a fully unbuffered solution.
        self->become(
          keep_behavior,
          on_exit,
          [=](down_msg const& msg) {
            VAST_ASSERT(self->state.exporter.address() == msg.source);
            self->state.exporter = invalid_actor;
            self->unbecome();
          },
          [=](uuid const&, std::vector<event> const& es) {
            VAST_DEBUG_AT(self, "got", es.size(), "results");
            json::object o{{"results", to_json(es)}};
            send_data_chunk(self->state.sink, to_string(o));
            self->state.extracted += es.size();
            if (self->state.extracted == self->state.requested) {
              send_last_chunk(self->state.sink);
              self->demonitor(self->state.sink);
              self->state.sink = invalid_actor;
              self->unbecome();
            }
          },
          [=](uuid const& eid, progress_atom, double progress, uint64_t hits) {
            VAST_DEBUG_AT(self, "got progress from query ", eid << ':',
                          hits, "hits (" << size_t(progress * 100) << "%)");
            json::object o{
              {"status", json::object{
                {"state", "running"},
                {"progress", progress},
                {"hits", hits},
              }}
            };
            send_data_chunk(self->state.sink, to_string(o));
          },
          [=](uuid const& eid, done_atom, time::extent runtime) {
            VAST_DEBUG_AT(self, "got DONE from exporter",
                          eid << ", took", runtime);
            // Terminate EXPORTER.
            self->demonitor(self->state.exporter);
            self->send_exit(self->state.exporter, exit::done);
            self->state.exporter = invalid_actor;
            // Finish this chunked transfer with a status message.
            json::object o{
              {"status", json::object{
                {"state", "done"},
                {"runtime", runtime.count()}
              }}
            };
            send_data_chunk(self->state.sink, to_string(o));
            send_last_chunk(self->state.sink);
            self->demonitor(self->state.sink);
            self->state.sink = invalid_actor;
            self->unbecome();
          }
        );
      }
    };
  }
};

// Acts as bridge between queries and HTTP worker actors.
struct mediator {
  struct state : basic_state {
    state(local_actor* self) : basic_state{self, "mediator"} {}

    std::map<uuid, actor> queries;  // Shepherds by their UUID.
  };

  static behavior make(stateful_actor<state>* self, actor const& node) {
    return {
      on(filter({"queries"}, "GET")) >> [=](http::request const&) {
        VAST_DEBUG_AT(self, "got GET for /queries");
      },
      on(filter({"queries"}, "POST")) >> [=](http::request const& req) {
        VAST_DEBUG_AT(self, "got POST for /queries with body:", req.body);
        auto rp = self->make_response_promise();
        // Get POST parameters from HTTP request body.
        uri::query_type params;
        if (!uri_query_string_parser{}(req.body, params)) {
          json::object o{{"error", "invalid POST body: " + req.body}};
          rp.deliver(make_http_response_msg(400, o));
          return;
        }
        // Check existence of mandatory parameters.
        auto expr = params.find("expression");
        if (expr == params.end()) {
          json::object o{{"error", "missing parameter: expression"}};
          rp.deliver(make_http_response_msg(400, o));
          return;
        }
        if (expr->second.empty()) {
          json::object o{{"error", "empty expression"}};
          rp.deliver(make_http_response_msg(400, o));
          return;
        }
        auto type = params.find("type");
        if (type == params.end()) {
          json::object o{{"error", "missing parameter: type"}};
          rp.deliver(make_http_response_msg(400, o));
          return;
        }
        // Construct message for NODE to spawn an EXPORTER.
        // TODO: in the future, make this work with multiple nodes. This means
        // that we need to spin up one EXPORTER per node and accumulate all of
        // them behind a shepherd
        json::object response;
        auto qid = uuid::random();
        response["id"] = to_string(qid);
        message_builder mb;
        mb.append("spawn");
        mb.append("exporter");
        mb.append("-l");
        mb.append("exporter-" + to_string(qid).substr(0, 7));
        mb.append("-a");  // enable auto-connect
        // Parse query type
        if (type->second == "continuous") {
          mb.append("-c");
        } else if (type->second == "historical") {
          mb.append("-h");
        } else if (type->second == "unified") {
          mb.append("-u");
        } else {
          json::object o{{"error", "invalid query type: " + type->second}};
          rp.deliver(make_http_response_msg(400, o));
          return;
        }
        response["type"] = type->second;
        mb.append(expr->second);
        response["created"] = time::duration_cast<time::milliseconds>(
          time::snapshot().time_since_epoch()).count();
        response["state"] = "created";
        response["hits"] = 0;
        response["candidates"] = 0;
        response["results"] = 0;
        // TODO: set Location header to new resource (/queries/{id}).
        auto response_msg = make_http_response_msg(201, response);
        VAST_DEBUG_AT(self, "requests to spawn EXPORTER for", expr->second);
        self->sync_send(node, mb.to_message()).then(
          [=](actor const& exporter) {
            VAST_DEBUG_AT(self, "got new EXPORTER");
            // Register EXPORTER with a new SHEPHERD.
            auto shep = self->spawn(shepherd::make);
            self->send(shep, put_atom::value, exporter);
            self->attach_functor([=](uint32_t reason) {
              anon_send_exit(shep, reason);
            });
            self->state.queries[qid] = shep;
            // Register the SHEPHERD as SINK and run.
            self->send(exporter, put_atom::value, sink_atom::value, shep);
            self->send(exporter, run_atom::value);
            rp.deliver(response_msg);
          },
          [=](error const& e) {
            VAST_ERROR_AT(self, "failed to spawn EXPORTER");
            json::object o{{"error", "failed to spawn EXPORTER: " + e.msg()}};
            rp.deliver(make_http_response_msg(500, o));
          },
          quit_on_others(self)
        );
      },
      on(filter({"queries", "*"}, "DELETE")) >> [=](http::request const& req) {
        VAST_DEBUG_AT(self, "got DELETE for", to_string(req.uri.path, "/"));
        auto qid = to<uuid>(req.uri.path[1]);
        if (!qid) {
          auto msg = "malformed query UUID: " + req.uri.path[1];
          VAST_WARN_AT(self, "got", msg);
          json::object o{{"error", msg}};
          return make_http_response_msg(500, o);
        }
        auto q = self->state.queries.find(*qid);
        if (q == self->state.queries.end()) {
          auto msg = "no such query: " + req.uri.path[1];
          VAST_WARN_AT(self, "got", msg);
          json::object o{{"error", msg}};
          return make_http_response_msg(404, o);
        }
        self->send_exit(q->second, exit::done);
        self->state.queries.erase(q);
        json::object o{{"success", "deleted query: " + req.uri.path[1]}};
        return make_http_response_msg(200, o);
      },
      on(filter({"queries", "*"}, "GET")) >> [=](http::request const& req) {
        VAST_DEBUG_AT(self, "got GET for", to_string(req.uri.path, "/"));
        // TODO: make this an explicit message handler parameter.
        auto job = actor_cast<actor>(self->current_sender());
        // Parse query UUID.
        auto qid = to<uuid>(req.uri.path[1]);
        if (!qid) {
          auto msg = "malformed query UUID: " + req.uri.path[1];
          VAST_WARN_AT(self, "got", msg);
          json::object o{{"error", msg}};
          self->send(job, make_http_response_msg(500, o));
          return;
        }
        auto q = self->state.queries.find(*qid);
        if (q == self->state.queries.end()) {
          auto msg = "no such query: " + req.uri.path[1];
          VAST_WARN_AT(self, "got", msg);
          json::object o{{"error", msg}};
          self->send(job, make_http_response_msg(404, o));
          return;
        }
        // React according to URI parameters.
        auto n = req.uri.query.find("n");
        auto id = req.uri.query.find("id");
        if (n != req.uri.query.end() && id != req.uri.query.end()) {
          auto msg = "invalid query parameters, both 'n' and 'id'";
          VAST_WARN_AT(self, "got", msg);
          json::object o{{"error", msg}};
          self->send(job, make_http_response_msg(500, o));
          return;
        }
        if (n != req.uri.query.end()) {
          // At this point we just have one EXPORTER per query.
          auto value = to<uint64_t>(n->second);
          if (!value) {
            auto msg = "invalid value for query paramter 'n': " + n->second;
            VAST_WARN_AT(self, "got", msg);
            json::object o{{"error", msg}};
            self->send(job, make_http_response_msg(500, o));
            return;
          }
          // Send extract request to SHEPHERD.
          self->send(q->second, job, extract_atom::value, *value);
          return;
        }
        if (id != req.uri.query.end()) {
          // TODO: implement
          return;
        }
        auto msg = "missing parameters: neither 'n' nor 'id' given";
        json::object o{{"error", msg}};
        self->send(job, make_http_response_msg(500, o));
      },
      on(filter({"types"}, "GET")) >> [=](http::request const&) {
        VAST_DEBUG_AT(self, "got GET for /types");
        auto rp = self->make_response_promise();
        message_builder mb;
        mb.append("show");
        mb.append("schema");
        self->send(node, mb.to_message());
        self->become(
          keep_behavior,
          [=](json const& schema) {
            VAST_DEBUG_AT(self, "got schema from NODE");
            json::object o{{"types", schema}};
            rp.deliver(make_http_response_msg(200, o));
            self->unbecome();
          },
          quit_on_others(self)
        );
      },
      on(filter({"types", "*"}, "GET")) >> [=](http::request const& req) {
        VAST_DEBUG_AT(self, "got GET for", to_string(req.uri.path, "/"));
        auto rp = self->make_response_promise();
        message_builder mb;
        mb.append("show");
        mb.append("schema");
        self->send(node, mb.to_message());
        self->become(
          keep_behavior,
          [self, rp, type=req.uri.path[1]](json const& schema) {
            VAST_DEBUG_AT(self, "got schema from NODE");
            json::object result;
            auto nodes = get<json::object>(schema);
            VAST_ASSERT(nodes != nullptr);
            for (auto node_pair : *nodes) {
              auto types = get<json::object>(node_pair.second);
              VAST_ASSERT(types != nullptr);
              for (auto type_pair : *types)
                if (type_pair.first == type) {
                  result[node_pair.first] = type_pair.second;
                  break;
                }
            }
            if (result.empty())
              rp.deliver(make_http_response_msg(404, "no such type: " + type));
            else
              rp.deliver(make_http_response_msg(200, result));
            self->unbecome();
          },
          quit_on_others(self)
        );
      },
      [=](http::request const& req) {
        VAST_DEBUG_AT(self, "got unsupported API call:", req.method, req.uri);
        auto msg = "unsupported API call: " + to_string(req.method) + ' '
                     + to_string(req.uri);
        json::object o{{"error", std::move(msg)}};
        return make_http_response_msg(400, o);
      },
      quit_on_others(self)
    };
  }
};

// When the HTTP broker accepts a connection, it spawns this worker which is in
// charge of the HTTP session.
behavior http_worker(broker* self, connection_handle conn, actor mediator) {
  // At this point the REST API doesn't support POST requests with data in the
  // body, so the maximum request size probably won't exceed 4096 bytes. Adjust
  // if this no longer holds true.
  self->configure_read(conn, receive_policy::at_most(4096));
  // Because we spin up a new job actor per request, jobs can complete in an
  // order different from the requests have arrived. This would violate the
  // HTTP invariant that each message gets its corresponding response, thereby
  // breaking pipelining. To fix this issue, we associating a sequence number
  // with each job.
  auto request_id = std::make_shared<size_t>(0);
  auto response_id = std::make_shared<size_t>(0);
  return {
    [=](connection_closed_msg const&) {
      VAST_DEBUG_AT("http-worker", "terminates after remote connection closed");
      self->quit(exit::done);
    },
    [=](exit_msg const& msg) {
      self->quit(msg.reason);
    },
    [=](new_data_msg const& msg) {
      auto request = to<http::request>(msg.buf);
      if (!request) {
        VAST_ERROR_AT("http-worker", "received malformed HTTP request");
        return;
      }
      auto& rid = *request_id;
      // Spawn a helper actor to avoid blocking in the broker context.
      auto job = self->spawn(
        [=, worker=broker_ptr(self)](event_based_actor* job) {
          job->send(mediator, std::move(*request));
          job->become(
            [=](std::string const&) {
              auto response = make_message(response_atom::value, rid)
                                + job->current_message() + make_message(false);
              job->send(worker, response);
              job->quit();
            },
            [=](std::string const&, bool more) {
              auto response = make_message(response_atom::value, rid)
                                + job->current_message();
              job->send(worker, response);
              if (!more)
                job->quit();
            },
            after(time::minutes(2)) >> [=] {
              auto msg = "failed to answer request after 2 minutes";
              VAST_ERROR_AT("http-job#" << job->id(), msg);
              auto response = make_http_response_msg(500, msg);
              job->send(self, make_message(response_atom::value, rid, false)
                                + response);
              job->quit(exit::timeout);
            }
          );
        }
      );
      ++rid;
    },
    [=](response_atom, size_t rid, std::string const& response, bool more)
      -> maybe<skip_message_t> {
      if (rid != *response_id)
        return skip_message();
      if (!more)
        ++*response_id;
      self->write(conn, response.size(), response.c_str());
      self->flush(conn);
      return nil;
    },
    quit_on_others(self)
  };
}

} // namespace <anonymous>

behavior http_broker(broker* self, actor const& node) {
  VAST_DEBUG_AT("http-broker#" << self->id(), "spawned");
  auto med = self->spawn<linked>(mediator::make, node);
  return {
    [=](new_connection_msg const& msg) {
      VAST_DEBUG_AT("http-broker#" << self->id(), "got new HTTP connection");
      auto worker = self->fork(http_worker, msg.handle, med);
      self->attach_functor([=](uint32_t reason) {
        anon_send_exit(worker, reason);
      });
    },
    quit_on_others(self)
  };
}

} // namespace vast
