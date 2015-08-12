#include <csignal>
#include <cstdlib>

#include <algorithm>
#include <iostream>
#include <type_traits>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/config.h"
#include "vast/logger.h"
#include "vast/time.h"
#include "vast/expression.h"
#include "vast/query_options.h"
#include "vast/actor/accountant.h"
#include "vast/actor/archive.h"
#include "vast/actor/identifier.h"
#include "vast/actor/importer.h"
#include "vast/actor/index.h"
#include "vast/actor/exporter.h"
#include "vast/actor/node.h"
#include "vast/actor/sink/spawn.h"
#include "vast/actor/source/spawn.h"
#include "vast/concept/printable/vast/expression.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"
#include "vast/expr/normalize.h"
#include "vast/io/compression.h"
#include "vast/io/file_stream.h"
#include "vast/util/assert.h"
#include "vast/util/endpoint.h"
#include "vast/util/string.h"

#ifdef VAST_HAVE_GPERFTOOLS
#include "vast/actor/profiler.h"
#endif

using namespace caf;
using namespace std::string_literals;

namespace vast {

path const& node::log_path() {
  static auto const ts
    = std::to_string(time::now().time_since_epoch().seconds());
  static auto const pid = std::to_string(util::process_id());
  static auto const dir = path{"log"} / (ts + '_' + pid);
  return dir;
}

node::node(std::string const& name, path const& dir)
  : default_actor{"node"}, name_{name}, dir_{dir} {
}

void node::on_exit() {
  store_ = invalid_actor;
  accountant_ = invalid_actor;
}

behavior node::make_behavior() {
  accountant_ = spawn<accountant<uint64_t>, linked>(dir_ / log_path() /
                                                    "accounting.log");
  store_ = spawn<key_value_store, linked>();
  send(store_, put_atom::value, "nodes/" + name_, this);
  become(
    [&](ok_atom) {
      become(operating_);
    },
    [&](error const& e)
    {
      VAST_ERROR(e);
      quit(exit::error);
    }
  );
  operating_ = {
    //
    // PUBLIC
    //
    on("stop") >> [=] {
      VAST_VERBOSE(this, "stops");
      quit(exit::stop);
      return make_message(ok_atom::value);
    },
    on("peer", arg_match) >> [=](std::string const& endpoint) {
      VAST_VERBOSE(this, "attempts to peer with", endpoint);
      forward_to(spawn([=](event_based_actor* self) {
        return request_peering(self);
      }));
    },
    on("spawn", any_vals) >> [=] {
      VAST_VERBOSE(this, "spawns new actor");
      current_message() = current_message().drop(1);
      forward_to(spawn([=](event_based_actor* self) {
        return spawn_actor(self);
      }));
    },
    on("send", val<std::string>, "run") >> [=](std::string const& arg,
                                               std::string const&) {
      VAST_VERBOSE(this, "sends RUN to", arg);
      forward_to(spawn([=](event_based_actor* self) {
        return send_run(self);
      }));
    },
    on("send", val<std::string>, "flush") >> [=](std::string const& arg,
                                                 std::string const&) {
      VAST_VERBOSE(this, "sends FLUSH to", arg);
      forward_to(spawn([=](event_based_actor* self) {
        return send_flush(self);
      }));
    },
    on("quit", arg_match) >> [=](std::string const& arg) {
      VAST_VERBOSE(this, "terminates actor", arg);
      forward_to(spawn([=](event_based_actor* self) {
        return quit_actor(self);
      }));
    },
    on("connect", arg_match) >> [=](std::string const& source,
                                    std::string const& sink) {
      VAST_VERBOSE(this, "connects", source, "->", sink);
      forward_to(spawn([=](event_based_actor* self) {
        return connect(self);
      }));
    },
    on("disconnect", arg_match) >> [=](std::string const& source,
                                       std::string const& sink) {
      VAST_VERBOSE(this, "disconnects", source, "->", sink);
      forward_to(spawn([=](event_based_actor* self) {
        return disconnect(self);
      }));
    },
    on("show", arg_match) >> [=](std::string const& arg) {
      VAST_VERBOSE(this, "shows", arg);
      forward_to(spawn([=](event_based_actor* self) {
        return show(self);
      }));
    },
    on("show", "-v", arg_match) >> [=](std::string const& arg) {
      VAST_VERBOSE(this, "shows", arg, "in verbose mode");
      forward_to(spawn([=](event_based_actor* self) {
        return show(self);
      }));
    },
    [=](store_atom) {
      return store_;
    },
    [=](store_atom, get_atom, actor_atom, std::string const& label) {
      VAST_VERBOSE(this, "got GET for actor", label);
      forward_to(spawn([=](event_based_actor* self) {
        return store_get_actor(self);
      }));
    },
    [=](store_atom, peer_atom, actor const&, actor const&,
        std::string const& peer_name) {
      VAST_VERBOSE(this, "got peering response from", peer_name);
      forward_to(spawn([=](event_based_actor* self) {
        return respond_to_peering(self);
      }));
    },
    on(store_atom::value, any_vals) >> [=]
    {
      // We relay any non-peering request prefixed with the STORE atom to the
      // key-value store, but strip the atom first.
      current_message() = current_message().drop(1);
      forward_to(store_);
    },
    others >> [=]
    {
      std::string cmd;
      current_message().extract([&](std::string const& s) { cmd += ' ' + s; });
      if (cmd.empty())
        cmd = to_string(current_message());
      VAST_ERROR("invalid command syntax:" << cmd);
      return error{"invalid command syntax:", cmd};
    }
  };
  return operating_;
}

behavior node::spawn_actor(event_based_actor* self) {
  return others >> [=] {
    auto rp = self->make_response_promise();
    // Extract options common amongst all actors. We don't use extract_opts
    // because it consumes -h if present.
    auto label = self->current_message().get_as<std::string>(0);
    auto params = self->current_message().extract({
      on("-l", arg_match) >> [&](std::string const& l) { label = l; }
    });
    // Continuation to record a spawned actor and return to the user.
    auto save_actor = [=](actor const& a, std::string const& type) {
      VAST_ASSERT(a != invalid_actor);
      VAST_DEBUG(this, "records new actor in key-value store");
      auto key = "actors/" + name_ + '/' + label;
      self->send(store_, put_atom::value, key, a, type);
      self->become(
        [=](ok_atom) {
          a->attach_functor([=](uint32_t) {
            anon_send(store_, delete_atom::value, key);
          });
          rp.deliver(make_message(a));
          self->quit();
        }
      );
    };
    // Continuation to spawn an actor.
    message_handler spawning = {
      on("identifier") >> [=] {
        auto i = spawn<identifier>(dir_);
        attach_functor([=](uint32_t ec) { anon_send_exit(i, ec); });
        save_actor(i, "identifier");
      },
      on("archive", any_vals) >> [=] {
        io::compression method;
        auto comp = "lz4"s;
        uint64_t segments = 10;
        uint64_t size = 128;
        auto r = self->current_message().extract_opts({
          {"compression,c", "compression method for event batches", comp},
          {"segments,s", "maximum number of cached segments", segments},
          {"size,m", "maximum size of segment before flushing (MB)", size}
        });
        if (!r.error.empty()) {
          rp.deliver(make_message(error{std::move(r.error)}));
          self->quit(exit::error);
          return;
        }
        if (comp == "null") {
          method = io::null;
        } else if (comp == "lz4") {
          method = io::lz4;
        } else if (comp == "snappy") {
#ifdef VAST_HAVE_SNAPPY
          method = io::snappy;
#else
          rp.deliver(make_message(error{"not compiled with snappy support"}));
          self->quit(exit::error);
          return;
#endif
        } else {
          rp.deliver(make_message(error{"unknown compression method: ", comp}));
          self->quit(exit::error);
          return;
        }
        size <<= 20; // MB'ify
        auto dir = dir_ / "archive";
        auto a = spawn<archive, priority_aware>(dir, segments, size, method);
        attach_functor([=](uint32_t ec) { anon_send_exit(a, ec); });
        self->send(a, put_atom::value, accountant_atom::value, accountant_);
        save_actor(std::move(a), "archive");
      },
      on("index", any_vals) >> [=] {
        uint64_t events = 1 << 20;
        uint64_t passive = 10;
        uint64_t active = 5;
        auto r = self->current_message().extract_opts({
          {"events,e", "maximum events per partition", events},
          {"active,a", "maximum active partitions", active},
          {"passive,p", "maximum passive partitions", passive}
        });
        if (!r.error.empty()) {
          rp.deliver(make_message(error{std::move(r.error)}));
          self->quit(exit::error);
          return;
        }
        auto dir = dir_ / "index";
        auto idx = spawn<index, priority_aware>(dir, events, passive, active);
        attach_functor([=](uint32_t ec) { anon_send_exit(idx, ec); });
        self->send(idx, put_atom::value, accountant_atom::value, accountant_);
        save_actor(std::move(idx), "index");
      },
      on("importer") >> [=] {
        auto imp = spawn<importer, priority_aware>();
        attach_functor([=](uint32_t ec) { anon_send_exit(imp, ec); });
        // send(imp, put_atom::value, accountant_atom::value, accountant_);
        save_actor(std::move(imp), "importer");
      },
      on("exporter", any_vals) >> [=] {
        auto events = uint64_t{0};
        VAST_DEBUG(to_string(self->current_message()));
        auto r = self->current_message().drop(1).extract_opts({
          {"events,e", "the number of events to extract", events},
          {"continuous,c", "marks a query as continuous"},
          {"historical,h", "marks a query as historical"},
          {"unified,u", "marks a query as unified"},
          {"auto-connect,a", "connect to available archives & indexes"}
        });
        if (!r.error.empty())
        {
          rp.deliver(make_message(error{std::move(r.error)}));
          self->quit(exit::error);
          return;
        }
        // Join remainder into single string.
        std::string str;
        for (auto i = 0u; i < r.remainder.size(); ++i) {
          if (i != 0)
            str += ' ';
          str += r.remainder.get_as<std::string>(i);
        }
        VAST_VERBOSE(this, "got query:", str);
        auto query_opts = no_query_options;
        if (r.opts.count("continuous") > 0)
          query_opts = query_opts + continuous;
        if (r.opts.count("historical") > 0)
          query_opts = query_opts + historical;
        if (r.opts.count("unified") > 0)
          query_opts = unified;
        if (query_opts == no_query_options) {
          VAST_ERROR(this, "got query without options (-h, -c, -u)");
          rp.deliver(make_message(error{"missing query options (-h, -c, -u)"}));
          self->quit(exit::error);
          return;
        }
        VAST_DEBUG(this, "parses expression");
        auto expr = detail::to_expression(str);
        if (!expr) {
          VAST_VERBOSE(this, "ignores invalid query:", str);
          rp.deliver(make_message(std::move(expr.error())));
          self->quit(exit::error);
          return;
        }
        *expr = expr::normalize(*expr);
        VAST_VERBOSE(this, "normalized query to", *expr);
        auto exp = self->spawn<exporter>(*expr, query_opts);
        attach_functor([=](uint32_t ec) { anon_send_exit(exp, ec); });
        self->send(exp, extract_atom::value, events);
        if (r.opts.count("auto-connect") > 0) {
          std::vector<caf::actor> archives;
          std::vector<caf::actor> indexes;
          self->send(store_, list_atom::value, "actors/" + name_);
          self->become(
            [=](std::map<std::string, caf::message>& m) {
              for (auto& p : m)
                p.second.apply({
                  [&](caf::actor const& a, std::string const& type) {
                    VAST_ASSERT(a != invalid_actor);
                    if (type == "archive")
                      self->send(exp, put_atom::value, archive_atom::value, a);
                    else if (type == "index")
                      self->send(exp, put_atom::value, index_atom::value, a);
                  }
               });
              save_actor(exp, "exporter");
            }
          );
        } else {
          save_actor(exp, "exporter");
        }
      },
      on("source", any_vals) >> [=] {
        auto r = self->current_message().extract_opts({
          {"auto-connect,a", "connect to available archives & indexes"}
        });
        auto src = source::spawn(self->current_message().drop(1));
        if (!src) {
          rp.deliver(make_message(std::move(src.error())));
          self->quit(exit::error);
          return;
        }
        self->send(*src, put_atom::value, accountant_atom::value, accountant_);
        attach_functor([s=*src](uint32_t ec) { anon_send_exit(s, ec); });
        if (r.opts.count("auto-connect") > 0) {
          self->send(store_, list_atom::value, "actors/" + name_);
          self->become(
            [=](std::map<std::string, caf::message>& m) {
              for (auto& p : m)
                p.second.apply({
                  [&](caf::actor const& a, std::string const& type) {
                    VAST_ASSERT(a != invalid_actor);
                    if (type == "importer")
                      self->send(*src, put_atom::value, sink_atom::value, a);
                   }
                });
              save_actor(*src, "source");
            }
          );
        } else {
          save_actor(*src, "source");
        }
      },
      on("sink", any_vals) >> [=] {
        auto snk = sink::spawn(self->current_message().drop(1));
        if (!snk) {
          rp.deliver(make_message(std::move(snk.error())));
          self->quit(exit::error);
          return;
        }
        self->send(*snk, put_atom::value, accountant_atom::value, accountant_);
        attach_functor([s=*snk](uint32_t ec) { anon_send_exit(s, ec); });
        save_actor(*snk, "sink");
      },
      on("profiler", any_vals) >> [=] {
#ifdef VAST_HAVE_GPERFTOOLS
        auto resolution = 0u;
        auto r = self->current_message().extract_opts({
          {"cpu,c", "start the CPU profiler"},
          {"heap,h", "start the heap profiler"},
          {"resolution,r", "seconds between measurements", resolution}
        });
        if (!r.error.empty()) {
          rp.deliver(make_message(error{std::move(r.error)}));
          self->quit(exit::error);
          return;
        }
        auto secs = std::chrono::seconds(resolution);
        auto prof = spawn<profiler, detached>(dir_ / log_path(), secs);
        attach_functor([=](uint32_t ec) { anon_send_exit(prof, ec); });
        if (r.opts.count("cpu") > 0)
          self->send(prof, start_atom::value, "cpu");
        if (r.opts.count("heap") > 0)
          self->send(prof, start_atom::value, "heap");
        save_actor(prof, "profiler");
#else
        rp.deliver(make_message(error{"not compiled with gperftools"}));
        self->quit(exit::error);
#endif
      },
      others >> [=] {
        auto syntax = "spawn <actor> [params]";
        rp.deliver(make_message(error{"invalid syntax, use: ", syntax}));
        self->quit(exit::error);
      }
    };
    // Check if actor exists already.
    self->send(store_, exists_atom::value, qualify(label));
    self->send(self, std::move(params));
    self->become(
      [=](bool exists) {
        if (exists) {
          VAST_ERROR(this, "aborts spawn: actor", label, "exists already");
          rp.deliver(make_message(error{"actor already exists: ", label}));
          self->quit(exit::error);
          return;
        }
        // Spawn actor.
        self->become(spawning);
      }
    );
  };
}

behavior node::send_run(event_based_actor* self) {
  return on("send", val<std::string>, "run") >> [=](std::string const& arg,
                                                    std::string const&) {
    auto rp = self->make_response_promise();
    self->send(store_, get_atom::value, make_actor_key(arg));
    self->become(
      [=](actor const& a, std::string const&) {
        self->send(a, run_atom::value);
        rp.deliver(make_message(ok_atom::value));
        self->quit();
      },
      [=](none) {
        rp.deliver(make_message(error{"no such actor: ", arg}));
        self->quit(exit::error);
      }
    );
  };
}

caf::behavior node::send_flush(event_based_actor* self) {
  return on("send", val<std::string>, "flush") >> [=](std::string const& arg,
                                                      std::string const&) {
    auto rp = self->make_response_promise();
    self->send(store_, get_atom::value, make_actor_key(arg));
    self->become(
      [=](actor const& a, std::string const& type) {
        if (!(type == "index" || type == "archive")) {
          rp.deliver(make_message(error{type, " does not support flushing"}));
          self->quit(exit::error);
          return;
        }
        self->send(a, flush_atom::value);
        self->become(
          [=](actor const& task) {
            self->monitor(task);
            self->become(
              [=](down_msg const& msg) {
                VAST_ASSERT(msg.source == task);
                rp.deliver(make_message(ok_atom::value));
                self->quit(exit::done);
              }
            );
          },
          [=](ok_atom) {
            rp.deliver(self->current_message());
            self->quit(exit::done);
          },
          [=](error const&) {
            rp.deliver(self->current_message());
            self->quit(exit::error);
          },
          others >> [=] {
            rp.deliver(make_message(error{"unexpected response to FLUSH"}));
            self->quit(exit::error);
          },
          after(time::seconds(10)) >> [=] {
            rp.deliver(make_message(error{"FLUSH timed out"}));
            self->quit(exit::error);
          }
        );
      },
      [=](none) {
        rp.deliver(make_message(error{"no such actor: ", arg}));
        self->quit(exit::error);
      }
    );
  };
}

behavior node::quit_actor(event_based_actor* self) {
  return on("quit", arg_match) >> [=](std::string const& arg) {
    auto rp = self->make_response_promise();
    self->send(store_, get_atom::value, make_actor_key(arg));
    self->become(
      [=](actor const& a, std::string const&) {
        self->send_exit(a, exit::stop);
        rp.deliver(make_message(ok_atom::value));
        self->quit();
      },
      [=](none) {
        rp.deliver(make_message(error{"no such actor: ", arg}));
        self->quit(exit::error);
      }
    );
  };
}

behavior node::connect(event_based_actor* self) {
  return {on("connect", arg_match) >> [=](std::string const& source,
                                         std::string const& sink) {
    auto rp = self->make_response_promise();
    // Continuation executing after having checked that the edge in the
    // topology graph doesn't exist already.
    auto connect_actors = [=](actor const& src, std::string const& src_type,
                              std::string const& src_fqn, actor const& snk,
                              std::string const& snk_type,
                              std::string const& snk_fqn) {
      // Wire actors based on their type.
      if (src_type == "source") {
        if (snk_type == "importer") {
          self->send(src, put_atom::value, sink_atom::value, snk);
        } else {
          rp.deliver(make_message(error{"sink not an importer: ", snk_fqn}));
          self->quit(exit::error);
          return;
        }
      } else if (src_type == "importer") {
        if (snk_type == "identifier") {
          self->send(src, put_atom::value, identifier_atom::value, snk);
        } else if (snk_type == "archive") {
          self->send(src, put_atom::value, archive_atom::value, snk);
        } else if (snk_type == "index") {
          self->send(src, put_atom::value, index_atom::value, snk);
        } else {
          rp.deliver(make_message(error{"invalid importer sink: ", snk_type}));
          self->quit(exit::error);
          return;
        }
      } else if (src_type == "exporter") {
        if (snk_type == "archive") {
          self->send(src, put_atom::value, archive_atom::value, snk);
        } else if (snk_type == "index") {
          self->send(src, put_atom::value, index_atom::value, snk);
        } else if (snk_type == "sink") {
          self->send(src, put_atom::value, sink_atom::value, snk);
        } else {
          rp.deliver(make_message(error{"invalid exporter sink: ", snk_fqn}));
          self->quit(exit::error);
          return;
        }
      } else {
        rp.deliver(make_message(error{"invalid source: ", src_type}));
        self->quit(exit::error);
        return;
      }
      // Create new edge in topology.
      auto key = "topology/" + src_fqn + '/' + snk_fqn;
      auto del = [=](uint32_t) { anon_send(store_, delete_atom::value, key); };
      src->attach_functor(del);
      snk->attach_functor(del);
      self->send(store_, put_atom::value, key);
      self->become(
        [=](ok_atom) {
          rp.deliver(self->current_message());
          self->quit();
        }
      );
    };
    VAST_DEBUG(this, "checks if link exists:", source, "->", sink);
    self->send(store_, list_atom::value, "topology/" + qualify(source));
    self->become(
      [=](std::map<std::string, message> const& vals) {
        auto pred = [=](auto& p) {
          return util::split_to_str(p.first, "/").back() == qualify(sink);
        };
        if (std::find_if(vals.begin(), vals.end(), pred) != vals.end()) {
          rp.deliver(make_message(
            error{"connection already exists: ", source, " -> ", sink}));
          self->quit(exit::error);
        }
        VAST_VERBOSE(this, "connects actors:", source, "->", sink);
        self->send(store_, get_atom::value, make_actor_key(source));
        self->send(store_, get_atom::value, make_actor_key(sink));
        self->become(
          [=](actor const& src, std::string const& src_type) {
            self->become(
              [=](actor const& snk, std::string const& snk_type) {
                connect_actors(src, src_type, qualify(source),
                               snk, snk_type, qualify(sink));
              },
              [=](none) {
                rp.deliver(make_message(error{"no such sink: ", sink}));
                self->quit(exit::error);
              }
            );
          },
          [=](none) {
            rp.deliver(make_message(error{"no such source: ", source}));
            self->quit(exit::error);
          }
        );
      }
    );
  },
  others >> [=] {
    VAST_ERROR(this, "got unexpected message:",
               to_string(self->current_message()));
  }};
}

behavior node::disconnect(event_based_actor* self) {
  return on("disconnect", arg_match) >> [=](std::string const& source,
                                            std::string const& sink) {
    auto rp = self->make_response_promise();
    VAST_VERBOSE(this, "disconnects actors:", source, "->", sink);
    auto key = "topology/" + qualify(source) + '/' + qualify(sink);
    // FIXME: we currently only remove the edge from the topology
    // graph, but do not remove the source/sink at the actual actors.
    self->send(store_, delete_atom::value, std::move(key));
    self->become(
      [=](uint64_t n) {
        if (n == 0)
          rp.deliver(
            make_message(error{"no such link: ", source, " -> ", sink}));
        else
          rp.deliver(make_message(ok_atom::value));
        self->quit();
      }
    );
  };
}

behavior node::show(event_based_actor* self) {
  return on("show", any_vals) >> [=] {
    auto rp = self->make_response_promise();
    auto& arg = self->current_message().take_right(1).get_as<std::string>(0);
    auto r = self->current_message().extract_opts({
      {"verbose,-v", "show values in addition to keys"},
    });
    std::string result;
    std::string key;
    if (arg == "nodes") {
      key = "nodes/";
    } else if (arg == "peers") {
      key = "peers/";
    } else if (arg == "actors") {
      key = "actors/";
    } else if (arg == "topology") {
      key = "topology/";
    } else {
      rp.deliver(make_message(error{"show: invalid argument"}));
      self->quit();
      return;
    }
    auto pred = [verbose=r.opts.count("verbose")](auto& p) -> std::string {
      return p.first + (verbose ? " -> " + to_string(p.second) : "");
    };
    self->send(store_, list_atom::value, std::move(key));
    self->become(
      [=](std::map<std::string, message> const& values) {
        auto str = util::join(values.begin(), values.end(), "\n", pred);
        rp.deliver(make_message(std::move(str)));
        self->quit();
      }
    );
  };
}

behavior node::store_get_actor(caf::event_based_actor* self) {
  return [=](store_atom, get_atom, actor_atom, std::string const& label) {
    auto rp = self->make_response_promise();
    self->send(store_, get_atom::value, make_actor_key(label));
    self->become(
      [=](actor const&, std::string const&) {
        auto response = self->current_message().take(1)
                          + make_message(qualify(label))
                          + self->current_message().take_right(1);
        rp.deliver(std::move(response));
        self->quit();
      },
      [=](none) {
        rp.deliver(self->current_message());
        self->quit();
      }
    );
  };
}

behavior node::request_peering(event_based_actor* self) {
  return on("peer", arg_match) >> [=](std::string const& endpoint) {
    auto rp = self->make_response_promise();
    // Continuation connecting all IMPORTERs with a remote IDENTIFIER.
    message_handler connecting_importers = {
      [=](std::map<std::string, caf::message>& m) {
        std::map<std::string, actor> importers;
        std::pair<std::string, caf::actor> remote_identifier;
        for (auto& p : m)
          p.second.apply({[&](caf::actor const& a, std::string const& type) {
            if (util::starts_with(p.first, "actors/" + name_)) {
              if (type == "importer")
                importers.emplace(p.first, a);
            } else if (type == "identifier") {
              remote_identifier = {p.first, a}; // there can be only one.
            }
          }});
        // Turns a key like "/actors/<node>/<label>" into "label@node".
        auto parse_actor_key = [](std::string const& key) -> std::string {
          auto split = util::split_to_str(key, "/");
          VAST_ASSERT(split.size() > 2);
          return split[2] + '@' + split[1];
        };
        // Create one job per connection.
        auto id_fqn = parse_actor_key(remote_identifier.first);
        VAST_DEBUG(this, "connects", importers.size(),
                   "importer(s) with a remote identifier");
        for (auto& p : importers) {
          auto job = self->spawn([=](event_based_actor* job_self) {
            return connect(job_self);
          });
          self->send(job, "connect", parse_actor_key(p.first), id_fqn);
        }
        // Wait for sub-jobs to terminate.
        auto num_sub_jobs = std::make_shared<size_t>(importers.size());
        self->become(
          [=](ok_atom) {
            if (--*num_sub_jobs == 0) {
              rp.deliver(make_message(ok_atom::value));
              self->quit();
            }
          },
          [=](error& e) {
            VAST_ERROR(this, "failed to connect importers to identifier:", e);
            rp.deliver(make_message(std::move(e)));
            self->quit(exit::error);
          }
        );
      }
    };
    // Continuation that connects to a remote endpoint and sends peering
    // request.
    auto connect_and_peer = [=](bool has_local_identifier) {
      auto host = "127.0.0.1"s;
      auto port = uint16_t{42000};
      if (!util::parse_endpoint(endpoint, host, port)) {
        rp.deliver(make_message(error{"invalid endpoint: ", endpoint}));
        self->quit(exit::error);
      }
      VAST_DEBUG(this, "connects to", host << ':' << port);
      actor peer;
      try {
        peer = caf::io::remote_actor(host.c_str(), port);
      } catch (caf::network_error const& e) {
        rp.deliver(make_message(error{"failed to connect to ", host, ':', port,
                                ", ", e.what()}));
        self->quit(exit::error);
      }
      VAST_DEBUG(this, "sends peering request");
      self->send(peer, store_atom::value, peer_atom::value, this, store_,
                 name_);
      self->become(
        [=](ok_atom, std::string const& peer_name) {
          VAST_INFO(this, "got peering response from", peer_name);
          peer->attach_functor(
            [=](uint32_t) {
              auto key1 = "actors/" + name_ + '/' + peer_name;
              auto key2 = "actors/" + peer_name + '/' + name_;
              anon_send(store_, delete_atom::value, key1);
              anon_send(store_, delete_atom::value, key2);
            }
          );
          if (has_local_identifier) {
            VAST_DEBUG(this, "connects importers with remote identifier");
            self->send(store_, list_atom::value, "actors/");
            self->become(connecting_importers);
          } else {
            rp.deliver(make_message(ok_atom::value));
            self->quit();
          }
        },
        [=](error& e) {
          rp.deliver(make_message(std::move(e)));
          self->quit(exit::error);
        }
      );
    };
    // Peering nodes currently share the same IDENTIFIER.  Consequently, we
    // must terminate a local IDENTIFIER if it runs. If it has already
    // handed out IDs in the past, we can no longer peer with the remote
    // side. (This is only a quick-fix until we've rolled out the
    // Raft-based meta store.)
    self->send(store_, list_atom::value, "actors/" + name_);
    self->become(
      [=](std::map<std::string, caf::message>& m) {
        caf::actor identifier;
        for (auto& p : m)
          p.second.apply({
            [&](caf::actor const& a, std::string const& type) {
              if (type == "identifier")
                identifier = a;
            }
          });
        if (identifier == invalid_actor) {
          connect_and_peer(false);
          return;
        }
        self->send(identifier, id_atom::value);
        self->become(
          [=](event_id id) {
            if (id > 0) {
              auto str = "cannot peer with node which used identifier";
              VAST_ERROR(this, str);
              rp.deliver(make_message(error{str}));
              self->quit(exit::error);
            } else {
              VAST_DEBUG(this, "shuts down local identifier");
              self->send_exit(identifier, exit::done);
              connect_and_peer(true);
            }
          }
        );
      }
    );
  };
}

behavior node::respond_to_peering(event_based_actor* self) {
  return [=](store_atom, peer_atom, actor const& peer, actor const& peer_store,
        std::string const& peer_name) {
    auto rp = self->make_response_promise();
    if (peer_name == name_) {
      VAST_WARN(this, "ignores new peer with duplicate name");
      rp.deliver(make_message(error{"duplicate peer name"}));
      self->quit(exit::error);
      return;
    }
    VAST_INFO(this, "got new peer:", peer_name);
    auto key1 = "peers/" + name_ + '/' + peer_name;
    auto key2 = "peers/" + peer_name + '/' + name_;
    self->send(store_, put_atom::value, key1, peer);
    self->send(store_, put_atom::value, key2, this);
    self->become(
      [=](ok_atom) {
        self->become(
          [=](ok_atom) {
            self->send(store_, peer_atom::value, peer_store);
            self->become(
              [=](ok_atom) {
                peer->attach_functor(
                  [=](uint32_t) {
                    anon_send(store_, delete_atom::value, key1);
                    anon_send(store_, delete_atom::value, key2);
                  }
                );
                rp.deliver(make_message(ok_atom::value, name_));
                self->quit();
              },
              [=](error& e) {
                rp.deliver(make_message(std::move(e)));
                self->quit(exit::error);
              }
            );
          }
        );
      }
    );
  };
}

std::string node::qualify(std::string const& label) const {
  auto split = util::split_to_str(label, "@");
  return split[0] + '@' + (split.size() == 1 ? name_ : split[1]);
}

std::string node::make_actor_key(std::string const& label) const {
  auto split = util::split_to_str(label, "@");
  auto& name = split.size() == 1 ? name_ : split[1];
  return "actors/" + name + '/' + split[0];
}

} // namespace vast
