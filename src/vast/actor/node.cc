#include <csignal>
#include <cstdlib>

#include <algorithm>
#include <iostream>
#include <type_traits>

// TODO: remove
#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/config.h"
#include "vast/logger.h"
#include "vast/time.h"
#include "vast/expression.h"
#include "vast/query_options.h"
#include "vast/actor/archive.h"
#include "vast/actor/identifier.h"
#include "vast/actor/importer.h"
#include "vast/actor/index.h"
#include "vast/actor/exporter.h"
#include "vast/actor/node.h"
#include "vast/actor/sink/spawn.h"
#include "vast/actor/source/spawn.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/endpoint.h"
#include "vast/concept/parseable/vast/key.h"
#include "vast/concept/printable/vast/expression.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"
#include "vast/concept/parseable/vast/detail/to_expression.h"
#include "vast/expr/normalize.h"
#include "vast/io/compression.h"
#include "vast/io/file_stream.h"
#include "vast/util/assert.h"
#include "vast/util/flat_set.h"
#include "vast/util/string.h"

#ifdef VAST_HAVE_GPERFTOOLS
#include "vast/actor/profiler.h"
#endif

using namespace std::string_literals;

namespace vast {

node::state::state(local_actor* self) : basic_state{self, "node"} { }

path const& node::log_path() {
  auto secs = time::now().time_since_epoch().seconds();
  static auto const ts = std::to_string(secs);
  static auto const pid = std::to_string(util::process_id());
  static auto const dir = path{"log"} / (ts + '_' + pid);
  return dir;
}

namespace {

// Takes a label and makes it full-qualified (unless it is already).
std::string qualify(std::string const& label, std::string const& name) {
  auto split = util::split_to_str(label, "@");
  return split[0] + '@' + (split.size() == 1 ? name : split[1]);
}

// Takes a label for an actor and returns the corresponding key for the store.
std::string make_actor_key(std::string const& label, std::string const& name) {
  auto split = util::split_to_str(label, "@");
  auto& n = split.size() == 1 ? name : split[1];
  return key::str("actors", n, split[0]);
}

behavior spawn_core(event_based_actor* self,
                    stateful_actor<node::state>* node) {
  return others >> [=] {
    auto rp = self->make_response_promise();
    std::string id_batch_size;
    std::string archive_comp;
    std::string archive_segments;
    std::string archive_size;
    std::string index_events;
    std::string index_active;
    std::string index_passive;
    // These must be kept in sync with the individual options for each actor.
    auto r = self->current_message().extract_opts({
      {"identifier-batch-size", "", id_batch_size},
      {"archive-compression", "", archive_comp},
      {"archive-segments", "", archive_segments},
      {"archive-size", "", archive_size},
      {"index-events", "", index_events},
      {"index-active", "", index_active},
      {"index-passive", "", index_passive}
    });
    if (!r.error.empty()) {
      VAST_ERROR_AT(node, "failed to parse spawn core args:", r.error);
      rp.deliver(make_message(error{std::move(r.error)}));
      return;
    }
    // Spawn IDENTIFIER.
    auto msg = make_message("spawn", "identifier");
    if (r.opts.count("identifier-batch-size") > 0)
      msg = msg + make_message("--batch-size=", id_batch_size);
    self->send(node, msg);
    // Spawn ARCHIVE.
    msg = make_message("spawn", "archive");
    if (r.opts.count("archive-compression") > 0)
      msg = msg + make_message("--compression=" + archive_comp);
    if (r.opts.count("archive-segments") > 0)
      msg = msg + make_message("--segments=" + archive_segments);
    if (r.opts.count("archive-size") > 0)
      msg = msg + make_message("--size=" + archive_size);
    self->send(node, msg);
    // Spawn INDEX.
    msg = make_message("spawn", "index");
    if (r.opts.count("index-events") > 0)
      msg = msg + make_message("--events=" + index_events);
    if (r.opts.count("index-active") > 0)
      msg = msg + make_message("--active=" + index_active);
    if (r.opts.count("index-passive") > 0)
      msg = msg + make_message("--passive=" + index_passive);
    self->send(node, msg);
    auto replies = std::make_shared<size_t>(3 + 1);
    self->become(
      [&](error& e) {
        VAST_ERROR_AT(node, "failed to spawn core actor:", e);
        rp.deliver(make_message(std::move(e)));
        self->quit(exit::error);
      },
      [=](actor const&) {
        --*replies;
        if (*replies == 1) {
          self->send(node, "spawn", "importer", "-a");
        } else if (*replies == 0) {
          rp.deliver(make_message(ok_atom::value));
          self->quit();
        } else {
          // Wait until ARCHIVE, INDEX, and IDENTIFIER have spawned.
        }
      }
    );
  };
}

behavior spawn_actor(event_based_actor* self,
                     stateful_actor<node::state>* node) {
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
      VAST_DEBUG_AT(node, "records new actor in key-value store");
      auto k = key::str("actors", node->state.desc, label);
      self->send(node->state.store, put_atom::value, k, a, type);
      self->become(
        [=](ok_atom) {
          a->attach_functor([store=node->state.store, k](uint32_t) {
            anon_send(store, delete_atom::value, k);
          });
          rp.deliver(make_message(a));
          self->quit();
        }
      );
    };
    // Continuation to spawn an actor.
    message_handler spawning = {
      on("identifier") >> [=] {
        auto batch_size = event_id{128};
        auto r = self->current_message().extract_opts({
          {"batch-size,b", "the batch size to start from", batch_size}
        });
        if (!r.error.empty()) {
          rp.deliver(make_message(error{std::move(r.error)}));
          self->quit(exit::error);
          return;
        }
        auto i = spawn(identifier::make, node->state.store,
                       node->state.dir / "id", batch_size);
        save_actor(actor_cast<actor>(i), "identifier");
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
        auto a = spawn<priority_aware>(archive::make,
                                       node->state.dir / "archive",
                                       segments, size, method);
        self->send(a, node->state.accountant);
        save_actor(actor_cast<actor>(a), "archive");
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
        auto idx = spawn<priority_aware>(index::make,
                                         node->state.dir / "index", events,
                                         passive, active);
        self->send(idx, node->state.accountant);
        save_actor(std::move(idx), "index");
      },
      on("importer", any_vals) >> [=] {
        auto r = self->current_message().extract_opts({
          {"auto-connect,a",
           "connect to available identifier, archives, indexes"}
        });
        auto imp = spawn<priority_aware>(importer::make);
        if (r.opts.count("auto-connect") > 0) {
          self->send(node->state.store, list_atom::value,
                     key::str("actors", node->state.desc));
          self->become(
            [=](std::map<std::string, message>& m) {
              for (auto& p : m)
                p.second.apply({
                  [&](actor const& a, std::string const& type) {
                    VAST_ASSERT(a != invalid_actor);
                    if (type == "archive")
                      self->send(imp, actor_cast<archive::type>(a));
                    else if (type == "index")
                      self->send(imp, put_atom::value, index_atom::value, a);
                    else if (type == "identifier")
                      self->send(imp, put_atom::value, identifier_atom::value,
                                 a);
                  }
               });
              save_actor(imp, "importer");
            }
          );
        } else {
          save_actor(imp, "importer");
        }
      },
      on("exporter", any_vals) >> [=] {
        auto events = uint64_t{0};
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
        VAST_VERBOSE_AT(node, "got query:", str);
        auto query_opts = no_query_options;
        if (r.opts.count("continuous") > 0)
          query_opts = query_opts + continuous;
        if (r.opts.count("historical") > 0)
          query_opts = query_opts + historical;
        if (r.opts.count("unified") > 0)
          query_opts = unified;
        if (query_opts == no_query_options) {
          VAST_ERROR_AT(node, "got query without options (-h, -c, -u)");
          rp.deliver(make_message(error{"missing query options (-h, -c, -u)"}));
          self->quit(exit::error);
          return;
        }
        VAST_DEBUG_AT(node, "parses expression");
        auto expr = detail::to_expression(str);
        if (!expr) {
          VAST_VERBOSE_AT(node, "ignores invalid query:", str);
          rp.deliver(make_message(std::move(expr.error())));
          self->quit(exit::error);
          return;
        }
        *expr = expr::normalize(*expr);
        VAST_VERBOSE_AT(node, "normalized query to", *expr);
        auto exp = self->spawn(exporter::make, *expr, query_opts);
        self->send(exp, node->state.accountant);
        self->send(exp, extract_atom::value, events);
        if (r.opts.count("auto-connect") > 0) {
          self->send(node->state.store, list_atom::value,
                     key::str("actors", node->state.desc));
          self->become(
            [=](std::map<std::string, message>& m) {
              for (auto& p : m)
                p.second.apply({
                  [&](actor const& a, std::string const& type) {
                    VAST_ASSERT(a != invalid_actor);
                    if (type == "archive")
                      self->send(exp, actor_cast<archive::type>(a));
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
        self->send(*src, node->state.accountant);
        if (r.opts.count("auto-connect") > 0) {
          self->send(node->state.store, list_atom::value,
                     key::str("actors", node->state.desc));
          self->become(
            [=](std::map<std::string, message>& m) {
              for (auto& p : m)
                p.second.apply({
                  [&](actor const& a, std::string const& type) {
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
        self->send(*snk, node->state.accountant);
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
        auto s = std::chrono::seconds(resolution);
        auto prof = spawn<detached>(profiler::make,
                                    node->state.dir / node::log_path(), s);
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
    self->send(node->state.store, exists_atom::value,
               qualify(label, node->state.desc));
    self->send(self, std::move(params));
    self->become(
      [=](bool exists) {
        if (exists) {
          VAST_ERROR_AT(node, "aborts spawn: actor", label, "exists already");
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

behavior send_run(event_based_actor* self,
                  stateful_actor<node::state>* node) {
  return on("send", val<std::string>, "run") >> [=](std::string const& arg,
                                                    std::string const&) {
    auto rp = self->make_response_promise();
    self->send(node->state.store, get_atom::value,
               make_actor_key(arg, node->state.desc));
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

behavior send_flush(event_based_actor* self,
                    stateful_actor<node::state>* node) {
  return on("send", val<std::string>, "flush") >> [=](std::string const& arg,
                                                      std::string const&) {
    auto rp = self->make_response_promise();
    self->send(node->state.store, get_atom::value,
               make_actor_key(arg, node->state.desc));
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

behavior quit_actor(event_based_actor* self,
                    stateful_actor<node::state>* node) {
  return on("quit", arg_match) >> [=](std::string const& arg) {
    auto rp = self->make_response_promise();
    self->send(node->state.store, get_atom::value,
               make_actor_key(arg, node->state.desc));
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

behavior connect(event_based_actor* self,
                 stateful_actor<node::state>* node) {
  return {
    on("connect", arg_match) >> [=](std::string const& source,
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
            self->send(src, actor_cast<archive::type>(snk));
          } else if (snk_type == "index") {
            self->send(src, put_atom::value, index_atom::value, snk);
          } else {
            rp.deliver(make_message(error{"invalid importer sink: ",
                                          snk_type}));
            self->quit(exit::error);
            return;
          }
        } else if (src_type == "exporter") {
          if (snk_type == "archive") {
            self->send(src, actor_cast<archive::type>(snk));
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
        auto k = key::str("topology", src_fqn, snk_fqn);
        auto del = [store=node->state.store, k](uint32_t) {
          anon_send(store, delete_atom::value, k);
        };
        src->attach_functor(del);
        snk->attach_functor(del);
        self->send(node->state.store, put_atom::value, k);
        self->become(
          [=](ok_atom) {
            rp.deliver(self->current_message());
            self->quit();
          }
        );
      };
      VAST_DEBUG_AT(node, "checks if link exists:", source, "->", sink);
      self->send(node->state.store, list_atom::value,
                 key::str("topology", qualify(source, node->state.desc)));
      self->become(
        [=](std::map<std::string, message> const& vals) {
          auto pred = [=](auto& p) {
            auto k = to<key>(p.first);
            VAST_ASSERT(k && !k->empty());
            return k->back() == qualify(sink, node->state.desc);
          };
          if (std::find_if(vals.begin(), vals.end(), pred) != vals.end()) {
            rp.deliver(make_message(
              error{"connection already exists: ", source, " -> ", sink}));
            self->quit(exit::error);
          }
          VAST_VERBOSE_AT(node, "connects actors:", source, "->", sink);
          self->send(node->state.store, get_atom::value,
                     make_actor_key(source, node->state.desc));
          self->send(node->state.store, get_atom::value,
                     make_actor_key(sink, node->state.desc));
          self->become(
            [=](actor const& src, std::string const& src_type) {
              self->become(
                [=](actor const& snk, std::string const& snk_type) {
                  connect_actors(src, src_type,
                                 qualify(source, node->state.desc), snk,
                                 snk_type, qualify(sink, node->state.desc));
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
    log_others(node)
  };
}

behavior disconnect(event_based_actor* self,
                    stateful_actor<node::state>* node) {
  return on("disconnect", arg_match) >> [=](std::string const& source,
                                            std::string const& sink) {
    auto rp = self->make_response_promise();
    VAST_VERBOSE_AT(node, "disconnects actors:", source, "->", sink);
    auto k = key::str("topology", qualify(source, node->state.desc),
                      qualify(sink, node->state.desc));
    // FIXME: we currently only remove the edge from the topology
    // graph, but do not remove the source/sink at the actual actors.
    self->send(node->state.store, delete_atom::value, std::move(k));
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

behavior show(event_based_actor* self,
              stateful_actor<node::state>* node) {
  return on("show", any_vals) >> [=] {
    auto rp = self->make_response_promise();
    auto r = self->current_message().extract_opts({
      {"verbose,v", "show values in addition to keys"},
    });
    auto& arg = r.remainder.take_right(1).get_as_mutable<std::string>(0);
    std::string result;
    if (arg == "nodes") {
      arg = key::str("nodes");
    } else if (arg == "peers") {
      arg = key::str("peers");
    } else if (arg == "actors") {
      arg = key::str("actors");
    } else if (arg == "topology") {
      arg = key::str("topology");
    } else {
      rp.deliver(make_message(error{"show: invalid argument \"", arg, '"'}));
      self->quit();
      return;
    }
    auto pred = [verbose=r.opts.count("verbose")](auto& p) -> std::string {
      return p.first + (verbose ? " -> " + to_string(p.second) : "");
    };
    self->send(node->state.store, list_atom::value, std::move(arg));
    self->become(
      [=](std::map<std::string, message> const& values) {
        auto str = util::join(values.begin(), values.end(), "\n", pred);
        rp.deliver(make_message(std::move(str)));
        self->quit();
      }
    );
  };
}

behavior store_get_actor(event_based_actor* self,
                         stateful_actor<node::state>* node) {
  return [=](store_atom, get_atom, actor_atom, std::string const& label) {
    auto rp = self->make_response_promise();
    self->send(node->state.store, get_atom::value,
               make_actor_key(label, node->state.desc));
    self->become(
      [=](actor const&, std::string const&) {
        auto response = self->current_message().take(1)
                          + make_message(qualify(label, node->state.desc))
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

behavior request_peering(event_based_actor* self,
                         stateful_actor<node::state>* node) {
  return on("peer", arg_match) >> [=](std::string const& peer_endpoint) {
    auto rp = self->make_response_promise();
    auto ep = to<endpoint>(peer_endpoint);
    if (!ep) {
      rp.deliver(make_message(error{"invalid endpoint: ", peer_endpoint}));
      self->quit(exit::error);
      return;
    }
    // Use localhost:42000 by default.
    if (ep->host.empty())
      ep->host = "127.0.0.1";
    if (ep->port == 0)
      ep->port = 42000;
    VAST_DEBUG_AT(node, "connects to", ep->host << ':' << ep->port);
    actor peer;
    try {
      peer = caf::io::remote_actor(ep->host.c_str(), ep->port);
    } catch (caf::network_error const& e) {
      VAST_ERROR_AT(node, "failed to connect to peer", ep->host, ':', ep->port);
      rp.deliver(make_message(error{"failed to connect to peer ",
                                    ep->host, ':', ep->port, ", ", e.what()}));
      self->quit(exit::error);
      return;
    }
    VAST_DEBUG_AT(node, "sends follower request");
    self->send(node->state.store, follower_atom::value); // step down
    self->send(peer, store_atom::value, peer_atom::value, node,
               node->state.store, node->state.desc);
    self->become(
      [=](ok_atom, std::string const& peer_name) {
        VAST_INFO_AT(node, "got peering response from", peer_name);
        rp.deliver(make_message(ok_atom::value));
        self->quit();
      },
      [=](error& e) {
        rp.deliver(make_message(std::move(e)));
        self->quit(exit::error);
      }
    );
  };
}

behavior respond_to_peering(event_based_actor* self,
                            stateful_actor<node::state>* node) {
  return [=](store_atom, peer_atom, actor const& peer, actor const& peer_store,
        std::string const& peer_name) {
    auto rp = self->make_response_promise();
    // FIXME: check for conflicting names in the store as well.
    if (peer_name == node->state.desc) {
      VAST_WARN_AT(node, "ignores new peer with duplicate name");
      rp.deliver(make_message(error{"duplicate peer name"}));
      self->quit(exit::error);
      return;
    }
    VAST_INFO_AT(node, "got new peer:", peer_name);
    auto key1 = key::str("peers", node->state.desc, peer_name);
    auto key2 = key::str("peers", peer_name, node->state.desc);
    self->send(node->state.store, put_atom::value, key1, peer);
    self->send(node->state.store, put_atom::value, key2, node);
    self->become(
      [=](ok_atom) {
        self->become(
          [=](ok_atom) {
            self->send(node->state.store, follower_atom::value, add_atom::value,
                       peer_store);
            self->become(
              [=](ok_atom, key_value_store::storage const& delta) {
                peer->attach_functor(
                  [store=node->state.store, key1, key2](uint32_t) {
                    anon_send(store, delete_atom::value, key1);
                    anon_send(store, delete_atom::value, key2);
                  }
                );
                if (delta.empty()) {
                  rp.deliver(make_message(ok_atom::value, node->state.desc));
                  self->quit();
                  return;
                }
                // Add peer state.
                VAST_DEBUG_AT(node, "adds", delta.size(), "follower entries");
                auto num_entries = std::make_shared<size_t>(delta.size());
                for (auto& pair : delta) {
                  auto msg = make_message(put_atom::value, pair.first);
                  self->send(node->state.store, msg + pair.second);
                }
                self->become(
                  [=](ok_atom) {
                    if (--*num_entries == 0) {
                      VAST_DEBUG_AT(node, "completed replay of delta state");
                      rp.deliver(make_message(ok_atom::value,
                                              node->state.desc));
                      self->quit();
                    }
                  },
                  [=](error const& e) {
                    VAST_ERROR_AT(node, "failed to add follower state:", e);
                    rp.deliver(self->current_message());
                    self->quit();
                  }
                );
              },
              [=](error const& e) {
                VAST_ERROR_AT(node, "failed to incorporate follower:", e);
                rp.deliver(self->current_message());
                self->quit(exit::error);
              }
            );
          }
        );
      }
    );
  };
}

} // namespace <anonymous>

behavior node::make(stateful_actor<state>* self, std::string const& name,
                    path const& dir) {
  self->state.dir = dir;
  self->state.desc = name;
  self->trap_exit(true);
  // Shut down the node safely.
  auto terminate = [=](uint32_t reason) {
    // Terminates all builtin actors.
    auto terminate_builtin = [=] {
      self->monitor(self->state.accountant);
      self->monitor(self->state.store);
      self->send_exit(self->state.accountant, reason);
      self->send_exit(self->state.store, reason);
      self->become(
        [=](down_msg const&) {
          self->become([=](down_msg const&) { self->quit(reason); });
        }
      );
    };
    auto others = std::make_shared<std::vector<actor>>();
    // Terminates all non-IMPORTER actors.
    auto terminate_others = [=] {
      if (others->empty()) {
        terminate_builtin();
      } else {
        for (auto& a : *others) {
          self->monitor(a);
          VAST_DEBUG_AT(self, "sends EXIT to", a);
          self->send_exit(a, reason);
        }
        VAST_DEBUG_AT(self, "waits for", others->size(), "actors to quit");
        self->become(
          [=](down_msg const&) {
            // Order doesn't matter, only size. As usual.
            others->pop_back();
            if (others->empty())
              terminate_builtin();
          }
        );
      }
    };
    // Get all locally running actors.
    self->send(self->state.store, list_atom::value, key::str("actors", name));
    self->become(
      [=](std::map<std::string, message>& m) {
        auto importers = std::make_shared<size_t>(0);
        for (auto& p : m)
          p.second.apply({
            [&](actor const& a, std::string const& type) {
              VAST_ASSERT(a != invalid_actor);
              if (type == "importer") {
                self->monitor(a);
                // Begin with shutting down IMPORTERs.
                VAST_DEBUG_AT(self, "sends EXIT to importer" << a);
                self->send_exit(a, reason);
                ++*importers;
              } else {
                others->push_back(a);
              }
            }
          });
        if (*importers == 0) {
          terminate_others();
        } else {
          VAST_DEBUG_AT(self, "waits for", *importers, "importers to quit");
          self->become(
            [=](down_msg const&) {
              if (--*importers == 0)
                terminate_others();
            }
          );
        }
      }
    );
  };
  behavior operating = {
    [=](exit_msg const& msg) {
      VAST_DEBUG_AT(self, "got EXIT from", msg.source);
      terminate(msg.reason);
    },
    [=](down_msg const& msg) {
      VAST_DEBUG_AT(self, "got DOWN from", msg.source);
    },
    on("stop") >> [=] {
      VAST_VERBOSE_AT(self, "stops");
      terminate(exit::stop);
      return make_message(ok_atom::value);
    },
    on("peer", arg_match) >> [=](std::string const& endpoint) {
      VAST_VERBOSE_AT(self, "attempts to peer with", endpoint);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return request_peering(job, self); }
      ));
    },
    on("spawn", "core", any_vals) >> [=] {
      VAST_VERBOSE_AT(self, "spawns core actors");
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return spawn_core(job, self); }
      ));
    },
    on("spawn", any_vals) >> [=] {
      VAST_VERBOSE_AT(self, "spawns",
                      self->current_message().get_as<std::string>(1));
      self->current_message() = self->current_message().drop(1);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return spawn_actor(job, self); }
      ));
    },
    on("send", val<std::string>, "run") >> [=](std::string const& arg,
                                               std::string const&) {
      VAST_VERBOSE_AT(self, "sends RUN to", arg);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return send_run(job, self); }
      ));
    },
    on("send", val<std::string>, "flush") >> [=](std::string const& arg,
                                                 std::string const&) {
      VAST_VERBOSE_AT(self, "sends FLUSH to", arg);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return send_flush(job, self); }
      ));
    },
    on("quit", arg_match) >> [=](std::string const& arg) {
      VAST_VERBOSE_AT(self, "terminates actor", arg);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return quit_actor(job, self); }
      ));
    },
    on("connect", arg_match) >> [=](std::string const& source,
                                    std::string const& sink) {
      VAST_VERBOSE_AT(self, "connects", source, "->", sink);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return connect(job, self); }
      ));
    },
    on("disconnect", arg_match) >> [=](std::string const& source,
                                       std::string const& sink) {
      VAST_VERBOSE_AT(self, "disconnects", source, "->", sink);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return disconnect(job, self); }
      ));
    },
    on("show", arg_match) >> [=](std::string const& arg) {
      VAST_VERBOSE_AT(self, "shows", arg);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return show(job, self); }
      ));
    },
    on("show", "-v", arg_match) >> [=](std::string const& arg) {
      VAST_VERBOSE_AT(self, "shows", arg, "in verbose mode");
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return show(job, self); }
      ));
    },
    [=](store_atom) {
      return self->state.store;
    },
    [=](store_atom, get_atom, actor_atom, std::string const& label) {
      VAST_VERBOSE_AT(self, "got GET for actor", label);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return store_get_actor(job, self); }
      ));
    },
    [=](store_atom, peer_atom, actor const&, actor const&,
        std::string const& peer_name) {
      VAST_VERBOSE_AT(self, "got peering response from", peer_name);
      self->forward_to(self->spawn(
        [=](event_based_actor* job) { return respond_to_peering(job, self); }
      ));
    },
    on(store_atom::value, any_vals) >> [=] {
      // We relay any non-peering request prefixed with the STORE atom to the
      // key-value store, but strip the atom first.
      self->current_message() = self->current_message().drop(1);
      self->forward_to(self->state.store);
    },
    others >> [=] {
      std::string cmd;
      self->current_message().extract(
        [&](std::string const& s) { cmd += ' ' + s; }
      );
      if (cmd.empty())
        cmd = to_string(self->current_message());
      VAST_ERROR_AT(self, "got invalid command:" << cmd);
      return error{"invalid command syntax:", cmd};
    }
  };
  // Spawn internal actors.
  auto acc_log = dir / log_path() / "accounting.log";
  self->state.accountant = self->spawn<linked>(accountant::make, acc_log);
  self->state.store = self->spawn<linked>(key_value_store::make, dir / "meta");
  // Until we've implemented leader election, each node starts as leader.
  self->send(self->state.store, leader_atom::value);
  // Mark the global event ID as persistent.
  self->send(self->state.store, persist_atom::value, key::str("id"));
  // Register actors in store.
  self->send(self->state.store, put_atom::value, key::str("nodes", name), self);
  self->send(self->state.store, put_atom::value,
             key::str("actors", name, "accountant"), self->state.accountant);
  return {
    [=](ok_atom) {
      self->become(
        [=](ok_atom) {
          self->become(operating);
        },
        [=](error const& e) {
          VAST_ERROR(e);
          self->quit(exit::error);
        }
      );
    },
    [=](error const& e) {
      VAST_ERROR(e);
      self->quit(exit::error);
    }
  };
}

} // namespace vast
