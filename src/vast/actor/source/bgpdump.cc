#include "vast/actor/source/bgpdump.h"

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric.h"
#include "vast/concept/parseable/string/any.h"
#include "vast/concept/parseable/string/char_class.h"
#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/subnet.h"
#include "vast/concept/parseable/vast/time.h"
#include "vast/util/string.h"

namespace vast {
namespace source {

bgpdump::bgpdump(std::unique_ptr<io::input_stream> is)
  : line_based<bgpdump>{"bgpdump-source", std::move(is)}
{
  std::vector<type::record::field> fields;
  fields.emplace_back("timestamp", type::time_point{});
  fields.emplace_back("source_ip", type::address{});
  fields.emplace_back("source_as", type::count{});
  fields.emplace_back("prefix", type::subnet{});
  fields.emplace_back("as_path", type::vector{type::count{}});
  fields.emplace_back("origin_as", type::count{});
  fields.emplace_back("origin", type::string{});
  fields.emplace_back("nexthop", type::address{});
  fields.emplace_back("local_pref", type::count{});
  fields.emplace_back("med", type::count{});
  fields.emplace_back("community", type::string{});
  fields.emplace_back("atomic_aggregate", type::string{});
  fields.emplace_back("aggregator", type::string{});
  announce_type_ = type::record{fields};
  announce_type_.name("bgpdump::announcement");

  route_type_ = type::record{std::move(fields)};
  route_type_.name("bgpdump::routing");

  std::vector<type::record::field> withdraw_fields;
  withdraw_fields.emplace_back("timestamp", type::time_point{});
  withdraw_fields.emplace_back("source_ip", type::address{});
  withdraw_fields.emplace_back("source_as", type::count{});
  withdraw_fields.emplace_back("prefix", type::subnet{});
  withdraw_type_ = type::record{std::move(withdraw_fields)};
  withdraw_type_.name("bgpdump::withdrawn");

  std::vector<type::record::field> state_change_fields;
  state_change_fields.emplace_back("timestamp", type::time_point{});
  state_change_fields.emplace_back("source_ip", type::address{});
  state_change_fields.emplace_back("source_as", type::count{});
  state_change_fields.emplace_back("old_state", type::string{});
  state_change_fields.emplace_back("new_state", type::string{});
  state_change_type_ = type::record{std::move(state_change_fields)};
  state_change_type_.name("bgpdump::state_change");
}

schema bgpdump::sniff()
{
  schema sch;
  sch.add(announce_type_);
  sch.add(route_type_);
  sch.add(withdraw_type_);
  sch.add(state_change_type_);
  return sch;
}

void bgpdump::set(schema const& sch)
{
  if (auto t = sch.find_type(announce_type_.name()))
  {
    if (congruent(*t, announce_type_))
    {
      VAST_VERBOSE("prefers type in schema over default type:", *t);
      announce_type_ = *t;
    }
    else
    {
      VAST_WARN("ignores incongruent schema type:", t->name());
    }
  }
  if (auto t = sch.find_type(route_type_.name()))
  {
    if (congruent(*t, route_type_))
    {
      VAST_VERBOSE("prefers type in schema over default type:", *t);
      route_type_ = *t;
    }
    else
    {
      VAST_WARN("ignores incongruent schema type:", t->name());
    }
  }
  if (auto t = sch.find_type(withdraw_type_.name()))
  {
    if (congruent(*t, withdraw_type_))
    {
      VAST_VERBOSE("prefers type in schema over default type:", *t);
      withdraw_type_ = *t;
    }
    else
    {
      VAST_WARN("ignores incongruent schema type:", t->name());
    }
  }
  if (auto t = sch.find_type(state_change_type_.name()))
  {
    if (congruent(*t, state_change_type_))
    {
      VAST_VERBOSE("prefers type in schema over default type:", *t);
      state_change_type_ = *t;
    }
    else
    {
      VAST_WARN("ignores incongruent schema type:", t->name());
    }
  }
}

result<event> bgpdump::extract()
{
  using namespace parsers;
  static auto str = +(any - '|');
  static auto ts =
    u64 ->* [](count x) { return time::point{time::seconds{x}}; };
  static auto head
    = "BGP4MP|" >> ts >> '|' >> str >> '|' >> addr >> '|' >> u64 >> '|';
  if (! next_line())
    return {};
  time::point timestamp;
  std::string update;
  vast::address source_ip;
  count source_as;
  auto tuple = std::tie(timestamp, update, source_ip, source_as);
  auto f = this->line().begin();
  auto l = this->line().end();
  if (! head.parse(f, l, tuple))
    return {};
  record r;
  r.emplace_back(timestamp);
  r.emplace_back(std::move(source_ip));
  r.emplace_back(source_as);
  if (update == "A" || update == "B")
  {
    // Announcement or routing table entry
    static auto num = u64 ->* [](count x) { return data{x}; };
    static auto tail
      =   net
      >> '|'
      >>  (num % ' ') >> -(" {" >> u64 >> '}')
      >> '|'
      >> str
      >> '|'
      >> addr
      >> '|'
      >> u64
      >> '|'
      >> u64
      >> '|'
      >> -str
      >> '|'
      >> -str
      >> '|'
      >> -str
      ;
    subnet sn;
    std::vector<data> as_path;
    optional<count> origin_as;
    std::string origin;
    vast::address nexthop;
    count local_pref;
    count med;
    optional<std::string> community;
    optional<std::string> atomic_aggregate;
    optional<std::string> aggregator;
    auto t = std::tie(sn, as_path, origin_as, origin, nexthop, local_pref, med,
                      community, atomic_aggregate, aggregator);
    if (! tail.parse(f, l, t))
      return {};
    r.emplace_back(std::move(sn));
    r.emplace_back(vector(std::move(as_path)));
    r.emplace_back(std::move(origin_as));
    r.emplace_back(std::move(origin));
    r.emplace_back(nexthop);
    r.emplace_back(local_pref);
    r.emplace_back(med);
    r.emplace_back(std::move(community));
    r.emplace_back(std::move(atomic_aggregate));
    r.emplace_back(std::move(aggregator));
    event e{{std::move(r), update == "A" ? announce_type_ : route_type_}};
    e.timestamp(timestamp);
    return e;
  }
  else if (update == "W")
  {
    subnet sn;
    if (! net.parse(f, l, sn))
      return {};
    r.emplace_back(sn);
    event e{{std::move(r), withdraw_type_}};
    e.timestamp(timestamp);
    return e;
  }
  else if (update == "STATE")
  {
    static auto tail = -str >> '|' >> -str;
    optional<std::string> old_state;
    optional<std::string> new_state;
    auto t = std::tie(old_state, new_state);
    if (! tail.parse(f, l, t))
      return {};
    r.emplace_back(std::move(old_state));
    r.emplace_back(std::move(new_state));
    event e{{std::move(r), state_change_type_}};
    e.timestamp(timestamp);
    return e;
  }
  return {};
}

} // namespace source
} // namespace vast
