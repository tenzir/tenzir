#include "vast/actor/source/bgpdump.h"

#include <cassert>
#include "vast/util/string.h"

namespace vast {
namespace source {

bgpdump::bgpdump(std::string const& filename)
  : file<bgpdump>{"bgpdump-source", filename}
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

namespace {

template <typename Iterator>
trial<void> parse_origin_as(count& origin_as, vast::vector& as_path,
                            Iterator& begin, Iterator end)
{
  if (begin == end)
    return error{"empty as_path"};
  auto s = util::split(begin, end, " ", "");
  if (s.size() <= 0)
    return error{"empty as_path"};

  for (size_t i = 0; i < s.size() - 1; ++i)
  {
    count asn;
    auto t = parse(asn, s[i].first, s[i].second);
    if (t)
      as_path.push_back(std::move(asn));
    else
      return t.error();
  }
  auto last_first = s[s.size() - 1].first;
  auto last_second = s[s.size() - 1].second;

  if (util::starts_with(last_first, last_second, "{"))
    ++last_first;
  if (util::ends_with(last_first, last_second, "}"))
    --last_second;

  auto t = parse(origin_as, last_first, last_second);
  if (! t)
    return t.error();
  as_path.push_back(std::move(origin_as));
  return nothing;
}

} // namespace

result<event> bgpdump::extract()
{
  if (! next_line())
    return {};

  auto elems = util::split(this->line(), separator_);
  if (elems.size() < 5)
    return {};

  time::point timestamp;
  auto t = parse(timestamp, elems[1].first, elems[1].second);
  if (! t)
    return {};

  std::string update; // A,W,STATE,...
  t = parse(update, elems[2].first, elems[2].second);
  if (! t)
    return {};

  vast::address source_ip;
  t = parse(source_ip, elems[3].first, elems[3].second);
  if (! t)
    return {};

  count source_as;
  t = parse(source_as, elems[4].first, elems[4].second);
  if (! t)
    return {};

  record r;
  r.emplace_back(std::move(timestamp));
  r.emplace_back(std::move(source_ip));
  r.emplace_back(std::move(source_as));

  if ((update == "A" || update == "B") && elems.size() >= 14)
  {
    // announcement or routing table entry
    subnet prefix;
    t = parse(prefix, elems[5].first, elems[5].second);
    if (! t)
      return {};

    vast::vector as_path;
    count origin_as = 0;
    t = parse_origin_as(origin_as, as_path, elems[6].first, elems[6].second);
    if (! t)
      return {};

    std::string origin;
    t = parse(origin, elems[7].first, elems[7].second);
    if (! t)
      return {};

    vast::address nexthop;
    t = parse(nexthop, elems[8].first, elems[8].second);
    if (! t)
      return {};

    count local_pref;
    t = parse(local_pref, elems[9].first, elems[9].second);
    if (! t)
      return {};

    count med;
    t = parse(med, elems[10].first, elems[10].second);
    if (! t)
      return {};

    std::string community;
    if (elems[11].first != elems[11].second)
    {
      t = parse(community, elems[11].first, elems[11].second);
      if (! t)
        return {};
    }

    std::string atomic_aggregate;
    if (elems[12].first != elems[12].second)
    {
      t = parse(atomic_aggregate, elems[12].first, elems[12].second);
      if (! t)
        return {};
    }

    std::string aggregator;
    if (elems[13].first != elems[13].second)
    {
      t = parse(aggregator, elems[13].first, elems[13].second);
      if (! t)
        return {};
    }

    r.emplace_back(std::move(prefix));
    r.emplace_back(std::move(as_path));
    r.emplace_back(std::move(origin_as));
    r.emplace_back(std::move(origin));
    r.emplace_back(std::move(nexthop));
    r.emplace_back(std::move(local_pref));
    r.emplace_back(std::move(med));
    r.emplace_back(std::move(community));
    r.emplace_back(std::move(atomic_aggregate));
    r.emplace_back(std::move(aggregator));
    event e{{std::move(r), update == "A" ? announce_type_ : route_type_}};
    e.timestamp(timestamp);
    return std::move(e);
  }
  else if (update == "W" && elems.size() >= 6) // withdraw
  {
    subnet prefix;
    t = parse(prefix, elems[5].first, elems[5].second);
    if (! t)
      return {};

    r.emplace_back(std::move(prefix));
    event e{{std::move(r), withdraw_type_}};
    e.timestamp(timestamp);
    return std::move(e);
  }
  else if (update == "STATE" && elems.size() >= 7) // state change
  {
    std::string old_state;
    t = parse(old_state, elems[5].first, elems[5].second);
    if (! t)
      return {};

    std::string new_state;
    t = parse(new_state, elems[6].first, elems[6].second);
    if (! t)
      return {};

    r.emplace_back(std::move(old_state));
    r.emplace_back(std::move(new_state));
    event e{{std::move(r), state_change_type_}};
    e.timestamp(timestamp);
    return std::move(e);
  }

  return {};
}

} // namespace source
} // namespace vast
