#include "vast/source/bgpdump.h"

#include <cassert>
#include "vast/util/string.h"

namespace vast {
namespace source {

bgpdump::bgpdump(schema sch, std::string const& filename, bool sniff)
  : file<bgpdump>{filename},
    schema_{std::move(sch)},
    sniff_{sniff}
{
  std::vector<type::record::field> announce_fields;
  announce_fields.emplace_back("timestamp", type::time_point{});
  announce_fields.emplace_back("source_ip", type::address{});
  announce_fields.emplace_back("source_as", type::count{});
  announce_fields.emplace_back("prefix", type::subnet{});
  announce_fields.emplace_back("as_path", type::vector{type::count{}});
  //announce_fields.emplace_back("as_path", type::string{});
  announce_fields.emplace_back("origin_as", type::count{});
  announce_fields.emplace_back("origin", type::string{});
  announce_fields.emplace_back("nexthop", type::address{});
  announce_fields.emplace_back("local_pref", type::count{});
  announce_fields.emplace_back("med", type::count{});
  announce_fields.emplace_back("community", type::string{});
  announce_fields.emplace_back("atomic_aggregate", type::string{});
  announce_fields.emplace_back("aggregator", type::string{});
  type::record announce_flat{std::move(announce_fields)};
  announce_type_ = announce_flat;
  announce_type_.name("announcement");
  
  std::vector<type::record::field> route_fields;
  route_fields.emplace_back("timestamp", type::time_point{});
  route_fields.emplace_back("source_ip", type::address{});
  route_fields.emplace_back("source_as", type::count{});
  route_fields.emplace_back("prefix", type::subnet{});
  route_fields.emplace_back("as_path", type::vector{type::count{}});
  //route_fields.emplace_back("as_path", type::string{});
  route_fields.emplace_back("origin_as", type::count{});
  route_fields.emplace_back("origin", type::string{});
  route_fields.emplace_back("nexthop", type::address{});
  route_fields.emplace_back("local_pref", type::count{});
  route_fields.emplace_back("med", type::count{});
  route_fields.emplace_back("community", type::string{});
  route_fields.emplace_back("atomic_aggregate", type::string{});
  route_fields.emplace_back("aggregator", type::string{});
  type::record route_flat{std::move(route_fields)};
  route_type_ = route_flat;
  route_type_.name("routing");
  
  std::vector<type::record::field> withdraw_fields;
  withdraw_fields.emplace_back("timestamp", type::time_point{});
  withdraw_fields.emplace_back("source_ip", type::address{});
  withdraw_fields.emplace_back("source_as", type::count{});
  withdraw_fields.emplace_back("prefix", type::subnet{});
  type::record withdraw_flat{std::move(withdraw_fields)};
  withdraw_type_ = withdraw_flat;
  withdraw_type_.name("withdrawn");
  
  std::vector<type::record::field> state_change_fields;
  state_change_fields.emplace_back("timestamp", type::time_point{});
  state_change_fields.emplace_back("source_ip", type::address{});
  state_change_fields.emplace_back("source_as", type::count{});
  state_change_fields.emplace_back("old_state", type::string{});
  state_change_fields.emplace_back("new_state", type::string{});
  type::record state_change_flat{std::move(state_change_fields)};
  state_change_type_ = state_change_flat;
  state_change_type_.name("state_change");
  
  if (sniff_)
  {
    schema sch;
    sch.add(announce_type_);
    sch.add(route_type_);
    sch.add(withdraw_type_);
    sch.add(state_change_type_);
    std::cout << sch << std::flush;
    halt();
  }
}

result<event> bgpdump::extract_impl()
{
  auto line = this->next();
  if (! line)
    return {}; //error{"could not read line"};

  auto elems = util::split(*line, separator_);
  
  if (elems.size() >= 3)
  {
    time_point timestamp;
    auto t = parse(timestamp, elems[1].first, elems[1].second);
    if (! t)
      return t.error() + error{std::string{elems[1].first, elems[1].second}};

    std::string update;// A,W,STATE,...
    t = parse(update, elems[2].first, elems[2].second);
    if (! t)
      return t.error() + error{std::string{elems[2].first, elems[2].second}};

    if (((update.compare("A") == 0) || (update.compare("B") == 0)) 
	  && elems.size() >= 14) //announcement or routing table entry
	{

      vast::address source_ip;
      t = parse(source_ip, elems[3].first, elems[3].second);
      if (! t)
        return t.error() + error{std::string{elems[3].first, elems[3].second}};

      count source_as;
      t = parse(source_as, elems[4].first, elems[4].second);
      if (! t)
        return t.error() + error{std::string{elems[4].first, elems[4].second}};

      subnet prefix;
      t = parse(prefix, elems[5].first, elems[5].second);
      if (! t)
        return t.error() + error{std::string{elems[5].first, elems[5].second}};

	  //std::string as_path;
      //t = parse(as_path, elems[6].first, elems[6].second);
      vast::vector as_path;
      count origin_as = 0;
      t = parse_origin_as(origin_as, as_path, elems[6].first, elems[6].second);
      if (! t)
        return t.error() + error{std::string{elems[6].first, elems[6].second}};

	  std::string origin;
      t = parse(origin, elems[7].first, elems[7].second);
      if (! t)
        return t.error() + error{std::string{elems[7].first, elems[7].second}};

	  vast::address nexthop;
      t = parse(nexthop, elems[8].first, elems[8].second);
      if (! t)
        return t.error() + error{std::string{elems[8].first, elems[8].second}};

      count local_pref;
      t = parse(local_pref, elems[9].first, elems[9].second);
      if (! t)
        return t.error() + error{std::string{elems[9].first, elems[9].second}};

      count med;
      t = parse(med, elems[10].first, elems[10].second);
      if (! t)
        return t.error() + error{std::string{elems[10].first, 
          elems[10].second}};

      std::string community;
      if (elems[11].first == elems[11].second)
	  {
	    community = "";
	  }
      else
	  {
        t = parse(community, elems[11].first, elems[11].second);
	    if (! t)
          return t.error() + error{std::string{elems[11].first, 
            elems[11].second}};
      }

      std::string atomic_aggregate;
      if (elems[12].first == elems[12].second)
	  {
	    atomic_aggregate = "";
	  }
      else
	  {
        t = parse(atomic_aggregate, elems[12].first, elems[12].second);
        if (! t)
          return t.error() + error{std::string{elems[12].first, 
            elems[12].second}};
      }

      std::string aggregator;
      if (elems[13].first == elems[13].second)
	  {
	    aggregator = "";
	  }
      else
	  {
        t = parse(aggregator, elems[13].first, elems[13].second);
        if (! t)
          return t.error() + error{std::string{elems[13].first, 
            elems[13].second}};
      }

      record event_record;
	  event_record.emplace_back(std::move(timestamp));
      event_record.emplace_back(std::move(source_ip));
      event_record.emplace_back(std::move(source_as));
      event_record.emplace_back(std::move(prefix));
      event_record.emplace_back(std::move(as_path));
      event_record.emplace_back(std::move(origin_as));
      event_record.emplace_back(std::move(origin));
      event_record.emplace_back(std::move(nexthop));
      event_record.emplace_back(std::move(local_pref));
      event_record.emplace_back(std::move(med));
      event_record.emplace_back(std::move(community));
      event_record.emplace_back(std::move(atomic_aggregate));
      event_record.emplace_back(std::move(aggregator));
      type type_;
      if (update.compare("A") == 0)
        type_ = announce_type_;
      else
        type_ = route_type_;
      event e{{std::move(event_record), type_}};
      e.timestamp(timestamp);
      return std::move(e);

    }else if (update.compare("W") == 0 && elems.size() >= 6) //withdrawn
	{

      vast::address source_ip;
      t = parse(source_ip, elems[3].first, elems[3].second);
      if (! t)
        return t.error() + error{std::string{elems[3].first, elems[3].second}};

      count source_as;
      t = parse(source_as, elems[4].first, elems[4].second);
      if (! t)
        return t.error() + error{std::string{elems[4].first, elems[4].second}};

      subnet prefix;
      t = parse(prefix, elems[5].first, elems[5].second);
      if (! t)
        return t.error() + error{std::string{elems[5].first, elems[5].second}};

      record event_record;
	  event_record.emplace_back(std::move(timestamp));
      event_record.emplace_back(std::move(source_ip));
      event_record.emplace_back(std::move(source_as));
      event_record.emplace_back(std::move(prefix));
      event e{{std::move(event_record), withdraw_type_}};
      e.timestamp(timestamp);
      return std::move(e);

    }else if (update.compare("STATE") == 0 && elems.size() >= 7) //state change
	{
      vast::address source_ip;
      t = parse(source_ip, elems[3].first, elems[3].second);
      if (! t)
        return t.error() + error{std::string{elems[3].first, elems[3].second}};

      count source_as;
      t = parse(source_as, elems[4].first, elems[4].second);
      if (! t)
        return t.error() + error{std::string{elems[4].first, elems[4].second}};

      std::string old_state;
      t = parse(old_state, elems[5].first, elems[5].second);
      if (! t)
        return t.error() + error{std::string{elems[5].first, elems[5].second}};

      std::string new_state;
      t = parse(new_state, elems[6].first, elems[6].second);
      if (! t)
        return t.error() + error{std::string{elems[6].first, elems[6].second}};

      record event_record;
	  event_record.emplace_back(std::move(timestamp));
      event_record.emplace_back(std::move(source_ip));
      event_record.emplace_back(std::move(source_as));
      event_record.emplace_back(std::move(old_state));
      event_record.emplace_back(std::move(new_state));
      event e{{std::move(event_record), state_change_type_}};
      e.timestamp(timestamp);
      return std::move(e);
    }else
	{
      return error{"unknown type"};
    }
  }
  
  return {};
}

std::string bgpdump::describe() const
{
  return "bgpdump-source";
}

template <typename Iterator>
trial<void> bgpdump::parse_origin_as(count& origin_as, vast::vector& as_path, 
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




} // namespace source
} // namespace vast

