#include "vast/source/bgpdump.h"

#include <cassert>
#include "vast/util/string.h"

#include <boost/regex.hpp>

namespace vast {
namespace source {

namespace {

//trial<type> make_type(std::string const& bro_type)
//{
//  type t;
//  if (bro_type == "enum" || bro_type == "string" || bro_type == "file")
//    t = type::string{};
//  else if (bro_type == "bool")
//    t = type::boolean{};
//  else if (bro_type == "int")
//    t = type::integer{};
//  else if (bro_type == "count")
//    t = type::count{};
//  else if (bro_type == "double")
//    t = type::real{};
//  else if (bro_type == "time")
//    t = type::time_point{};
//  else if (bro_type == "interval")
//    t = type::time_duration{};
//  else if (bro_type == "pattern")
//    t = type::pattern{};
//  else if (bro_type == "addr")
//    t = type::address{};
//  else if (bro_type == "subnet")
//    t = type::subnet{};
//  else if (bro_type == "port")
//    t = type::port{};
//
//  if (is<none>(t)
//      && (util::starts_with(bro_type, "vector")
//          || util::starts_with(bro_type, "set")
//          || util::starts_with(bro_type, "table")))
//  {
//    // Bro's logging framwork cannot log nested vectors/sets/tables, so we can
//    // safely assume that we're dealing with a basic type inside the brackets.
//    // If this will ever change, we'll have to enhance this simple parser.
//    auto open = bro_type.find("[");
//    auto close = bro_type.rfind("]");
//    if (open == std::string::npos || close == std::string::npos)
//      return error{"missing delimiting container brackets: ", bro_type};
//
//    auto elem = make_type(bro_type.substr(open + 1, close - open - 1));
//    if (! elem)
//      return elem.error();
//
//    // Bro sometimes logs sets as tables, e.g., represents set[string] as
//    // table[string]. We iron out this inconsistency by normalizing the type to
//    // a set.
//    if (util::starts_with(bro_type, "vector"))
//      t = type::vector{*elem};
//    else
//      t = type::set{*elem};
//  }
//
//  if (is<none>(t))
//    return error{"failed to make type for: ", bro_type};
//
//  return t;
//}

} // namespace <anonymous>

const std::string re_IPv4("(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");//"\\d+\\.\\d+\\.\\d+\\.\\d+"
const std::string re_IPv6("([0-9a-fA-F]{0,4}(:[0-9a-fA-F]{0,4})+)");//qr/(((?=.*(::))(?!.*\3.+\3))\3?|([\dA-F]{1,4}(\3|:\b|$)|\2))(?4){5}((?4){2}|(((2[0-4]|1\d|[1-9])?\d|25[0-5])\.?\b){4})\z/ai;#'(\dabcdef)+';
const std::string re_IP("(("+re_IPv4+")|("+re_IPv6+"))");
const std::string re_Prefix("("+re_IP+"\\/\\d+)");
const std::string re_ASN("[1-9][0-9]*");

bgpdump::bgpdump(schema sch, std::string const& filename, bool sniff)
  : file<bgpdump>{filename},
    schema_{std::move(sch)},
    sniff_{sniff}
{
}

result<event> bgpdump::extract_impl()
{
  //if (is<none>(type_))
  //{
  //  auto line = this->next();
  //  if (! line)
  //    return error{"could not read first line of header"};
  //
  //  if (sniff_)
  //  {
  //    schema sch;
  //    sch.add(type_);
  //    std::cout << sch << std::flush;
  //    halt();
  //    return {};
  //  }
  //}

  auto line = this->next();
  if (! line)
    return {};

  //auto elems = util::split(*line, separator_);
  std::vector<std::string> elems = split(*line,separator_[0]);
  
  record event_record;
  record* r = &event_record;
  auto ts = now();
  
  
  if(elems.size() >= 6){
      std::string time = elems[1];
      std::string update = elems[2];// A,W,STATE,...
      std::string sourceip = elems[3];
      std::string sourceasn = elems[4];
      std::string prefix = elems[5];
      
      if(((update.compare("A") == 0) or (update.compare("B") == 0)) and elems.size() >= 12){ //Announcement or Routing table entry
        std::string aspath = elems[6];
        std::string origin = elems[7];
        std::string nexthop = elems[8];
        
        std::string localPref = elems[9];
        std::string med = elems[10];
        std::string community = "";
        if(elems.size() >= 13){
          community = elems[11];
        }
        std::string atomic_aggregate = elems[11];
        if(community.compare("") != 0){
          std::string atomic_aggregate = elems[12];
        }else{
        
        }
        std::string aggregator = "";
        if(elems.size() >= 14){
          aggregator = elems[13];
        }
        
        std::vector<std::string> asns = split(aspath,' ');
        std::string originas = asns[asns.size()-1];
        
        originas = boost::regex_replace(originas, boost::regex("\\{"), std::string(""));
        originas = boost::regex_replace(originas, boost::regex("\\}"), std::string(""));
        
        if(boost::regex_match(prefix, boost::regex(re_Prefix)) and
          boost::regex_match(originas, boost::regex(re_ASN)) and
          boost::regex_match(sourceip, boost::regex(re_IP)) and
          boost::regex_match(sourceasn, boost::regex(re_ASN)) and
          boost::regex_match(nexthop, boost::regex(re_IP)) 
          ){
          
		  
          std::vector<type::record::field> fields;
          fields.emplace_back("time", type::time_point{});
	  fields.emplace_back("update", type::string{});
	  fields.emplace_back("sourceip", type::address{});
	  fields.emplace_back("sourceasn", type::integer{});
	  fields.emplace_back("prefix", type::string{});
	  fields.emplace_back("aspath", type::string{});
	  fields.emplace_back("originas", type::integer{});
	  fields.emplace_back("origin", type::string{});
	  fields.emplace_back("nexthop", type::address{});
	  fields.emplace_back("localPref", type::integer{});
	  fields.emplace_back("med", type::integer{});
	  fields.emplace_back("community", type::string{});
	  fields.emplace_back("atomic_aggregate", type::string{});
	  fields.emplace_back("aggregator", type::string{});
          type::record flat{std::move(fields)};
          type_ = flat.unflatten();
          type_.name("Announcement");
		  
		  
          r->push_back(time);
          r->push_back(update);
          r->push_back(sourceip);
          r->push_back(sourceasn);
          r->push_back(prefix);
          r->push_back(aspath);
          r->push_back(originas);
          r->push_back(origin);
          r->push_back(nexthop);
          r->push_back(localPref);
          r->push_back(med);
          r->push_back(community);
          r->push_back(atomic_aggregate);
          r->push_back(aggregator);
          
        }else{
          //format error
          
        }
      }else if(update.compare("W") == 0){ //Withdrawn
        if(boost::regex_match(prefix, boost::regex(re_Prefix)) and
          boost::regex_match(sourceip, boost::regex(re_IP)) and
          boost::regex_match(sourceasn, boost::regex(re_ASN))  ){
          
	  std::vector<type::record::field> fields;
          fields.emplace_back("time", type::time_point{});
	  fields.emplace_back("update", type::string{});
	  fields.emplace_back("sourceip", type::address{});
	  fields.emplace_back("sourceasn", type::integer{});
	  fields.emplace_back("prefix", type::string{});
          type::record flat{std::move(fields)};
          type_ = flat.unflatten();
          type_.name("Withdrawn");

	  r->push_back(time);
	  r->push_back(update);
	  r->push_back(sourceip);
	  r->push_back(sourceasn);
	  r->push_back(prefix);
          
        }else{
          //format error
          
        }
      }else if(update.compare("STATE") == 0 and elems.size() >= 7){ //state change
        //std::string oldState = prefix;
        //std::string newState = elems[6];
        
      }else{
        //unknown type
        
      }
    }else{
      //format error
      
    }
  
  

  event e{{std::move(event_record), type_}};
  e.timestamp(ts);

  return std::move(e);
}

std::string bgpdump::describe() const
{
  return "bgpdump-source";
}

std::vector<std::string> bgpdump::split(const std::string &s, char delim)
{
    std::vector<std::string> elems;
    std::string item;

    // use stdlib to tokenize the string
    std::stringstream ss(s);
    while (std::getline(ss, item, delim))
        if(!item.empty())
            elems.push_back(item);

    return elems;
}



} // namespace source
} // namespace vast

