#include <map>
#include <vector>

#include <caf/io/middleman.hpp>

#include "vast/chunk.hpp"
#include "vast/compression.hpp"
#include "vast/configuration.hpp"
#include "vast/expression.hpp"
#include "vast/data.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"
#include "vast/key.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/pattern.hpp"
#include "vast/query_options.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"
#include "vast/value.hpp"
//#include "vast/actor/accountant.hpp"
//#include "vast/actor/archive.hpp"
//#include "vast/actor/identifier.hpp"

namespace vast {

//template <typename Bitstream>
//void announce_bmi_hierarchy(std::string const& bs_name) {
//  using detail::bitmap_index_concept;
//  using detail::bitmap_index_model;
//  static auto wrap =
//    [](std::string const& prefix, std::string const& str, std::string const& t)
//      -> std::string {
//    return prefix + '<' + str + ">,T=" + t;
//  };
//  static auto model_wrap = [](std::string const& str, std::string const& t)
//    -> std::string {
//    return wrap("vast::detail::bitmap_index_model", str, t);
//  };
//  add_message_type<arithmetic_bitmap_index<Bitstream, boolean>>(
//    wrap("vast::arithmetic_bitmap_index", "T,boolean", bs_name));
//  add_message_type<arithmetic_bitmap_index<Bitstream, integer>>(
//    wrap("vast::arithmetic_bitmap_index", "T,integer", bs_name));
//  add_message_type<arithmetic_bitmap_index<Bitstream, count>>(
//    wrap("vast::arithmetic_bitmap_index", "T,count", bs_name));
//  add_message_type<arithmetic_bitmap_index<Bitstream, real>>(
//    wrap("vast::arithmetic_bitmap_index", "T,real", bs_name));
//  add_message_type<arithmetic_bitmap_index<Bitstream, time::point>>(
//    wrap("vast::arithmetic_bitmap_index", "T,time::point", bs_name));
//  add_message_type<arithmetic_bitmap_index<Bitstream, time::duration>>(
//    wrap("vast::arithmetic_bitmap_index", "T,time::duration", bs_name));
//  add_message_type<address_bitmap_index<Bitstream>>(
//    wrap("vast::address_bitmap_index", "T", bs_name));
//  add_message_type<subnet_bitmap_index<Bitstream>>(
//    wrap("vast::subnet_bitmap_index", "T", bs_name));
//  add_message_type<port_bitmap_index<Bitstream>>(
//    wrap("vast::port_bitmap_index", "T", bs_name));
//  add_message_type<string_bitmap_index<Bitstream>>(
//    wrap("vast::string_bitmap_index", "T", bs_name));
//  add_message_type<sequence_bitmap_index<Bitstream>>(
//    wrap("vast::sequence_bitmap_index", "T", bs_name));
//  add_type
//    bitmap_index_model<arithmetic_bitmap_index<Bitstream, boolean>>,
//    bitmap_index_model<arithmetic_bitmap_index<Bitstream, integer>>,
//    bitmap_index_model<arithmetic_bitmap_index<Bitstream, count>>,
//    bitmap_index_model<arithmetic_bitmap_index<Bitstream, real>>,
//    bitmap_index_model<arithmetic_bitmap_index<Bitstream, time::point>>,
//    bitmap_index_model<arithmetic_bitmap_index<Bitstream, time::duration>>,
//    bitmap_index_model<address_bitmap_index<Bitstream>>,
//    bitmap_index_model<subnet_bitmap_index<Bitstream>>,
//    bitmap_index_model<port_bitmap_index<Bitstream>>,
//    bitmap_index_model<string_bitmap_index<Bitstream>>,
//    bitmap_index_model<sequence_bitmap_index<Bitstream>>
//  >(model_wrap("arithmetic_bitmap_index<T,boolean>", bs_name),
//    model_wrap("arithmetic_bitmap_index<T,integer>", bs_name),
//    model_wrap("arithmetic_bitmap_index<T,count>", bs_name),
//    model_wrap("arithmetic_bitmap_index<T,real>", bs_name),
//    model_wrap("arithmetic_bitmap_index<T,time::point>", bs_name),
//    model_wrap("arithmetic_bitmap_index<T,time::duration>", bs_name),
//    model_wrap("address_bitmap_index<T>", bs_name),
//    model_wrap("subnet_bitmap_index<T>", bs_name),
//    model_wrap("port_bitmap_index<T>", bs_name),
//    model_wrap("string_bitmap_index<T>", bs_name),
//    model_wrap("sequence_bitmap_index<T>", bs_name)
//  );
//}

// TODO: port these types
//add_message_type<schema>("vast::schema")
//add_message_type<time::point>("vast::time::point");
//add_message_type<time::duration>("vast::time::duration");
//add_message_type<time::moment>("vast::time::moment");
//add_message_type<time::extent>("vast::time::extent");
//add_message_type<pattern>("vast::pattern");
//add_message_type<vector>("vast::vector");
//add_message_type<set>("vast::set");
//add_message_type<table>("vast::table");
//add_message_type<record>("vast::record");
//add_message_type<address>("vast::address");
//add_message_type<subnet>("vast::subnet");
//add_message_type<port>("vast::port");
//add_message_type<data>("vast::data");
//add_message_type<type>("vast::type");
//add_message_type<key>("vast::key");
//add_message_type<offset>("vast::offset");
//add_message_type<value>("vast::value");
//add_message_type<event>("vast::event");
//add_message_type<expression>("vast::expression");
//add_message_type<predicate>("vast::predicate");
//add_message_type<io::compression>("vast::io::compression");
//add_message_type<none>("vast::none");
//add_message_type<error>("vast::error");
//// std::vector<T>
//add_message_type<std::vector<data>>("std::vector<vast::data>");
//add_message_type<std::vector<value>>("std::vector<vast::value>");
//add_message_type<std::vector<uuid>>("std::vector<vast::uuid>");
//add_message_type<util::radix_tree<message>>(
//  "vast::util::radix_tree<caf::message>>");
//// Polymorphic bitstreams
//add_message_type<ewah_bitstream>("vast::ewah_bitstream");
//add_message_type<null_bitstream>("vast::null_bitstream");
//announce_hierarchy<
//  detail::bitstream_concept,
//  detail::bitstream_model<null_bitstream>,
//  detail::bitstream_model<ewah_bitstream>
//>("vast::detail::bitstream_model<vast::null_bitstream>",
//  "vast::detail::bitstream_model<vast::ewah_bitstream>"
//);
////// Polymorphic bitmap indexes.
//announce_bmi_hierarchy<ewah_bitstream>("ewah_bitstream");
//announce_bmi_hierarchy<null_bitstream>("null_bitstream");
//// CAF only
//caf::add_message_type<std::map<std::string, message>>(
//  "std::map<std::string,caf::message>>");
//// Temporary workaround.
//caf::add_message_type(typeid(util::radix_tree<message>),
//              std::make_unique<radix_tree_msg_type_info>());
//// Actors
//caf::add_message_type<accountant::type>("vast::accountant");
//caf::add_message_type<archive::type>("vast::archive");
//caf::add_message_type<identifier::type>("vast::identifier");

// -- Optimized (de)serialization for event batches ---------------------------

void serialize(caf::serializer& sink, std::vector<event> const& events) {
  util::flat_set<type::hash_type::result_type> digests;
  auto size = events.size();
  sink.begin_sequence(size);
  for (auto& e : events) {
    auto digest = e.type().digest();
    sink << digest;
    if (digests.count(digest) == 0) {
      digests.insert(digest);
      sink << e.type();
    }
    sink << e.data() << e.id() << e.timestamp();
  }
  sink.end_sequence();
}

void serialize(caf::deserializer& source, std::vector<event>& events) {
  std::map<type::hash_type::result_type, type> types;
  size_t size;
  source.begin_sequence(size);
  events.resize(size);
  for (auto& e : events) {
    type::hash_type::result_type digest;
    source >> digest;
    auto i = types.find(digest);
    if (i == types.end()) {
      type t;
      source >> t;
      VAST_ASSERT(digest == t.digest());
      i = types.emplace(digest, std::move(t)).first;
    }
    data d;
    event_id id;
    time::point ts;
    source >> d >> id >> ts;
    e = value{std::move(d), i->second};
    e.id(id);
    e.timestamp(ts);
  }
  source.end_sequence();
}

configuration::configuration() {
  // Register VAST's custom types.
  add_message_type<arithmetic_operator>("vast::arithmetic_operator");
  add_message_type<boolean_operator>("vast::boolean_operator");
  //add_message_type<chunk>("vast::chunk");
  add_message_type<data>("vast::data");
  add_message_type<event>("vast::event");
  add_message_type<path>("vast::path");
  add_message_type<query_options>("vast::query_options");
  add_message_type<relational_operator>("vast::relational_operator");
  add_message_type<type>("vast::type");
  add_message_type<uuid>("vast::uuid");
  add_message_type<value>("vast::value");
  add_message_type<std::vector<event>>("std::vector<vast::event>");
  // Register VAST's custom error type.
  auto renderer = [](uint8_t x, caf::atom_value, const caf::message&) {
    return "VAST error:" + caf::deep_to_string_as_tuple(static_cast<ec>(x));
  };
  add_error_category(caf::atom("vast"), renderer);
  // Load modules.
  load<caf::io::middleman>();
}

configuration::configuration(int argc, char** argv) : configuration{} {
  parse(argc, argv);
}

} // namespace vast
