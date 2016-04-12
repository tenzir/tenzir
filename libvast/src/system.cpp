#include <caf/scheduler/profiled_coordinator.hpp>

//#include "vast/address.hpp"
//#include "vast/add_message_type.hpp"
//#include "vast/bitmap_index_polymorphic.hpp"
//#include "vast/bitstream.hpp"
//#include "vast/caf.hpp"
#include "vast/chunk.hpp"
//#include "vast/expression.hpp"
//#include "vast/event.hpp"
#include "vast/filesystem.hpp"
//#include "vast/key.hpp"
//#include "vast/offset.hpp"
#include "vast/operator.hpp"
//#include "vast/pattern.hpp"
#include "vast/query_options.hpp"
//#include "vast/time.hpp"
//#include "vast/type.hpp"
#include "vast/uuid.hpp"
//#include "vast/value.hpp"
//#include "vast/util/radix_tree.hpp"
//#include "vast/actor/accountant.hpp"
//#include "vast/actor/archive.hpp"
//#include "vast/actor/identifier.hpp"
#include "vast/concept/serializable/state.hpp"
#include "vast/concept/serializable/std/chrono.hpp"
//#include "vast/concept/serializable/vast/bitstream_polymorphic.hpp"
//#include "vast/concept/serializable/vast/bitmap_index_polymorphic.hpp"
//#include "vast/concept/serializable/vast/data.hpp"
//#include "vast/concept/serializable/vast/expression.hpp"
#include "vast/concept/serializable/vast/schema.hpp"
//#include "vast/concept/serializable/vast/type.hpp"
//#include "vast/concept/serializable/vast/vector_event.hpp"
//#include "vast/concept/serializable/vast/util/radix_tree.hpp"
//#include "vast/concept/state/address.hpp"
//#include "vast/concept/state/bitmap_index.hpp"
#include "vast/concept/state/chunk.hpp"
//#include "vast/concept/state/error.hpp"
//#include "vast/concept/state/event.hpp"
#include "vast/concept/state/filesystem.hpp"
//#include "vast/concept/state/none.hpp"
//#include "vast/concept/state/pattern.hpp"
#include "vast/concept/state/time.hpp"
//#include "vast/concept/state/type.hpp"
#include "vast/concept/state/uuid.hpp"
//#include "vast/concept/state/value.hpp"
//#include "vast/io/compression.hpp"

using namespace caf;

namespace vast {

namespace {

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

} // namespace <anonymous>

void augment(actor_system_config& cfg) {
  cfg.add_message_type<path>("vast::path")
     .add_message_type<uuid>("vast::uuid")
     .add_message_type<arithmetic_operator>("vast::arithmetic_operator")
     .add_message_type<relational_operator>("vast::relational_operator")
     .add_message_type<boolean_operator>("vast::boolean_operator")
     .add_message_type<query_options>("vast::query_options")
     .add_message_type<chunk>("vast::chunk")
//     .add_message_type<schema>("vast::schema")
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
  //add_message_type<std::vector<event>>("std::vector<vast::event>");
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
    ;
}

actor_system_config make_config() {
  actor_system_config cfg;
  augment(cfg);
  return cfg;
}

actor_system_config make_config(int argc, char** argv) {
  actor_system_config cfg{argc, argv};
  augment(cfg);
  return cfg;
}

} // namespace vast
