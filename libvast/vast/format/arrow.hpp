#ifndef VAST_FORMAT_ARROW_HPP

#define VAST_FORMAT_ARROW_HPP

#include <string>

#include "plasma/client.h"
#include "plasma/common.h"

#include "arrow/api.h"
#include "arrow/builder.h"

#include "vast/data.hpp"
#include "vast/expected.hpp"
#include "vast/type.hpp"

namespace vast {

class event;

namespace format {
namespace arrow {

/// Converts events into Arrow Record batches and writes them into a Plasma
/// store.

class writer {
public:
  writer() = default;

  /// Constructs an Arrow writer that connects to a (local) plasma store.
  /// @param plasma_socket The path to the local Plasma listening socket.
  writer(const std::string& plasma_socket);

  ~writer();

  expected<void> write(const std::vector<event>& e);

  expected<void> write(const std::vector<event>& e,
                       std::vector<plasma::ObjectID>& oids);

  expected<void> write(const event& x);

  expected<void> flush();

  const char* name() const;

  /// Checks whether the writer is connected to the Plasma store.
  /// @returns `true` if the connection to the Plasma store is alive.
  bool connected() const;

private:
  expected<plasma::ObjectID> make_object(const void* data, size_t size);

  bool connected_;

  plasma::PlasmaClient plasma_client_;

  std::vector<event> buffer_;
};
struct convert_visitor {
  using result_type = std::shared_ptr<::arrow::Field>;
  
  result_type operator()(const boolean_type&);
  
  result_type operator()(const count_type&);
  
  result_type operator()(const integer_type&);
  
  result_type operator()(const real_type&);
  
  result_type operator()(const string_type&);
  
  result_type operator()(const pattern_type&);
  
  result_type operator()(const address_type&);
  
  result_type operator()(const port_type&);
  
  result_type operator()(const subnet_type&);
  
  result_type operator()(const timespan_type&);
  
  result_type operator()(const timestamp_type&);
  
  result_type operator()(const vector_type& t);
  
  result_type operator()(const set_type& t);
  
  template <class T>
  result_type operator()(const T& t) {
    std::cout << "NONE: " << typeid(t).name() << std::endl;
    return ::arrow::field("none", ::arrow::null());
  }
};

struct insert_visitor {
  std::shared_ptr<::arrow::RecordBatchBuilder> rbuilder;
  
  u_int64_t counter = 0;
  
  u_int64_t offset = 0;
  
  u_int64_t c_builder = 0;
  
  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b);
  
  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b, u_int64_t c);
  
  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b, u_int64_t c,
                 u_int64_t c_builder);
  
  ::arrow::Status operator()(const record_type& t, const std::vector<data>& d);
  
  ::arrow::Status operator()(const count_type& t, const count& d);
  
  ::arrow::Status operator()(const integer_type& t, const integer& d);
  
  ::arrow::Status operator()(const real_type& t, const real& d);
  
  ::arrow::Status operator()(const type& t, const data& d);
  
  ::arrow::Status operator()(const string_type& t, const std::string& d);
  
  ::arrow::Status operator()(const boolean_type& t, const bool& d);
  
  ::arrow::Status operator()(const timestamp_type& t, const timestamp& d);
  
  ::arrow::Status operator()(const timespan_type& t, const timespan& d);
  
  ::arrow::Status operator()(const subnet_type& t, const subnet& d);
  
  ::arrow::Status operator()(const address_type& t, const address& d);
  
  ::arrow::Status operator()(const port_type& t, const port& d);
  
  ::arrow::Status operator()(const vector_type& t, const std::vector<data>& d);
  
  ::arrow::Status operator()(const set_type& t, const set& d);
  
  // none data -> AppendNull
  ::arrow::Status operator()(const none_type&, const none&);
  
  ::arrow::Status operator()(const count_type& t, const none&);
  
  ::arrow::Status operator()(const integer_type& t, const none& d);
  
  ::arrow::Status operator()(const real_type& t, const none&);
  
  ::arrow::Status operator()(const string_type& t, const none&);
  
  ::arrow::Status operator()(const boolean_type& t, const none&);
  
  ::arrow::Status operator()(const timespan_type& t, const none&);
  
  ::arrow::Status operator()(const address_type& t, const none&);
  
  ::arrow::Status operator()(const port_type& t, const none&);
  
  ::arrow::Status operator()(const vector_type& t, const none&);
  
  ::arrow::Status operator()(const set_type& t, const none&);

  template <class T1, class T2>
  ::arrow::Status operator()(const T1, const T2) {
    std::cout << typeid(T1).name() << " nop " << typeid(T2).name() << std::endl;
    return ::arrow::Status::OK();
  };
};

struct insert_visitor_helper {
  using result_type = ::arrow::Status;
  
  ::arrow::ArrayBuilder* builder;
  
  insert_visitor_helper(::arrow::ArrayBuilder* b);
  
  result_type operator()(const boolean_type&, const boolean& d);
  
  result_type operator()(const count_type&, const count& d);
  
  result_type operator()(const integer_type&, const int& d);
  
  result_type operator()(const real_type&, const real& d);
  
  result_type operator()(const string_type&, const std::string& d);
  
  result_type operator()(const pattern_type&, const pattern& d);
  
  result_type operator()(const address_type&, const address& d);
  
  result_type operator()(const port_type&, const port& d);
  
  result_type operator()(const subnet_type&, const subnet& d);
  
  result_type operator()(const timespan_type&, const timespan& d);
  
  result_type operator()(const timestamp_type&, const timestamp& d);
  
  result_type operator()(const none_type&, const none&);
  
  template <class T1, class T2>
  result_type operator()(const T1& t1, const T2& t2) {
    std::cout << "Type: " << typeid(t1).name() 
      << ", Data: " << typeid(t2).name() << std::endl;
    return result_type::OK();
  }
};
} // namespace arrow
} // namespace format
} // namespace vast

#endif
