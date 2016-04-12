#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/http.hpp"
#include "vast/concept/printable/vast/uri.hpp"

#define SUITE printable
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(HTTP response) {
  http::response r;
  r.status_code = 200;
  r.status_text = "OK";
  r.protocol = "HTTP";
  r.version = 1.1;
  r.headers.push_back({"Content-Type", "text/plain"});
  r.headers.push_back({"Connection", "keep-alive"});
  r.body = "foo";
  auto ok = "HTTP/1.1 200 OK\r\n"s;
  ok += "Content-Type: text/plain\r\nConnection: keep-alive\r\n\r\nfoo";
  CHECK(to_string(r) == ok);
}

TEST(URI) {
  uri u;
  u.scheme = "http";
  u.host = "foo.bar";
  u.port = 80;
  u.path.push_back("foo");
  u.path.push_back("bar");
  u.path.push_back("baz");
  u.query["opt1"] = "val 1";
  u.query["opt2"] = "val2";
  u.fragment = "frag 1";
  auto ok = "http://foo.bar:80/foo/bar/baz?opt1=val%201&opt2=val2#frag%201"s;
  CHECK(to_string(u) == ok);
}

