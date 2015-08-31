#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/http.h"

#define SUITE printable
#include "test.h"

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
