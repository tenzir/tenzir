#include "vast/detail/compressedbuf.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/compression.hpp"

#define SUITE streambuf
#include "test.hpp"

using namespace std::string_literals;
using namespace vast;
using namespace vast::detail;

TEST(compressedbuf - two blocks) {
  // Create a compressed buffer with an internal block size of 8 bytes. A
  // compressed buffer can either be used for writing or reading, but not both
  // at the same time.
  std::stringbuf buf;
  compressedbuf sink{buf, compression::null, 8};
  MESSAGE("put area");
  CHECK_EQUAL(sink.sputn("", 0), 0);      // nop
  CHECK_EQUAL(sink.sputn("foo", 3), 3);   // 5 bytes left
  CHECK_EQUAL(sink.sputn("bar", 3), 3);   // 2 bytes left
  CHECK_EQUAL(sink.sputn("##", 2), 2);    // 0 bytes left
  CHECK_EQUAL(sink.sputc('*'), '*');      // trigger overflow()
  CHECK_EQUAL(sink.pubsync(), 3);         // flush (3: 2 for head, 1 for data)
  // Read from the compressed sequence of blocks in the underlying stringbuf.
  MESSAGE("get area");
  compressedbuf source{buf, compression::null, 8};
  CHECK_EQUAL(source.in_avail(), 0);
  CHECK_EQUAL(source.sgetn(nullptr, 0), 0); // nop
  auto c = source.sgetc();                // trigger underflow()
  CHECK_EQUAL(source.in_avail(), 8);      // get area filled with block data
  CHECK_EQUAL(c, 'f');
  std::string str;
  str.resize(8);
  auto n = source.sgetn(&str[0], 8);
  CHECK_EQUAL(n, 8);
  CHECK_EQUAL(str, "foobar##");
  CHECK_EQUAL(source.in_avail(), 0);
  c = source.sbumpc();                    // trigger underflow again()
  CHECK_EQUAL(c, '*');
  c = source.sgetc();                     // input exhausted
  CHECK_EQUAL(c, compressedbuf::traits_type::eof());
}

TEST(compressedbuf - iostream interface) {
  std::vector<compression> methods = {compression::null, compression::lz4};
#ifdef VAST_HAVE_SNAPPY
  methods.push_back(compression::snappy);
#endif
  std::vector<size_t> block_sizes = {1, 2, 64, 256, 1024, 16 << 10};
  auto data = "Im Kampf zwischen dir und der Welt sekundiere der Welt."s;
  auto inflation = 1000;
  for (auto block_size : block_sizes) {
    for (auto method : methods) {
      MESSAGE("block size " << block_size << ", method " << method);
      std::stringbuf buf;
      // Compress in full with std::ostream.
      compressedbuf sink{buf, method, block_size};
      std::ostream os{&sink};
      for (auto i = 0; i < inflation; ++i)
        os << data;
      os.flush(); // calls pubsync() on the contained stream buffer
      // Uncompress in full via std::istream into another std::stringstream.
      compressedbuf source{buf, method, block_size};
      std::istream is{&source};
      std::stringstream ss;
      ss << is.rdbuf();
      auto reassembled = ss.str();
      // Ensure correctness
      CHECK_EQUAL(reassembled.size(), data.size() * inflation);
      CHECK(reassembled.compare(0, data.size() + 1, data));
    }
  }
}

TEST(compressedbuf - xsgetn) {
  auto data = "Alle Wege bahnen sich vor mir"s;
  std::stringbuf buf;
  compressedbuf sink{buf};
  std::ostream os{&sink};
  os << data;
  os.flush();
  // Uncompress manually.
  compressedbuf source{buf, compression::null, 4};
  std::string str;
  str.resize(data.size());
  auto n = source.sgetn(&str[0], 5);
  CHECK_EQUAL(n, 5);
  CHECK(data.compare(0, 5 + 1, str));
  n += source.sgetn(&str[5], 20);
  CHECK_EQUAL(n, 5 + 20);
  CHECK(data.compare(0, 25 + 1, str));
  n += source.sgetn(&str[25], 42);  // only 4 more available
  CHECK_EQUAL(n, static_cast<std::streamsize>(data.size()));
  CHECK_EQUAL(str, data);
}
