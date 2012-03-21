#ifndef VAST_FS_FSTREAM_H
#define VAST_FS_FSTREAM_H

#include <boost/filesystem/fstream.hpp>

namespace vast {
namespace fs {

using boost::filesystem::filebuf;
using boost::filesystem::ifstream;
using boost::filesystem::ofstream;
using boost::filesystem::fstream;
using boost::filesystem::wfilebuf;
using boost::filesystem::wifstream;
using boost::filesystem::wfstream;
using boost::filesystem::wofstream;
using boost::filesystem::basic_filebuf;
using boost::filesystem::basic_ifstream;
using boost::filesystem::basic_ofstream;
using boost::filesystem::basic_fstream;

} // namespace fs
} // namespace vast

#endif
