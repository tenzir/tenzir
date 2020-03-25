#pragma once

namespace artifacts {
namespace logs {
namespace zeek {

extern const char* conn;
extern const char* dns;
extern const char* ftp;
extern const char* http;
extern const char* out;
extern const char* small_conn;
extern const char* smtp;
extern const char* ssl;

} // namespace zeek

namespace syslog {

extern const char* syslog_msgs;

} // namespace syslog

} // namespace logs

namespace traces {

extern const char* nmap_vsn;
extern const char* workshop_2011_browse;

} // namespace traces
} // namespace artifacts
