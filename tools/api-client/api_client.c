#include "vast/api/vast_api.h"

#include <stdio.h>

int main() {
  struct vast_info vm;
  vast_info(&vm);
  printf("VAST version: %s\n", vm.version);
  struct VAST* vast = vast_initialize();
  if (!vast) {
    fprintf(stderr, "failed to initialize VAST\n");
    return 1;
  }
  const char* endpoint = "localhost:42000";
  struct vast_connection* conn = vast_open(vast, endpoint);
  if (!conn) {
    fprintf(stderr, "failed to open connection to VAST at %s\n", endpoint);
    return 1;
  }
  char buf[1024];
  vast_status_json(vast, conn, buf, 1024);
  printf("status: %s\n", buf);
  vast_close(vast, conn);
  vast_finalize(vast);
  return 0;
}