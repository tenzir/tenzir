add_executable(api_client api_client.c)
target_link_libraries(api_client PRIVATE vast::libvast vast::internal)

