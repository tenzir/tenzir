add_subdirectory(dscat)
if (VAST_HAVE_BROKER)
  add_subdirectory(zeek-to-vast)
endif ()
