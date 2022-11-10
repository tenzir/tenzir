//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://github.com/Stiffstream/restinio
// - Commit:     318db5642db6ffdf04558041aa4a74029c24820d
// - Path:       dev/sample/sendfiles/main.cpp
// - Author:     Nicolai Grodzitski
// - Created:    Feb 12, 2018
// - License:    BSD 3-Clause

#pragma once

#include <string_view>

namespace vast::plugins::web {

inline const char* content_type_by_file_extension(const std::string_view& ext) {
  // Function from Incomplete list of mime types from here:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Complete_list_of_MIME_types
  if (ext == ".aac")
    return "audio/aac";
  if (ext == ".abw")
    return "application/x-abiword";
  if (ext == ".arc")
    return "application/octet-stream";
  if (ext == ".avi")
    return "video/x-msvideo";
  if (ext == ".azw")
    return "application/vnd.amazon.ebook";
  if (ext == ".bin")
    return "application/octet-stream";
  if (ext == ".bz")
    return "application/x-bzip";
  if (ext == ".bz2")
    return "application/x-bzip2";
  if (ext == ".csh")
    return "application/x-csh";
  if (ext == ".css")
    return "text/css";
  if (ext == ".csv")
    return "text/csv";
  if (ext == ".doc")
    return "application/msword";
  if (ext == ".docx")
    return "application/"
           "vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (ext == ".eot")
    return "application/vnd.ms-fontobject";
  if (ext == ".epub")
    return "application/epub+zip";
  if (ext == ".gif")
    return "image/gif";
  if (ext == ".htm" || ext == ".html")
    return "text/html";
  if (ext == ".ico")
    return "image/x-icon";
  if (ext == ".ics")
    return "text/calendar";
  if (ext == ".jar")
    return "application/java-archive";
  if (ext == ".jpeg" || ext == ".jpg")
    return "image/jpeg";
  if (ext == ".js")
    return "application/javascript";
  if (ext == ".json")
    return "application/json";
  if (ext == ".mid" || ext == ".midi")
    return "audio/midi";
  if (ext == ".mpeg")
    return "video/mpeg";
  if (ext == ".mpkg")
    return "application/vnd.apple.installer+xml";
  if (ext == ".odp")
    return "application/vnd.oasis.opendocument.presentation";
  if (ext == ".ods")
    return "application/vnd.oasis.opendocument.spreadsheet";
  if (ext == ".odt")
    return "application/vnd.oasis.opendocument.text";
  if (ext == ".oga")
    return "audio/ogg";
  if (ext == ".ogv")
    return "video/ogg";
  if (ext == ".ogx")
    return "application/ogg";
  if (ext == ".otf")
    return "font/otf";
  if (ext == ".png")
    return "image/png";
  if (ext == ".pdf")
    return "application/pdf";
  if (ext == ".ppt")
    return "application/vnd.ms-powerpoint";
  if (ext == ".pptx")
    return "application/"
           "vnd.openxmlformats-officedocument.presentationml.presentation";
  if (ext == ".rar")
    return "archive application/x-rar-compressed";
  if (ext == ".rtf")
    return "application/rtf";
  if (ext == ".sh")
    return "application/x-sh";
  if (ext == ".svg")
    return "image/svg+xml";
  if (ext == ".swf")
    return "application/x-shockwave-flash";
  if (ext == ".tar")
    return "application/x-tar";
  if (ext == ".tif" || ext == ".tiff")
    return "image/tiff";
  if (ext == ".ts")
    return "application/typescript";
  if (ext == ".ttf")
    return "font/ttf";
  if (ext == ".vsd")
    return "application/vnd.visio";
  if (ext == ".wav")
    return "audio/x-wav";
  if (ext == ".weba")
    return "audio/webm";
  if (ext == ".webm")
    return "video/webm";
  if (ext == ".webp")
    return "image/webp";
  if (ext == ".woff")
    return "font/woff";
  if (ext == ".woff2")
    return "font/woff2";
  if (ext == ".xhtml")
    return "application/xhtml+xml";
  if (ext == ".xls")
    return "application/vnd.ms-excel";
  if (ext == ".xlsx")
    return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  if (ext == ".xml")
    return "application/xml";
  if (ext == ".xul")
    return "application/vnd.mozilla.xul+xml";
  if (ext == ".zip")
    return "archive application/zip";
  if (ext == ".3gp")
    return "video/3gpp";
  if (ext == ".3g2")
    return "video/3gpp2";
  if (ext == ".7z")
    return "application/x-7z-compressed";

  return "application/text";
}

} // namespace vast::plugins::web
