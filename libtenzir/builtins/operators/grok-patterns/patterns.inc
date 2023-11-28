//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file is a modified amalgamated copy of the source found in Logstash:
//     https://github.com/logstash-plugins/logstash-patterns-core
//
// Copyright 2020 Elastic and contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

{
// grok-patterns
R"grok-patterns(
USERNAME [a-zA-Z0-9._-]+
USER %{USERNAME}
EMAILLOCALPART [a-zA-Z0-9!#$%&'*+\-/=?^_`{|}~]{1,64}(?:\.[a-zA-Z0-9!#$%&'*+\-/=?^_`{|}~]{1,62}){0,63}
EMAILADDRESS %{EMAILLOCALPART}@%{HOSTNAME}
INT (?:[+-]?(?:[0-9]+))
BASE10NUM (?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\.[0-9]+)?)|(?:\.[0-9]+)))
NUMBER (?:%{BASE10NUM})
BASE16NUM (?<![0-9A-Fa-f])(?:[+-]?(?:0x)?(?:[0-9A-Fa-f]+))
BASE16FLOAT \b(?<![0-9A-Fa-f.])(?:[+-]?(?:0x)?(?:(?:[0-9A-Fa-f]+(?:\.[0-9A-Fa-f]*)?)|(?:\.[0-9A-Fa-f]+)))\b

POSINT \b(?:[1-9][0-9]*)\b
NONNEGINT \b(?:[0-9]+)\b
WORD \b\w+\b
NOTSPACE \S+
SPACE \s*
DATA .*?
GREEDYDATA .*
QUOTEDSTRING (?>(?<!\\)(?>"(?>\\.|[^\\"]+)+"|""|(?>'(?>\\.|[^\\']+)+')|''|(?>`(?>\\.|[^\\`]+)+`)|``))
UUID [A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}
# URN, allowing use of RFC 2141 section 2.3 reserved characters
URN urn:[0-9A-Za-z][0-9A-Za-z-]{0,31}:(?:%[0-9a-fA-F]{2}|[0-9A-Za-z()+,.:=@;$_!*'/?#-])+

# Networking
MAC (?:%{CISCOMAC}|%{WINDOWSMAC}|%{COMMONMAC})
CISCOMAC (?:(?:[A-Fa-f0-9]{4}\.){2}[A-Fa-f0-9]{4})
WINDOWSMAC (?:(?:[A-Fa-f0-9]{2}-){5}[A-Fa-f0-9]{2})
COMMONMAC (?:(?:[A-Fa-f0-9]{2}:){5}[A-Fa-f0-9]{2})
IPV6 ((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))(%.+)?
IPV4 (?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])
IP (?:%{IPV6}|%{IPV4})
HOSTNAME \b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\.?|\b)
IPORHOST (?:%{IP}|%{HOSTNAME})
HOSTPORT %{IPORHOST}:%{POSINT}

# paths (only absolute paths are matched)
PATH (?:%{UNIXPATH}|%{WINPATH})
UNIXPATH (/[[[:alnum:]]_%!$@:.,+~-]*)+
TTY (?:/dev/(pts|tty([pq])?)(\w+)?/?(?:[0-9]+))
WINPATH (?>[A-Za-z]+:|\\)(?:\\[^\\?*]*)+
URIPROTO [A-Za-z]([A-Za-z0-9+\-.]+)+
URIHOST %{IPORHOST}(?::%{POSINT})?
# uripath comes loosely from RFC1738, but mostly from what Firefox doesn't turn into %XX
URIPATH (?:/[A-Za-z0-9$.+!*'(){},~:;=@#%&_\-]*)+
URIQUERY [A-Za-z0-9$.+!*'|(){},~@#%&/=:;_?\-\[\]<>]*
# deprecated (kept due compatibility):
URIPARAM \?%{URIQUERY}
URIPATHPARAM %{URIPATH}(?:\?%{URIQUERY})?
URI %{URIPROTO}://(?:%{USER}(?::[^@]*)?@)?(?:%{URIHOST})?(?:%{URIPATH}(?:\?%{URIQUERY})?)?

# Months: January, Feb, 3, 03, 12, December
MONTH \b(?:[Jj]an(?:uary|uar)?|[Ff]eb(?:ruary|ruar)?|[Mm](?:a|ä)?r(?:ch|z)?|[Aa]pr(?:il)?|[Mm]a(?:y|i)?|[Jj]un(?:e|i)?|[Jj]ul(?:y|i)?|[Aa]ug(?:ust)?|[Ss]ep(?:tember)?|[Oo](?:c|k)?t(?:ober)?|[Nn]ov(?:ember)?|[Dd]e(?:c|z)(?:ember)?)\b
MONTHNUM (?:0?[1-9]|1[0-2])
MONTHNUM2 (?:0[1-9]|1[0-2])
MONTHDAY (?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])

# Days: Monday, Tue, Thu, etc...
DAY (?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)

# Years?
YEAR (?>\d\d){1,2}
HOUR (?:2[0123]|[01]?[0-9])
MINUTE (?:[0-5][0-9])
# '60' is a leap second in most time standards and thus is valid.
SECOND (?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)
TIME (?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])
# datestamp is YYYY/MM/DD-HH:MM:SS.UUUU (or something like it)
DATE_US %{MONTHNUM}[/-]%{MONTHDAY}[/-]%{YEAR}
DATE_EU %{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}
ISO8601_TIMEZONE (?:Z|[+-]%{HOUR}(?::?%{MINUTE}))
ISO8601_SECOND %{SECOND}
TIMESTAMP_ISO8601 %{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?%{ISO8601_TIMEZONE}?
DATE %{DATE_US}|%{DATE_EU}
DATESTAMP %{DATE}[- ]%{TIME}
TZ (?:[APMCE][SD]T|UTC)
DATESTAMP_RFC822 %{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{TIME} %{TZ}
DATESTAMP_RFC2822 %{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{TIME} %{ISO8601_TIMEZONE}
DATESTAMP_OTHER %{DAY} %{MONTH} %{MONTHDAY} %{TIME} %{TZ} %{YEAR}
DATESTAMP_EVENTLOG %{YEAR}%{MONTHNUM2}%{MONTHDAY}%{HOUR}%{MINUTE}%{SECOND}

# Syslog Dates: Month Day HH:MM:SS
SYSLOGTIMESTAMP %{MONTH} +%{MONTHDAY} %{TIME}
PROG [\x21-\x5a\x5c\x5e-\x7e]+
SYSLOGPROG %{PROG:[process][name]}(?:\[%{POSINT:[process][pid]:int}\])?
SYSLOGHOST %{IPORHOST}
SYSLOGFACILITY <%{NONNEGINT:[log][syslog][facility][code]:int}.%{NONNEGINT:[log][syslog][priority]:int}>
HTTPDATE %{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}

# Shortcuts
QS %{QUOTEDSTRING}

# Log formats
SYSLOGBASE %{SYSLOGTIMESTAMP:timestamp} (?:%{SYSLOGFACILITY} )?%{SYSLOGHOST:[host][hostname]} %{SYSLOGPROG}:

# Log Levels
LOGLEVEL ([Aa]lert|ALERT|[Tt]race|TRACE|[Dd]ebug|DEBUG|[Nn]otice|NOTICE|[Ii]nfo?(?:rmation)?|INFO?(?:RMATION)?|[Ww]arn?(?:ing)?|WARN?(?:ING)?|[Ee]rr?(?:or)?|ERR?(?:OR)?|[Cc]rit?(?:ical)?|CRIT?(?:ICAL)?|[Ff]atal|FATAL|[Ss]evere|SEVERE|EMERG(?:ENCY)?|[Ee]merg(?:ency)?)
)grok-patterns",

// aws
R"aws-patterns(
S3_REQUEST_LINE (?:%{WORD:[http][request][method]} %{NOTSPACE:[url][original]}(?: HTTP/%{NUMBER:[http][version]})?)

S3_ACCESS_LOG %{WORD:[aws][s3access][bucket_owner]} %{NOTSPACE:[aws][s3access][bucket]} \[%{HTTPDATE:timestamp}\] (?:-|%{IP:[client][ip]}) (?:-|%{NOTSPACE:[client][user][id]}) %{NOTSPACE:[aws][s3access][request_id]} %{NOTSPACE:[aws][s3access][operation]} (?:-|%{NOTSPACE:[aws][s3access][key]}) (?:-|"%{S3_REQUEST_LINE:[aws][s3access][request_uri]}") (?:-|%{INT:[http][response][status_code]:int}) (?:-|%{NOTSPACE:[aws][s3access][error_code]}) (?:-|%{INT:[aws][s3access][bytes_sent]:int}) (?:-|%{INT:[aws][s3access][object_size]:int}) (?:-|%{INT:[aws][s3access][total_time]:int}) (?:-|%{INT:[aws][s3access][turn_around_time]:int}) "(?:-|%{DATA:[http][request][referrer]})" "(?:-|%{DATA:[user_agent][original]})" (?:-|%{NOTSPACE:[aws][s3access][version_id]})(?: (?:-|%{NOTSPACE:[aws][s3access][host_id]}) (?:-|%{NOTSPACE:[aws][s3access][signature_version]}) (?:-|%{NOTSPACE:[tls][cipher]}) (?:-|%{NOTSPACE:[aws][s3access][authentication_type]}) (?:-|%{NOTSPACE:[aws][s3access][host_header]}) (?:-|%{NOTSPACE:[aws][s3access][tls_version]}))?
# :long - %{INT:[aws][s3access][bytes_sent]:int}
# :long - %{INT:[aws][s3access][object_size]:int}

ELB_URIHOST %{IPORHOST:[url][domain]}(?::%{POSINT:[url][port]:int})?
ELB_URIPATHQUERY %{URIPATH:[url][path]}(?:\?%{URIQUERY:[url][query]})?
# deprecated - old name:
ELB_URIPATHPARAM %{ELB_URIPATHQUERY}
ELB_URI %{URIPROTO:[url][scheme]}://(?:%{USER:[url][username]}(?::[^@]*)?@)?(?:%{ELB_URIHOST})?(?:%{ELB_URIPATHQUERY})?

ELB_REQUEST_LINE (?:%{WORD:[http][request][method]} %{ELB_URI:[url][original]}(?: HTTP/%{NUMBER:[http][version]})?)

# pattern supports 'regular' HTTP ELB format
ELB_V1_HTTP_LOG %{TIMESTAMP_ISO8601:timestamp} %{NOTSPACE:[aws][elb][name]} %{IP:[source][ip]}:%{INT:[source][port]:int} (?:-|(?:%{IP:[aws][elb][backend][ip]}:%{INT:[aws][elb][backend][port]:int})) (?:-1|%{NUMBER:[aws][elb][request_processing_time][sec]:float}) (?:-1|%{NUMBER:[aws][elb][backend_processing_time][sec]:float}) (?:-1|%{NUMBER:[aws][elb][response_processing_time][sec]:float}) %{INT:[http][response][status_code]:int} (?:-|%{INT:[aws][elb][backend][http][response][status_code]:int}) %{INT:[http][request][body][bytes]:int} %{INT:[http][response][body][bytes]:int} "%{ELB_REQUEST_LINE}"(?: "(?:-|%{DATA:[user_agent][original]})" (?:-|%{NOTSPACE:[tls][cipher]}) (?:-|%{NOTSPACE:[aws][elb][ssl_protocol]}))?
# :long - %{INT:[http][request][body][bytes]:int}
# :long - %{INT:[http][response][body][bytes]:int}

ELB_ACCESS_LOG %{ELB_V1_HTTP_LOG}

# Each edge location is identified by a three-letter code and an arbitrarily assigned number.
# The three-letter IATA code typically represents an airport near the edge location.
# examples: "LHR62-C2", "SFO5-P1", ""IND6", "CPT50"
CLOUDFRONT_EDGE_LOCATION [A-Z]{3}[0-9]{1,2}(?:-[A-Z0-9]{2})?

# pattern used to match a shorted format, that's why we have the optional part (starting with *http.version*) at the end
CLOUDFRONT_ACCESS_LOG (?<timestamp>%{YEAR}-%{MONTHNUM}-%{MONTHDAY}\t%{TIME})\t%{CLOUDFRONT_EDGE_LOCATION:[aws][cloudfront][x_edge_location]}\t(?:-|%{INT:[destination][bytes]:int})\t%{IPORHOST:[source][ip]}\t%{WORD:[http][request][method]}\t%{HOSTNAME:[url][domain]}\t%{NOTSPACE:[url][path]}\t(?:(?:000)|%{INT:[http][response][status_code]:int})\t(?:-|%{DATA:[http][request][referrer]})\t%{DATA:[user_agent][original]}\t(?:-|%{DATA:[url][query]})\t(?:-|%{DATA:[aws][cloudfront][http][request][cookie]})\t%{WORD:[aws][cloudfront][x_edge_result_type]}\t%{NOTSPACE:[aws][cloudfront][x_edge_request_id]}\t%{HOSTNAME:[aws][cloudfront][http][request][host]}\t%{URIPROTO:[network][protocol]}\t(?:-|%{INT:[source][bytes]:int})\t%{NUMBER:[aws][cloudfront][time_taken]:float}\t(?:-|%{IP:[network][forwarded_ip]})\t(?:-|%{DATA:[aws][cloudfront][ssl_protocol]})\t(?:-|%{NOTSPACE:[tls][cipher]})\t%{WORD:[aws][cloudfront][x_edge_response_result_type]}(?:\t(?:-|HTTP/%{NUMBER:[http][version]})\t(?:-|%{DATA:[aws][cloudfront][fle_status]})\t(?:-|%{DATA:[aws][cloudfront][fle_encrypted_fields]})\t%{INT:[source][port]:int}\t%{NUMBER:[aws][cloudfront][time_to_first_byte]:float}\t(?:-|%{DATA:[aws][cloudfront][x_edge_detailed_result_type]})\t(?:-|%{NOTSPACE:[http][request][mime_type]})\t(?:-|%{INT:[aws][cloudfront][http][request][size]:int})\t(?:-|%{INT:[aws][cloudfront][http][request][range][start]:int})\t(?:-|%{INT:[aws][cloudfront][http][request][range][end]:int}))?
# :long - %{INT:[destination][bytes]:int}
# :long - %{INT:[source][bytes]:int}
# :long - %{INT:[aws][cloudfront][http][request][size]:int}
# :long - %{INT:[aws][cloudfront][http][request][range][start]:int}
# :long - %{INT:[aws][cloudfront][http][request][range][end]:int}
)aws-patterns",

// bacula
R"bacula-patterns(
BACULA_TIMESTAMP %{MONTHDAY}-%{MONTH}(?:-%{YEAR})? %{HOUR}:%{MINUTE}
BACULA_HOST %{HOSTNAME}
BACULA_VOLUME %{USER}
BACULA_DEVICE %{USER}
BACULA_DEVICEPATH %{UNIXPATH}
BACULA_CAPACITY %{INT}{1,3}(,%{INT}{3})*
BACULA_VERSION %{USER}
BACULA_JOB %{USER}

BACULA_LOG_MAX_CAPACITY User defined maximum volume capacity %{BACULA_CAPACITY:[bacula][volume][max_capacity]} exceeded on device \"%{BACULA_DEVICE:[bacula][volume][device]}\" \(%{BACULA_DEVICEPATH:[bacula][volume][path]}\).?
BACULA_LOG_END_VOLUME End of medium on Volume \"%{BACULA_VOLUME:[bacula][volume][name]}\" Bytes=%{BACULA_CAPACITY:[bacula][volume][bytes]} Blocks=%{BACULA_CAPACITY:[bacula][volume][blocks]} at %{BACULA_TIMESTAMP:[bacula][timestamp]}.
BACULA_LOG_NEW_VOLUME Created new Volume \"%{BACULA_VOLUME:[bacula][volume][name]}\" in catalog.
BACULA_LOG_NEW_LABEL Labeled new Volume \"%{BACULA_VOLUME:[bacula][volume][name]}\" on (?:file )?device \"%{BACULA_DEVICE:[bacula][volume][device]}\" \(%{BACULA_DEVICEPATH:[bacula][volume][path]}\).
BACULA_LOG_WROTE_LABEL Wrote label to prelabeled Volume \"%{BACULA_VOLUME:[bacula][volume][name]}\" on device \"%{BACULA_DEVICE:[bacula][volume][device]}\" \(%{BACULA_DEVICEPATH:[bacula][volume][path]}\)
BACULA_LOG_NEW_MOUNT New volume \"%{BACULA_VOLUME:[bacula][volume][name]}\" mounted on device \"%{BACULA_DEVICE:[bacula][volume][device]}\" \(%{BACULA_DEVICEPATH:[bacula][volume][path]}\) at %{BACULA_TIMESTAMP:[bacula][timestamp]}.
BACULA_LOG_NOOPEN \s*Cannot open %{DATA}: ERR=%{GREEDYDATA:[error][message]}
BACULA_LOG_NOOPENDIR \s*Could not open directory \"?%{DATA:[file][path]}\"?: ERR=%{GREEDYDATA:[error][message]}
BACULA_LOG_NOSTAT \s*Could not stat %{DATA:[file][path]}: ERR=%{GREEDYDATA:[error][message]}
BACULA_LOG_NOJOBS There are no more Jobs associated with Volume \"%{BACULA_VOLUME:[bacula][volume][name]}\". Marking it purged.
BACULA_LOG_ALL_RECORDS_PRUNED .*?All records pruned from Volume \"%{BACULA_VOLUME:[bacula][volume][name]}\"; marking it \"Purged\"
BACULA_LOG_BEGIN_PRUNE_JOBS Begin pruning Jobs older than %{INT} month %{INT} days .
BACULA_LOG_BEGIN_PRUNE_FILES Begin pruning Files.
BACULA_LOG_PRUNED_JOBS Pruned %{INT} Jobs* for client %{BACULA_HOST:[bacula][client][name]} from catalog.
BACULA_LOG_PRUNED_FILES Pruned Files from %{INT} Jobs* for client %{BACULA_HOST:[bacula][client][name]} from catalog.
BACULA_LOG_ENDPRUNE End auto prune.
BACULA_LOG_STARTJOB Start Backup JobId %{INT}, Job=%{BACULA_JOB:[bacula][job][name]}
BACULA_LOG_STARTRESTORE Start Restore Job %{BACULA_JOB:[bacula][job][name]}
BACULA_LOG_USEDEVICE Using Device \"%{BACULA_DEVICE:[bacula][volume][device]}\"
BACULA_LOG_DIFF_FS \s*%{UNIXPATH} is a different filesystem. Will not descend from %{UNIXPATH} into it.
BACULA_LOG_JOBEND Job write elapsed time = %{DATA:[bacula][job][elapsed_time]}, Transfer rate = %{NUMBER} (K|M|G)? Bytes/second
BACULA_LOG_NOPRUNE_JOBS No Jobs found to prune.
BACULA_LOG_NOPRUNE_FILES No Files found to prune.
BACULA_LOG_VOLUME_PREVWRITTEN Volume \"?%{BACULA_VOLUME:[bacula][volume][name]}\"? previously written, moving to end of data.
BACULA_LOG_READYAPPEND Ready to append to end of Volume \"%{BACULA_VOLUME:[bacula][volume][name]}\" size=%{INT:[bacula][volume][size]:int}
# :long - %{INT:[bacula][volume][size]:int}
BACULA_LOG_CANCELLING Cancelling duplicate JobId=%{INT:[bacula][job][other_id]}.
BACULA_LOG_MARKCANCEL JobId %{INT:[bacula][job][id]}, Job %{BACULA_JOB:[bacula][job][name]} marked to be canceled.
BACULA_LOG_CLIENT_RBJ shell command: run ClientRunBeforeJob \"%{GREEDYDATA:[bacula][job][client_run_before_command]}\"
BACULA_LOG_VSS (Generate )?VSS (Writer)?
BACULA_LOG_MAXSTART Fatal [eE]rror: Job canceled because max start delay time exceeded.
BACULA_LOG_DUPLICATE Fatal [eE]rror: JobId %{INT:[bacula][job][other_id]} already running. Duplicate job not allowed.
BACULA_LOG_NOJOBSTAT Fatal [eE]rror: No Job status returned from FD.
BACULA_LOG_FATAL_CONN Fatal [eE]rror: bsock.c:133 Unable to connect to (Client: %{BACULA_HOST:[bacula][client][name]}|Storage daemon) on %{IPORHOST:[client][address]}:%{POSINT:[client][port]:int}. ERR=%{GREEDYDATA:[error][message]}
BACULA_LOG_NO_CONNECT Warning: bsock.c:127 Could not connect to (Client: %{BACULA_HOST:[bacula][client][name]}|Storage daemon) on %{IPORHOST:[client][address]}:%{POSINT:[client][port]:int}. ERR=%{GREEDYDATA:[error][message]}
BACULA_LOG_NO_AUTH Fatal error: Unable to authenticate with File daemon at \"?%{IPORHOST:[client][address]}(?::%{POSINT:[client][port]:int})?\"?. Possible causes:
BACULA_LOG_NOSUIT No prior or suitable Full backup found in catalog. Doing FULL backup.
BACULA_LOG_NOPRIOR No prior Full backup Job record found.

BACULA_LOG_JOB (Error: )?Bacula %{BACULA_HOST} %{BACULA_VERSION} \(%{BACULA_VERSION}\):

BACULA_LOG %{BACULA_TIMESTAMP:timestamp} %{BACULA_HOST:[host][hostname]}(?: JobId %{INT:[bacula][job][id]})?:? (%{BACULA_LOG_MAX_CAPACITY}|%{BACULA_LOG_END_VOLUME}|%{BACULA_LOG_NEW_VOLUME}|%{BACULA_LOG_NEW_LABEL}|%{BACULA_LOG_WROTE_LABEL}|%{BACULA_LOG_NEW_MOUNT}|%{BACULA_LOG_NOOPEN}|%{BACULA_LOG_NOOPENDIR}|%{BACULA_LOG_NOSTAT}|%{BACULA_LOG_NOJOBS}|%{BACULA_LOG_ALL_RECORDS_PRUNED}|%{BACULA_LOG_BEGIN_PRUNE_JOBS}|%{BACULA_LOG_BEGIN_PRUNE_FILES}|%{BACULA_LOG_PRUNED_JOBS}|%{BACULA_LOG_PRUNED_FILES}|%{BACULA_LOG_ENDPRUNE}|%{BACULA_LOG_STARTJOB}|%{BACULA_LOG_STARTRESTORE}|%{BACULA_LOG_USEDEVICE}|%{BACULA_LOG_DIFF_FS}|%{BACULA_LOG_JOBEND}|%{BACULA_LOG_NOPRUNE_JOBS}|%{BACULA_LOG_NOPRUNE_FILES}|%{BACULA_LOG_VOLUME_PREVWRITTEN}|%{BACULA_LOG_READYAPPEND}|%{BACULA_LOG_CANCELLING}|%{BACULA_LOG_MARKCANCEL}|%{BACULA_LOG_CLIENT_RBJ}|%{BACULA_LOG_VSS}|%{BACULA_LOG_MAXSTART}|%{BACULA_LOG_DUPLICATE}|%{BACULA_LOG_NOJOBSTAT}|%{BACULA_LOG_FATAL_CONN}|%{BACULA_LOG_NO_CONNECT}|%{BACULA_LOG_NO_AUTH}|%{BACULA_LOG_NOSUIT}|%{BACULA_LOG_JOB}|%{BACULA_LOG_NOPRIOR})
# old (deprecated) name :
BACULA_LOGLINE %{BACULA_LOG}
)bacula-patterns",

// bind
R"bind-patterns(
BIND9_TIMESTAMP %{MONTHDAY}[-]%{MONTH}[-]%{YEAR} %{TIME}

BIND9_DNSTYPE (?:A|AAAA|CAA|CDNSKEY|CDS|CERT|CNAME|CSYNC|DLV|DNAME|DNSKEY|DS|HINFO|LOC|MX|NAPTR|NS|NSEC|NSEC3|OPENPGPKEY|PTR|RRSIG|RP|SIG|SMIMEA|SOA|SRV|TSIG|TXT|URI)
BIND9_CATEGORY (?:queries)

# dns.question.class is static - only 'IN' is supported by Bind9
# bind.log.question.name is expected to be a 'duplicate' (same as the dns.question.name capture)
BIND9_QUERYLOGBASE client(:? @0x(?:[0-9A-Fa-f]+))? %{IP:[client][ip]}#%{POSINT:[client][port]:int} \(%{GREEDYDATA:[bind][log][question][name]}\): query: %{GREEDYDATA:[dns][question][name]} (?<[dns][question][class]>IN) %{BIND9_DNSTYPE:[dns][question][type]}(:? %{DATA:[bind][log][question][flags]})? \(%{IP:[server][ip]}\)

# for query-logging category and severity are always fixed as "queries: info: "
BIND9_QUERYLOG %{BIND9_TIMESTAMP:timestamp} %{BIND9_CATEGORY:[bind][log][category]}: %{LOGLEVEL:[log][level]}: %{BIND9_QUERYLOGBASE}

BIND9 %{BIND9_QUERYLOG}
)bind-patterns",

// bro
R"bro-patterns(
# supports the 'old' BRO log files, for updated Zeek log format see the patters/ecs-v1/zeek
# https://www.bro.org/sphinx/script-reference/log-files.html

BRO_BOOL [TF]
BRO_DATA [^\t]+

# http.log - old format (before the Zeek rename) :
BRO_HTTP %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{INT:[zeek][http][trans_depth]:int}\t(?:-|%{WORD:[http][request][method]})\t(?:-|%{BRO_DATA:[url][domain]})\t(?:-|%{BRO_DATA:[url][original]})\t(?:-|%{BRO_DATA:[http][request][referrer]})\t(?:-|%{BRO_DATA:[user_agent][original]})\t(?:-|%{NUMBER:[http][request][body][bytes]:int})\t(?:-|%{NUMBER:[http][response][body][bytes]:int})\t(?:-|%{POSINT:[http][response][status_code]:int})\t(?:-|%{DATA:[zeek][http][status_msg]})\t(?:-|%{POSINT:[zeek][http][info_code]:int})\t(?:-|%{DATA:[zeek][http][info_msg]})\t(?:-|%{BRO_DATA:[zeek][http][filename]})\t(?:\(empty\)|%{BRO_DATA:[zeek][http][tags]})\t(?:-|%{BRO_DATA:[url][username]})\t(?:-|%{BRO_DATA:[url][password]})\t(?:-|%{BRO_DATA:[zeek][http][proxied]})\t(?:-|%{BRO_DATA:[zeek][http][orig_fuids]})\t(?:-|%{BRO_DATA:[http][request][mime_type]})\t(?:-|%{BRO_DATA:[zeek][http][resp_fuids]})\t(?:-|%{BRO_DATA:[http][response][mime_type]})
# :long - %{NUMBER:[http][request][body][bytes]:int}
# :long - %{NUMBER:[http][response][body][bytes]:int}

# dns.log - old format
BRO_DNS %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{WORD:[network][transport]}\t(?:-|%{INT:[dns][id]:int})\t(?:-|%{BRO_DATA:[dns][question][name]})\t(?:-|%{INT:[zeek][dns][qclass]:int})\t(?:-|%{BRO_DATA:[zeek][dns][qclass_name]})\t(?:-|%{INT:[zeek][dns][qtype]:int})\t(?:-|%{BRO_DATA:[dns][question][type]})\t(?:-|%{INT:[zeek][dns][rcode]:int})\t(?:-|%{BRO_DATA:[dns][response_code]})\t(?:-|%{BRO_BOOL:[zeek][dns][AA]})\t(?:-|%{BRO_BOOL:[zeek][dns][TC]})\t(?:-|%{BRO_BOOL:[zeek][dns][RD]})\t(?:-|%{BRO_BOOL:[zeek][dns][RA]})\t(?:-|%{NONNEGINT:[zeek][dns][Z]:int})\t(?:-|%{BRO_DATA:[zeek][dns][answers]})\t(?:-|%{DATA:[zeek][dns][TTLs]})\t(?:-|%{BRO_BOOL:[zeek][dns][rejected]})

# conn.log - old bro, also supports 'newer' format (optional *zeek.connection.local_resp* flag) compared to non-ecs mode
BRO_CONN %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{WORD:[network][transport]}\t(?:-|%{BRO_DATA:[network][protocol]})\t(?:-|%{NUMBER:[zeek][connection][duration]:float})\t(?:-|%{INT:[zeek][connection][orig_bytes]:int})\t(?:-|%{INT:[zeek][connection][resp_bytes]:int})\t(?:-|%{BRO_DATA:[zeek][connection][state]})\t(?:-|%{BRO_BOOL:[zeek][connection][local_orig]})\t(?:(?:-|%{BRO_BOOL:[zeek][connection][local_resp]})\t)?(?:-|%{INT:[zeek][connection][missed_bytes]:int})\t(?:-|%{BRO_DATA:[zeek][connection][history]})\t(?:-|%{INT:[source][packets]:int})\t(?:-|%{INT:[source][bytes]:int})\t(?:-|%{INT:[destination][packets]:int})\t(?:-|%{INT:[destination][bytes]:int})\t(?:\(empty\)|%{BRO_DATA:[zeek][connection][tunnel_parents]})
# :long - %{INT:[zeek][connection][orig_bytes]:int}
# :long - %{INT:[zeek][connection][resp_bytes]:int}
# :long - %{INT:[zeek][connection][missed_bytes]:int}
# :long - %{INT:[source][packets]:int}
# :long - %{INT:[source][bytes]:int}
# :long - %{INT:[destination][packets]:int}
# :long - %{INT:[destination][bytes]:int}

# files.log - old format
BRO_FILES %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][files][fuid]}\t(?:-|%{IP:[server][ip]})\t(?:-|%{IP:[client][ip]})\t(?:-|%{BRO_DATA:[zeek][files][session_ids]})\t(?:-|%{BRO_DATA:[zeek][files][source]})\t(?:-|%{INT:[zeek][files][depth]:int})\t(?:-|%{BRO_DATA:[zeek][files][analyzers]})\t(?:-|%{BRO_DATA:[file][mime_type]})\t(?:-|%{BRO_DATA:[file][name]})\t(?:-|%{NUMBER:[zeek][files][duration]:float})\t(?:-|%{BRO_DATA:[zeek][files][local_orig]})\t(?:-|%{BRO_BOOL:[zeek][files][is_orig]})\t(?:-|%{INT:[zeek][files][seen_bytes]:int})\t(?:-|%{INT:[file][size]:int})\t(?:-|%{INT:[zeek][files][missing_bytes]:int})\t(?:-|%{INT:[zeek][files][overflow_bytes]:int})\t(?:-|%{BRO_BOOL:[zeek][files][timedout]})\t(?:-|%{BRO_DATA:[zeek][files][parent_fuid]})\t(?:-|%{BRO_DATA:[file][hash][md5]})\t(?:-|%{BRO_DATA:[file][hash][sha1]})\t(?:-|%{BRO_DATA:[file][hash][sha256]})\t(?:-|%{BRO_DATA:[zeek][files][extracted]})
# :long - %{INT:[zeek][files][seen_bytes]:int}
# :long - %{INT:[file][size]:int}
# :long - %{INT:[zeek][files][missing_bytes]:int}
# :long - %{INT:[zeek][files][overflow_bytes]:int}
)bro-patterns",

// exim
R"exim-patterns(
EXIM_MSGID [0-9A-Za-z]{6}-[0-9A-Za-z]{6}-[0-9A-Za-z]{2}
# <=     message arrival
# =>     normal message delivery
# ->     additional address in same delivery
# *>     delivery suppressed by -N
# **     delivery failed; address bounced
# ==     delivery deferred; temporary problem
EXIM_FLAGS (?:<=|=>|->|\*>|\*\*|==|<>|>>)
EXIM_DATE (:?%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{TIME})
EXIM_PID \[%{POSINT:[process][pid]:int}\]
EXIM_QT ((\d+y)?(\d+w)?(\d+d)?(\d+h)?(\d+m)?(\d+s)?)
EXIM_EXCLUDE_TERMS (Message is frozen|(Start|End) queue run| Warning: | retry time not reached | no (IP address|host name) found for (IP address|host) | unexpected disconnection while reading SMTP command | no immediate delivery: |another process is handling this message)
EXIM_REMOTE_HOST (H=(%{NOTSPACE:[source][address]} )?(\(%{NOTSPACE:[exim][log][remote_address]}\) )?\[%{IP:[source][ip]}\](?::%{POSINT:[source][port]:int})?)
EXIM_INTERFACE (I=\[%{IP:[destination][ip]}\](?::%{NUMBER:[destination][port]:int}))
EXIM_PROTOCOL (P=%{NOTSPACE:[network][protocol]})
EXIM_MSG_SIZE (S=%{NUMBER:[exim][log][message][size]:int})
EXIM_HEADER_ID (id=%{NOTSPACE:[exim][log][header_id]})
EXIM_QUOTED_CONTENT (?:\\.|[^\\"])*
EXIM_SUBJECT (T="%{EXIM_QUOTED_CONTENT:[exim][log][message][subject]}")

EXIM_UNKNOWN_FIELD (?:[A-Za-z0-9]{1,4}=(?:%{QUOTEDSTRING}|%{NOTSPACE}))
EXIM_NAMED_FIELDS (?: (?:%{EXIM_REMOTE_HOST}|%{EXIM_INTERFACE}|%{EXIM_PROTOCOL}|%{EXIM_MSG_SIZE}|%{EXIM_HEADER_ID}|%{EXIM_SUBJECT}|%{EXIM_UNKNOWN_FIELD}))*

EXIM_MESSAGE_ARRIVAL %{EXIM_DATE:timestamp} (?:%{EXIM_PID} )?%{EXIM_MSGID:[exim][log][message][id]} (?<[exim][log][flags]><=) (?<[exim][log][status]>[a-z:] )?%{EMAILADDRESS:[exim][log][sender][email]}%{EXIM_NAMED_FIELDS}(?:(?: from <?%{DATA:[exim][log][sender][original]}>?)? for %{EMAILADDRESS:[exim][log][recipient][email]})?

EXIM %{EXIM_MESSAGE_ARRIVAL}
)exim-patterns",

// firewalls
R"firewalls(
# NetScreen firewall logs
NETSCREENSESSIONLOG %{SYSLOGTIMESTAMP:timestamp} %{IPORHOST:[observer][hostname]} %{NOTSPACE:[observer][name]}: (?<[observer][product]>NetScreen) device_id=%{WORD:[netscreen][device_id]} .*?(system-\w+-%{NONNEGINT:[event][code]}\(%{WORD:[netscreen][session][type]}\))?: start_time="%{DATA:[netscreen][session][start_time]}" duration=%{INT:[netscreen][session][duration]:int} policy_id=%{INT:[netscreen][policy_id]} service=%{DATA:[netscreen][service]} proto=%{INT:[netscreen][protocol_number]:int} src zone=%{WORD:[observer][ingress][zone]} dst zone=%{WORD:[observer][egress][zone]} action=%{WORD:[event][action]} sent=%{INT:[source][bytes]:int} rcvd=%{INT:[destination][bytes]:int} src=%{IPORHOST:[source][address]} dst=%{IPORHOST:[destination][address]}(?: src_port=%{INT:[source][port]:int} dst_port=%{INT:[destination][port]:int})?(?: src-xlated ip=%{IP:[source][nat][ip]} port=%{INT:[source][nat][port]:int} dst-xlated ip=%{IP:[destination][nat][ip]} port=%{INT:[destination][nat][port]:int})?(?: session_id=%{INT:[netscreen][session][id]} reason=%{GREEDYDATA:[netscreen][session][reason]})?
# :long - %{INT:[source][bytes]:int}
# :long - %{INT:[destination][bytes]:int}

#== Cisco ASA ==
CISCO_TAGGED_SYSLOG ^<%{POSINT:[log][syslog][priority]:int}>%{CISCOTIMESTAMP:timestamp}( %{SYSLOGHOST:[host][hostname]})? ?: %%{CISCOTAG:[cisco][asa][tag]}:
CISCOTIMESTAMP %{MONTH} +%{MONTHDAY}(?: %{YEAR})? %{TIME}
CISCOTAG [A-Z0-9]+-%{INT}-(?:[A-Z0-9_]+)
# Common Particles
CISCO_ACTION Built|Teardown|Deny|Denied|denied|requested|permitted|denied by ACL|discarded|est-allowed|Dropping|created|deleted
CISCO_REASON Duplicate TCP SYN|Failed to locate egress interface|Invalid transport field|No matching connection|DNS Response|DNS Query|(?:%{WORD}\s*)*
CISCO_DIRECTION Inbound|inbound|Outbound|outbound
CISCO_INTERVAL first hit|%{INT}-second interval
CISCO_XLATE_TYPE static|dynamic
# helpers
CISCO_HITCOUNT_INTERVAL hit-cnt %{INT:[cisco][asa][hit_count]:int} (?:first hit|%{INT:[cisco][asa][interval]:int}-second interval)
CISCO_SRC_IP_USER %{NOTSPACE:[observer][ingress][interface][name]}:%{IP:[source][ip]}(?:\(%{DATA:[source][user][name]}\))?
CISCO_DST_IP_USER %{NOTSPACE:[observer][egress][interface][name]}:%{IP:[destination][ip]}(?:\(%{DATA:[destination][user][name]}\))?
CISCO_SRC_HOST_PORT_USER %{NOTSPACE:[observer][ingress][interface][name]}:(?:(?:%{IP:[source][ip]})|(?:%{HOSTNAME:[source][address]}))(?:/%{INT:[source][port]:int})?(?:\(%{DATA:[source][user][name]}\))?
CISCO_DST_HOST_PORT_USER %{NOTSPACE:[observer][egress][interface][name]}:(?:(?:%{IP:[destination][ip]})|(?:%{HOSTNAME:[destination][address]}))(?:/%{INT:[destination][port]:int})?(?:\(%{DATA:[destination][user][name]}\))?
# ASA-1-104001
CISCOFW104001 \((?:Primary|Secondary)\) Switching to ACTIVE - %{GREEDYDATA:[event][reason]}
# ASA-1-104002
CISCOFW104002 \((?:Primary|Secondary)\) Switching to STANDBY - %{GREEDYDATA:[event][reason]}
# ASA-1-104003
CISCOFW104003 \((?:Primary|Secondary)\) Switching to FAILED\.
# ASA-1-104004
CISCOFW104004 \((?:Primary|Secondary)\) Switching to OK\.
# ASA-1-105003
CISCOFW105003 \((?:Primary|Secondary)\) Monitoring on [Ii]nterface %{NOTSPACE:[network][interface][name]} waiting
# ASA-1-105004
CISCOFW105004 \((?:Primary|Secondary)\) Monitoring on [Ii]nterface %{NOTSPACE:[network][interface][name]} normal
# ASA-1-105005
CISCOFW105005 \((?:Primary|Secondary)\) Lost Failover communications with mate on [Ii]nterface %{NOTSPACE:[network][interface][name]}
# ASA-1-105008
CISCOFW105008 \((?:Primary|Secondary)\) Testing [Ii]nterface %{NOTSPACE:[network][interface][name]}
# ASA-1-105009
CISCOFW105009 \((?:Primary|Secondary)\) Testing on [Ii]nterface %{NOTSPACE:[network][interface][name]} (?:Passed|Failed)
# ASA-2-106001
CISCOFW106001 %{CISCO_DIRECTION:[cisco][asa][network][direction]} %{WORD:[cisco][asa][network][transport]} connection %{CISCO_ACTION:[cisco][asa][outcome]} from %{IP:[source][ip]}/%{INT:[source][port]:int} to %{IP:[destination][ip]}/%{INT:[destination][port]:int} flags %{DATA:[cisco][asa][tcp_flags]} on interface %{NOTSPACE:[observer][egress][interface][name]}
# ASA-2-106006, ASA-2-106007, ASA-2-106010
CISCOFW106006_106007_106010 %{CISCO_ACTION:[cisco][asa][outcome]} %{CISCO_DIRECTION:[cisco][asa][network][direction]} %{WORD:[cisco][asa][network][transport]} (?:from|src) %{IP:[source][ip]}/%{INT:[source][port]:int}(?:\(%{DATA:[source][user][name]}\))? (?:to|dst) %{IP:[destination][ip]}/%{INT:[destination][port]:int}(?:\(%{DATA:[destination][user][name]}\))? (?:(?:on interface %{NOTSPACE:[observer][egress][interface][name]})|(?:due to %{CISCO_REASON:[event][reason]}))
# ASA-3-106014
CISCOFW106014 %{CISCO_ACTION:[cisco][asa][outcome]} %{CISCO_DIRECTION:[cisco][asa][network][direction]} %{WORD:[cisco][asa][network][transport]} src %{CISCO_SRC_IP_USER} dst %{CISCO_DST_IP_USER}\s?\(type %{INT:[cisco][asa][icmp_type]:int}, code %{INT:[cisco][asa][icmp_code]:int}\)
# ASA-6-106015
CISCOFW106015 %{CISCO_ACTION:[cisco][asa][outcome]} %{WORD:[cisco][asa][network][transport]} \(%{DATA:[cisco][asa][rule_name]}\) from %{IP:[source][ip]}/%{INT:[source][port]:int} to %{IP:[destination][ip]}/%{INT:[destination][port]:int} flags %{DATA:[cisco][asa][tcp_flags]} on interface %{NOTSPACE:[observer][egress][interface][name]}
# ASA-1-106021
CISCOFW106021 %{CISCO_ACTION:[cisco][asa][outcome]} %{WORD:[cisco][asa][network][transport]} reverse path check from %{IP:[source][ip]} to %{IP:[destination][ip]} on interface %{NOTSPACE:[observer][egress][interface][name]}
# ASA-4-106023
CISCOFW106023 %{CISCO_ACTION:[cisco][asa][outcome]}(?: protocol)? %{WORD:[cisco][asa][network][transport]} src %{CISCO_SRC_HOST_PORT_USER} dst %{CISCO_DST_HOST_PORT_USER}( \(type %{INT:[cisco][asa][icmp_type]:int}, code %{INT:[cisco][asa][icmp_code]:int}\))? by access-group "?%{DATA:[cisco][asa][rule_name]}"? \[%{DATA:[@metadata][cisco][asa][hashcode1]}, %{DATA:[@metadata][cisco][asa][hashcode2]}\]
# ASA-4-106100, ASA-4-106102, ASA-4-106103
CISCOFW106100_2_3 access-list %{NOTSPACE:[cisco][asa][rule_name]} %{CISCO_ACTION:[cisco][asa][outcome]} %{WORD:[cisco][asa][network][transport]} for user '%{DATA:[user][name]}' %{DATA:[observer][ingress][interface][name]}/%{IP:[source][ip]}\(%{INT:[source][port]:int}\) -> %{DATA:[observer][egress][interface][name]}/%{IP:[destination][ip]}\(%{INT:[destination][port]:int}\) %{CISCO_HITCOUNT_INTERVAL} \[%{DATA:[@metadata][cisco][asa][hashcode1]}, %{DATA:[@metadata][cisco][asa][hashcode2]}\]
# ASA-5-106100
CISCOFW106100 access-list %{NOTSPACE:[cisco][asa][rule_name]} %{CISCO_ACTION:[cisco][asa][outcome]} %{WORD:[cisco][asa][network][transport]} %{DATA:[observer][ingress][interface][name]}/%{IP:[source][ip]}\(%{INT:[source][port]:int}\)(?:\(%{DATA:[source][user][name]}\))? -> %{DATA:[observer][egress][interface][name]}/%{IP:[destination][ip]}\(%{INT:[destination][port]:int}\)(?:\(%{DATA:[source][user][name]}\))? hit-cnt %{INT:[cisco][asa][hit_count]:int} %{CISCO_INTERVAL} \[%{DATA:[@metadata][cisco][asa][hashcode1]}, %{DATA:[@metadata][cisco][asa][hashcode2]}\]
# ASA-5-304001
CISCOFW304001 %{IP:[source][ip]}(?:\(%{DATA:[source][user][name]}\))? Accessed URL %{IP:[destination][ip]}:%{GREEDYDATA:[url][original]}
# ASA-6-110002
CISCOFW110002 %{CISCO_REASON:[event][reason]} for %{WORD:[cisco][asa][network][transport]} from %{DATA:[observer][ingress][interface][name]}:%{IP:[source][ip]}/%{INT:[source][port]:int} to %{IP:[destination][ip]}/%{INT:[destination][port]:int}
# ASA-6-302010
CISCOFW302010 %{INT:[cisco][asa][connections][in_use]:int} in use, %{INT:[cisco][asa][connections][most_used]:int} most used
# ASA-6-302013, ASA-6-302014, ASA-6-302015, ASA-6-302016
CISCOFW302013_302014_302015_302016 %{CISCO_ACTION:[cisco][asa][outcome]}(?: %{CISCO_DIRECTION:[cisco][asa][network][direction]})? %{WORD:[cisco][asa][network][transport]} connection %{INT:[cisco][asa][connection_id]} for %{NOTSPACE:[observer][ingress][interface][name]}:%{IP:[source][ip]}/%{INT:[source][port]:int}(?: \(%{IP:[source][nat][ip]}/%{INT:[source][nat][port]:int}\))?(?:\(%{DATA:[source][user][name]}\))? to %{NOTSPACE:[observer][egress][interface][name]}:%{IP:[destination][ip]}/%{INT:[destination][port]:int}( \(%{IP:[destination][nat][ip]}/%{INT:[destination][nat][port]:int}\))?(?:\(%{DATA:[destination][user][name]}\))?( duration %{TIME:[cisco][asa][duration]} bytes %{INT:[network][bytes]:int})?(?: %{CISCO_REASON:[event][reason]})?(?: \(%{DATA:[user][name]}\))?
# :long - %{INT:[network][bytes]:int}
# ASA-6-302020, ASA-6-302021
CISCOFW302020_302021 %{CISCO_ACTION:[cisco][asa][outcome]}(?: %{CISCO_DIRECTION:[cisco][asa][network][direction]})? %{WORD:[cisco][asa][network][transport]} connection for faddr %{IP:[destination][ip]}/%{INT:[cisco][asa][icmp_seq]:int}(?:\(%{DATA:[destination][user][name]}\))? gaddr %{IP:[source][nat][ip]}/%{INT:[cisco][asa][icmp_type]:int} laddr %{IP:[source][ip]}/%{INT}(?: \(%{DATA:[source][user][name]}\))?
# ASA-6-305011
CISCOFW305011 %{CISCO_ACTION:[cisco][asa][outcome]} %{CISCO_XLATE_TYPE} %{WORD:[cisco][asa][network][transport]} translation from %{DATA:[observer][ingress][interface][name]}:%{IP:[source][ip]}(/%{INT:[source][port]:int})?(?:\(%{DATA:[source][user][name]}\))? to %{DATA:[observer][egress][interface][name]}:%{IP:[destination][ip]}/%{INT:[destination][port]:int}
# ASA-3-313001, ASA-3-313004, ASA-3-313008
CISCOFW313001_313004_313008 %{CISCO_ACTION:[cisco][asa][outcome]} %{WORD:[cisco][asa][network][transport]} type=%{INT:[cisco][asa][icmp_type]:int}, code=%{INT:[cisco][asa][icmp_code]:int} from %{IP:[source][ip]} on interface %{NOTSPACE:[observer][egress][interface][name]}(?: to %{IP:[destination][ip]})?
# ASA-4-313005
CISCOFW313005 %{CISCO_REASON:[event][reason]} for %{WORD:[cisco][asa][network][transport]} error message: %{WORD} src %{CISCO_SRC_IP_USER} dst %{CISCO_DST_IP_USER} \(type %{INT:[cisco][asa][icmp_type]:int}, code %{INT:[cisco][asa][icmp_code]:int}\) on %{NOTSPACE} interface\.\s+Original IP payload: %{WORD:[cisco][asa][original_ip_payload][network][transport]} src %{IP:[cisco][asa][original_ip_payload][source][ip]}/%{INT:[cisco][asa][original_ip_payload][source][port]:int}(?:\(%{DATA:[cisco][asa][original_ip_payload][source][user][name]}\))? dst %{IP:[cisco][asa][original_ip_payload][destination][ip]}/%{INT:[cisco][asa][original_ip_payload][destination][port]:int}(?:\(%{DATA:[cisco][asa][original_ip_payload][destination][user][name]}\))?
# ASA-5-321001
CISCOFW321001 Resource '%{DATA:[cisco][asa][resource][name]}' limit of %{POSINT:[cisco][asa][resource][limit]:int} reached for system
# ASA-4-402117
CISCOFW402117 %{WORD:[cisco][asa][network][type]}: Received a non-IPSec packet \(protocol=\s?%{WORD:[cisco][asa][network][transport]}\) from %{IP:[source][ip]} to %{IP:[destination][ip]}\.?
# ASA-4-402119
CISCOFW402119 %{WORD:[cisco][asa][network][type]}: Received an %{WORD:[cisco][asa][ipsec][protocol]} packet \(SPI=\s?%{DATA:[cisco][asa][ipsec][spi]}, sequence number=\s?%{DATA:[cisco][asa][ipsec][seq_num]}\) from %{IP:[source][ip]} \(user=\s?%{DATA:[source][user][name]}\) to %{IP:[destination][ip]} that failed anti-replay checking\.?
# ASA-4-419001
CISCOFW419001 %{CISCO_ACTION:[cisco][asa][outcome]} %{WORD:[cisco][asa][network][transport]} packet from %{NOTSPACE:[observer][ingress][interface][name]}:%{IP:[source][ip]}/%{INT:[source][port]:int} to %{NOTSPACE:[observer][egress][interface][name]}:%{IP:[destination][ip]}/%{INT:[destination][port]:int}, reason: %{GREEDYDATA:[event][reason]}
# ASA-4-419002
CISCOFW419002 %{CISCO_REASON:[event][reason]} from %{DATA:[observer][ingress][interface][name]}:%{IP:[source][ip]}/%{INT:[source][port]:int} to %{DATA:[observer][egress][interface][name]}:%{IP:[destination][ip]}/%{INT:[destination][port]:int} with different initial sequence number
# ASA-4-500004
CISCOFW500004 %{CISCO_REASON:[event][reason]} for protocol=%{WORD:[cisco][asa][network][transport]}, from %{IP:[source][ip]}/%{INT:[source][port]:int} to %{IP:[destination][ip]}/%{INT:[destination][port]:int}
# ASA-6-602303, ASA-6-602304
CISCOFW602303_602304 %{WORD:[cisco][asa][network][type]}: An %{CISCO_DIRECTION:[cisco][asa][network][direction]} %{DATA:[cisco][asa][ipsec][tunnel_type]} SA \(SPI=\s?%{DATA:[cisco][asa][ipsec][spi]}\) between %{IP:[source][ip]} and %{IP:[destination][ip]} \(user=\s?%{DATA:[source][user][name]}\) has been %{CISCO_ACTION:[cisco][asa][outcome]}
# ASA-7-710001, ASA-7-710002, ASA-7-710003, ASA-7-710005, ASA-7-710006
CISCOFW710001_710002_710003_710005_710006 %{WORD:[cisco][asa][network][transport]} (?:request|access) %{CISCO_ACTION:[cisco][asa][outcome]} from %{IP:[source][ip]}/%{INT:[source][port]:int} to %{DATA:[observer][egress][interface][name]}:%{IP:[destination][ip]}/%{INT:[destination][port]:int}
# ASA-6-713172
CISCOFW713172 Group = %{DATA:[cisco][asa][source][group]}, IP = %{IP:[source][ip]}, Automatic NAT Detection Status:\s+Remote end\s*%{DATA:[@metadata][cisco][asa][remote_nat]}\s*behind a NAT device\s+This\s+end\s*%{DATA:[@metadata][cisco][asa][local_nat]}\s*behind a NAT device
# ASA-4-733100
CISCOFW733100 \[\s*%{DATA:[cisco][asa][burst][object]}\s*\] drop %{DATA:[cisco][asa][burst][id]} exceeded. Current burst rate is %{INT:[cisco][asa][burst][current_rate]:int} per second, max configured rate is %{INT:[cisco][asa][burst][configured_rate]:int}; Current average rate is %{INT:[cisco][asa][burst][avg_rate]:int} per second, max configured rate is %{INT:[cisco][asa][burst][configured_avg_rate]:int}; Cumulative total count is %{INT:[cisco][asa][burst][cumulative_count]:int}
#== End Cisco ASA ==


IPTABLES_TCP_FLAGS (CWR |ECE |URG |ACK |PSH |RST |SYN |FIN )*
IPTABLES_TCP_PART (?:SEQ=%{INT:[iptables][tcp][seq]:int}\s+)?(?:ACK=%{INT:[iptables][tcp][ack]:int}\s+)?WINDOW=%{INT:[iptables][tcp][window]:int}\s+RES=0x%{BASE16NUM:[iptables][tcp_reserved_bits]}\s+%{IPTABLES_TCP_FLAGS:[iptables][tcp][flags]}

IPTABLES4_FRAG (?:(?<= )(?:CE|DF|MF))*
IPTABLES4_PART SRC=%{IPV4:[source][ip]}\s+DST=%{IPV4:[destination][ip]}\s+LEN=(?:%{INT:[iptables][length]:int})?\s+TOS=(?:0|0x%{BASE16NUM:[iptables][tos]})?\s+PREC=(?:0x%{BASE16NUM:[iptables][precedence_bits]})?\s+TTL=(?:%{INT:[iptables][ttl]:int})?\s+ID=(?:%{INT:[iptables][id]})?\s+(?:%{IPTABLES4_FRAG:[iptables][fragment_flags]})?(?:\s+FRAG: %{INT:[iptables][fragment_offset]:int})?
IPTABLES6_PART SRC=%{IPV6:[source][ip]}\s+DST=%{IPV6:[destination][ip]}\s+LEN=(?:%{INT:[iptables][length]:int})?\s+TC=(?:0|0x%{BASE16NUM:[iptables][tos]})?\s+HOPLIMIT=(?:%{INT:[iptables][ttl]:int})?\s+FLOWLBL=(?:%{INT:[iptables][flow_label]})?

IPTABLES IN=(?:%{NOTSPACE:[observer][ingress][interface][name]})?\s+OUT=(?:%{NOTSPACE:[observer][egress][interface][name]})?\s+(?:MAC=(?:%{COMMONMAC:[destination][mac]})?(?::%{COMMONMAC:[source][mac]})?(?::[A-Fa-f0-9]{2}:[A-Fa-f0-9]{2})?\s+)?(:?%{IPTABLES4_PART}|%{IPTABLES6_PART}).*?PROTO=(?:%{WORD:[network][transport]})?\s+SPT=(?:%{INT:[source][port]:int})?\s+DPT=(?:%{INT:[destination][port]:int})?\s+(?:%{IPTABLES_TCP_PART})?

# Shorewall firewall logs
SHOREWALL (?:%{SYSLOGTIMESTAMP:timestamp}) (?:%{WORD:[observer][hostname]}) .*Shorewall:(?:%{WORD:[shorewall][firewall][type]})?:(?:%{WORD:[shorewall][firewall][action]})?.*%{IPTABLES}
#== End Shorewall
#== SuSE Firewall 2 ==
SFW2_LOG_PREFIX SFW2\-INext\-%{NOTSPACE:[suse][firewall][action]}
SFW2 ((?:%{SYSLOGTIMESTAMP:timestamp})|(?:%{TIMESTAMP_ISO8601:timestamp}))\s*%{HOSTNAME:[observer][hostname]}.*?%{SFW2_LOG_PREFIX:[suse][firewall][log_prefix]}\s*%{IPTABLES}
#== End SuSE ==
)firewalls",

// haproxy
R"haproxy(
HAPROXYTIME (?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])
HAPROXYDATE %{MONTHDAY}/%{MONTH}/%{YEAR}:%{HAPROXYTIME}.%{INT}

# Override these default patterns to parse out what is captured in your haproxy.cfg
HAPROXYCAPTUREDREQUESTHEADERS %{DATA:[haproxy][http][request][captured_headers]}
HAPROXYCAPTUREDRESPONSEHEADERS %{DATA:[haproxy][http][response][captured_headers]}

# Example:
#  These haproxy config lines will add data to the logs that are captured
#  by the patterns below. Place them in your custom patterns directory to
#  override the defaults.
#
#  capture request header Host len 40
#  capture request header X-Forwarded-For len 50
#  capture request header Accept-Language len 50
#  capture request header Referer len 200
#  capture request header User-Agent len 200
#
#  capture response header Content-Type len 30
#  capture response header Content-Encoding len 10
#  capture response header Cache-Control len 200
#  capture response header Last-Modified len 200
#
# HAPROXYCAPTUREDREQUESTHEADERS %{DATA:[haproxy][http][request][host]}\|%{DATA:[haproxy][http][request][x_forwarded_for]}\|%{DATA:[haproxy][http][request][accept_language]}\|%{DATA:[http][request][referrer]}\|%{DATA:[user_agent][original]}
# HAPROXYCAPTUREDRESPONSEHEADERS %{DATA:[http][response][mime_type]}\|%{DATA:[haproxy][http][response][encoding]}\|%{DATA:[haproxy][http][response][cache_control]}\|%{DATA:[haproxy][http][response][last_modified]}

HAPROXYURI (?:%{URIPROTO:[url][scheme]}://)?(?:%{USER:[url][username]}(?::[^@]*)?@)?(?:%{IPORHOST:[url][domain]}(?::%{POSINT:[url][port]:int})?)?(?:%{URIPATH:[url][path]}(?:\?%{URIQUERY:[url][query]})?)?

HAPROXYHTTPREQUESTLINE (?:<BADREQ>|(?:%{WORD:[http][request][method]} %{HAPROXYURI:[url][original]}(?: HTTP/%{NUMBER:[http][version]})?))

# parse a haproxy 'httplog' line
HAPROXYHTTPBASE %{IP:[source][address]}:%{INT:[source][port]:int} \[%{HAPROXYDATE:[haproxy][request_date]}\] %{NOTSPACE:[haproxy][frontend_name]} %{NOTSPACE:[haproxy][backend_name]}/(?:<NOSRV>|%{NOTSPACE:[haproxy][server_name]}) (?:-1|%{INT:[haproxy][http][request][time_wait_ms]:int})/(?:-1|%{INT:[haproxy][total_waiting_time_ms]:int})/(?:-1|%{INT:[haproxy][connection_wait_time_ms]:int})/(?:-1|%{INT:[haproxy][http][request][time_wait_without_data_ms]:int})/%{NOTSPACE:[haproxy][total_time_ms]} %{INT:[http][response][status_code]:int} %{INT:[source][bytes]:int} (?:-|%{DATA:[haproxy][http][request][captured_cookie]}) (?:-|%{DATA:[haproxy][http][response][captured_cookie]}) %{NOTSPACE:[haproxy][termination_state]} %{INT:[haproxy][connections][active]:int}/%{INT:[haproxy][connections][frontend]:int}/%{INT:[haproxy][connections][backend]:int}/%{INT:[haproxy][connections][server]:int}/%{INT:[haproxy][connections][retries]:int} %{INT:[haproxy][server_queue]:int}/%{INT:[haproxy][backend_queue]:int}(?: \{%{HAPROXYCAPTUREDREQUESTHEADERS}\}(?: \{%{HAPROXYCAPTUREDRESPONSEHEADERS}\})?)?(?: "%{HAPROXYHTTPREQUESTLINE}"?)?
# :long - %{INT:[source][bytes]:int}

HAPROXYHTTP (?:%{SYSLOGTIMESTAMP:timestamp}|%{TIMESTAMP_ISO8601:timestamp}) %{IPORHOST:[host][hostname]} %{SYSLOGPROG}: %{HAPROXYHTTPBASE}

# parse a haproxy 'tcplog' line
HAPROXYTCP (?:%{SYSLOGTIMESTAMP:timestamp}|%{TIMESTAMP_ISO8601:timestamp}) %{IPORHOST:[host][hostname]} %{SYSLOGPROG}: %{IP:[source][address]}:%{INT:[source][port]:int} \[%{HAPROXYDATE:[haproxy][request_date]}\] %{NOTSPACE:[haproxy][frontend_name]} %{NOTSPACE:[haproxy][backend_name]}/(?:<NOSRV>|%{NOTSPACE:[haproxy][server_name]}) (?:-1|%{INT:[haproxy][total_waiting_time_ms]:int})/(?:-1|%{INT:[haproxy][connection_wait_time_ms]:int})/%{NOTSPACE:[haproxy][total_time_ms]} %{INT:[source][bytes]:int} %{NOTSPACE:[haproxy][termination_state]} %{INT:[haproxy][connections][active]:int}/%{INT:[haproxy][connections][frontend]:int}/%{INT:[haproxy][connections][backend]:int}/%{INT:[haproxy][connections][server]:int}/%{INT:[haproxy][connections][retries]:int} %{INT:[haproxy][server_queue]:int}/%{INT:[haproxy][backend_queue]:int}
# :long - %{INT:[source][bytes]:int}
)haproxy",

// httpd
R"httpd-patterns(
HTTPDUSER %{EMAILADDRESS}|%{USER}
HTTPDERROR_DATE %{DAY} %{MONTH} %{MONTHDAY} %{TIME} %{YEAR}

# Log formats
HTTPD_COMMONLOG %{IPORHOST:[source][address]} (?:-|%{HTTPDUSER:[apache][access][user][identity]}) (?:-|%{HTTPDUSER:[user][name]}) \[%{HTTPDATE:timestamp}\] "(?:%{WORD:[http][request][method]} %{NOTSPACE:[url][original]}(?: HTTP/%{NUMBER:[http][version]})?|%{DATA})" (?:-|%{INT:[http][response][status_code]:int}) (?:-|%{INT:[http][response][body][bytes]:int})
# :long - %{INT:[http][response][body][bytes]:int}
HTTPD_COMBINEDLOG %{HTTPD_COMMONLOG} "(?:-|%{DATA:[http][request][referrer]})" "(?:-|%{DATA:[user_agent][original]})"

# Error logs
HTTPD20_ERRORLOG \[%{HTTPDERROR_DATE:timestamp}\] \[%{LOGLEVEL:[log][level]}\] (?:\[client %{IPORHOST:[source][address]}\] )?%{GREEDYDATA:message}
HTTPD24_ERRORLOG \[%{HTTPDERROR_DATE:timestamp}\] \[(?:%{WORD:[apache][error][module]})?:%{LOGLEVEL:[log][level]}\] \[pid %{POSINT:[process][pid]:int}(:tid %{INT:[process][thread][id]:int})?\](?: \(%{POSINT:[apache][error][proxy][error][code]?}\)%{DATA:[apache][error][proxy][error][message]}:)?(?: \[client %{IPORHOST:[source][address]}(?::%{POSINT:[source][port]:int})?\])?(?: %{DATA:[error][code]}:)? %{GREEDYDATA:message}
# :long - %{INT:[process][thread][id]:int}
HTTPD_ERRORLOG %{HTTPD20_ERRORLOG}|%{HTTPD24_ERRORLOG}

# Deprecated
COMMONAPACHELOG %{HTTPD_COMMONLOG}
COMBINEDAPACHELOG %{HTTPD_COMBINEDLOG}
)httpd-patterns",

// java
R"java-patterns(
JAVACLASS (?:[a-zA-Z$_][a-zA-Z$_0-9]*\.)*[a-zA-Z$_][a-zA-Z$_0-9]*
#Space is an allowed character to match special cases like 'Native Method' or 'Unknown Source'
JAVAFILE (?:[a-zA-Z$_0-9. -]+)
#Allow special <init>, <clinit> methods
JAVAMETHOD (?:(<(?:cl)?init>)|[a-zA-Z$_][a-zA-Z$_0-9]*)
#Line number is optional in special cases 'Native method' or 'Unknown source'
JAVASTACKTRACEPART %{SPACE}at %{JAVACLASS:[java][log][origin][class][name]}\.%{JAVAMETHOD:[log][origin][function]}\(%{JAVAFILE:[log][origin][file][name]}(?::%{INT:[log][origin][file][line]:int})?\)
# Java Logs
JAVATHREAD (?:[A-Z]{2}-Processor[\d]+)
JAVALOGMESSAGE (?:.*)

# MMM dd, yyyy HH:mm:ss eg: Jan 9, 2014 7:13:13 AM
# matches default logging configuration in Tomcat 4.1, 5.0, 5.5, 6.0, 7.0
CATALINA7_DATESTAMP %{MONTH} %{MONTHDAY}, %{YEAR} %{HOUR}:%{MINUTE}:%{SECOND} (?:AM|PM)
CATALINA7_LOG %{CATALINA7_DATESTAMP:timestamp} %{JAVACLASS:[java][log][origin][class][name]}(?: %{JAVAMETHOD:[log][origin][function]})?\s*(?:%{LOGLEVEL:[log][level]}:)? %{JAVALOGMESSAGE:message}

# 31-Jul-2020 16:40:38.578 in Tomcat 8.5/9.0
CATALINA8_DATESTAMP %{MONTHDAY}-%{MONTH}-%{YEAR} %{HOUR}:%{MINUTE}:%{SECOND}
CATALINA8_LOG %{CATALINA8_DATESTAMP:timestamp} %{LOGLEVEL:[log][level]} \[%{DATA:[java][log][origin][thread][name]}\] %{JAVACLASS:[java][log][origin][class][name]}\.(?:%{JAVAMETHOD:[log][origin][function]})? %{JAVALOGMESSAGE:message}

CATALINA_DATESTAMP (?:%{CATALINA8_DATESTAMP})|(?:%{CATALINA7_DATESTAMP})
CATALINALOG (?:%{CATALINA8_LOG})|(?:%{CATALINA7_LOG})

# in Tomcat 5.5, 6.0, 7.0 it is the same as catalina.out logging format
TOMCAT7_LOG %{CATALINA7_LOG}
TOMCAT8_LOG %{CATALINA8_LOG}

# NOTE: a weird log we started with - not sure what TC version this should match out of the box (due the | delimiters)
TOMCATLEGACY_DATESTAMP %{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:%{MINUTE}:%{SECOND}(?: %{ISO8601_TIMEZONE})?
TOMCATLEGACY_LOG %{TOMCATLEGACY_DATESTAMP:timestamp} \| %{LOGLEVEL:[log][level]} \| %{JAVACLASS:[java][log][origin][class][name]} - %{JAVALOGMESSAGE:message}

TOMCAT_DATESTAMP (?:%{CATALINA8_DATESTAMP})|(?:%{CATALINA7_DATESTAMP})|(?:%{TOMCATLEGACY_DATESTAMP})

TOMCATLOG (?:%{TOMCAT8_LOG})|(?:%{TOMCAT7_LOG})|(?:%{TOMCATLEGACY_LOG})
)java-patterns",

// junos
R"junos-patterns(
# JUNOS 11.4 RT_FLOW patterns
RT_FLOW_TAG (?:RT_FLOW_SESSION_CREATE|RT_FLOW_SESSION_CLOSE|RT_FLOW_SESSION_DENY)
# deprecated legacy name:
RT_FLOW_EVENT RT_FLOW_TAG

RT_FLOW1 %{RT_FLOW_TAG:[juniper][srx][tag]}: %{GREEDYDATA:[juniper][srx][reason]}: %{IP:[source][ip]}/%{INT:[source][port]:int}->%{IP:[destination][ip]}/%{INT:[destination][port]:int} %{DATA:[juniper][srx][service_name]} %{IP:[source][nat][ip]}/%{INT:[source][nat][port]:int}->%{IP:[destination][nat][ip]}/%{INT:[destination][nat][port]:int} (?:(?:None)|(?:%{DATA:[juniper][srx][src_nat_rule_name]})) (?:(?:None)|(?:%{DATA:[juniper][srx][dst_nat_rule_name]})) %{INT:[network][iana_number]} %{DATA:[rule][name]} %{DATA:[observer][ingress][zone]} %{DATA:[observer][egress][zone]} %{INT:[juniper][srx][session_id]} \d+\(%{INT:[source][bytes]:int}\) \d+\(%{INT:[destination][bytes]:int}\) %{INT:[juniper][srx][elapsed_time]:int} .*
# :long - %{INT:[source][bytes]:int}
# :long - %{INT:[destination][bytes]:int}

RT_FLOW2 %{RT_FLOW_TAG:[juniper][srx][tag]}: session created %{IP:[source][ip]}/%{INT:[source][port]:int}->%{IP:[destination][ip]}/%{INT:[destination][port]:int} %{DATA:[juniper][srx][service_name]} %{IP:[source][nat][ip]}/%{INT:[source][nat][port]:int}->%{IP:[destination][nat][ip]}/%{INT:[destination][nat][port]:int} (?:(?:None)|(?:%{DATA:[juniper][srx][src_nat_rule_name]})) (?:(?:None)|(?:%{DATA:[juniper][srx][dst_nat_rule_name]})) %{INT:[network][iana_number]} %{DATA:[rule][name]} %{DATA:[observer][ingress][zone]} %{DATA:[observer][egress][zone]} %{INT:[juniper][srx][session_id]} .*

RT_FLOW3 %{RT_FLOW_TAG:[juniper][srx][tag]}: session denied %{IP:[source][ip]}/%{INT:[source][port]:int}->%{IP:[destination][ip]}/%{INT:[destination][port]:int} %{DATA:[juniper][srx][service_name]} %{INT:[network][iana_number]}\(\d\) %{DATA:[rule][name]} %{DATA:[observer][ingress][zone]} %{DATA:[observer][egress][zone]} .*
)junos-patterns",

// linux-syslog
R"linux-syslog(
SYSLOG5424PRINTASCII [!-~]+

SYSLOGBASE2 (?:%{SYSLOGTIMESTAMP:timestamp}|%{TIMESTAMP_ISO8601:timestamp})(?: %{SYSLOGFACILITY})?(?: %{SYSLOGHOST:[host][hostname]})?(?: %{SYSLOGPROG}:)?
SYSLOGPAMSESSION %{SYSLOGBASE} (?=%{GREEDYDATA:message})%{WORD:[system][auth][pam][module]}\(%{DATA:[system][auth][pam][origin]}\): session %{WORD:[system][auth][pam][session_state]} for user %{USERNAME:[user][name]}(?: by %{GREEDYDATA})?

CRON_ACTION [A-Z ]+
CRONLOG %{SYSLOGBASE} \(%{USER:[user][name]}\) %{CRON_ACTION:[system][cron][action]} \(%{DATA:message}\)

SYSLOGLINE %{SYSLOGBASE2} %{GREEDYDATA:message}

# IETF 5424 syslog(8) format (see http://www.rfc-editor.org/info/rfc5424)
SYSLOG5424PRI <%{NONNEGINT:[log][syslog][priority]:int}>
SYSLOG5424SD \[%{DATA}\]+
SYSLOG5424BASE %{SYSLOG5424PRI}%{NONNEGINT:[system][syslog][version]} +(?:-|%{TIMESTAMP_ISO8601:timestamp}) +(?:-|%{IPORHOST:[host][hostname]}) +(?:-|%{SYSLOG5424PRINTASCII:[process][name]}) +(?:-|%{POSINT:[process][pid]:int}) +(?:-|%{SYSLOG5424PRINTASCII:[event][code]}) +(?:-|%{SYSLOG5424SD:[system][syslog][structured_data]})?

SYSLOG5424LINE %{SYSLOG5424BASE} +%{GREEDYDATA:message}
)linux-syslog",

// maven
R"maven-patterns(
MAVEN_VERSION (?:(\d+)\.)?(?:(\d+)\.)?(\*|\d+)(?:[.-](RELEASE|SNAPSHOT))?
)maven-patterns",

// mcollective
R"mcollective(
# Remember, these can be multi-line events.
MCOLLECTIVE ., \[%{TIMESTAMP_ISO8601:timestamp} #%{POSINT:[process][pid]:int}\]%{SPACE}%{LOGLEVEL:[log][level]}

MCOLLECTIVEAUDIT %{TIMESTAMP_ISO8601:timestamp}:
)mcollective",

// mongodb
R"mongodb-patterns(
MONGO_LOG %{SYSLOGTIMESTAMP:timestamp} \[%{WORD:[mongodb][component]}\] %{GREEDYDATA:message}
MONGO_QUERY \{ (?<={ ).*(?= } ntoreturn:) \}
MONGO_SLOWQUERY %{WORD:[mongodb][profile][op]} %{MONGO_WORDDASH:[mongodb][database]}\.%{MONGO_WORDDASH:[mongodb][collection]} %{WORD}: %{MONGO_QUERY:[mongodb][query][original]} ntoreturn:%{NONNEGINT:[mongodb][profile][ntoreturn]:int} ntoskip:%{NONNEGINT:[mongodb][profile][ntoskip]:int} nscanned:%{NONNEGINT:[mongodb][profile][nscanned]:int}.*? nreturned:%{NONNEGINT:[mongodb][profile][nreturned]:int}.*? %{INT:[mongodb][profile][duration]:int}ms
MONGO_WORDDASH \b[\w-]+\b
MONGO3_SEVERITY \w
MONGO3_COMPONENT %{WORD}
MONGO3_LOG %{TIMESTAMP_ISO8601:timestamp} %{MONGO3_SEVERITY:[log][level]} (?:-|%{MONGO3_COMPONENT:[mongodb][component]})%{SPACE}(?:\[%{DATA:[mongodb][context]}\])? %{GREEDYDATA:message}
)mongodb-patterns",

// nagios
R"nagios-patterns(
##################################################################################
##################################################################################
# Chop Nagios log files to smithereens!
#
# A set of GROK filters to process logfiles generated by Nagios.
# While it does not, this set intends to cover all possible Nagios logs.
#
# Some more work needs to be done to cover all External Commands:
#	http://old.nagios.org/developerinfo/externalcommands/commandlist.php
#
# If you need some support on these rules please contact:
#	Jelle Smet http://smetj.net
#
#################################################################################
#################################################################################

NAGIOSTIME \[%{NUMBER:timestamp}\]

###############################################
######## Begin nagios log types
###############################################
NAGIOS_TYPE_CURRENT_SERVICE_STATE CURRENT SERVICE STATE
NAGIOS_TYPE_CURRENT_HOST_STATE CURRENT HOST STATE

NAGIOS_TYPE_SERVICE_NOTIFICATION SERVICE NOTIFICATION
NAGIOS_TYPE_HOST_NOTIFICATION HOST NOTIFICATION

NAGIOS_TYPE_SERVICE_ALERT SERVICE ALERT
NAGIOS_TYPE_HOST_ALERT HOST ALERT

NAGIOS_TYPE_SERVICE_FLAPPING_ALERT SERVICE FLAPPING ALERT
NAGIOS_TYPE_HOST_FLAPPING_ALERT HOST FLAPPING ALERT

NAGIOS_TYPE_SERVICE_DOWNTIME_ALERT SERVICE DOWNTIME ALERT
NAGIOS_TYPE_HOST_DOWNTIME_ALERT HOST DOWNTIME ALERT

NAGIOS_TYPE_PASSIVE_SERVICE_CHECK PASSIVE SERVICE CHECK
NAGIOS_TYPE_PASSIVE_HOST_CHECK PASSIVE HOST CHECK

NAGIOS_TYPE_SERVICE_EVENT_HANDLER SERVICE EVENT HANDLER
NAGIOS_TYPE_HOST_EVENT_HANDLER HOST EVENT HANDLER

NAGIOS_TYPE_EXTERNAL_COMMAND EXTERNAL COMMAND
NAGIOS_TYPE_TIMEPERIOD_TRANSITION TIMEPERIOD TRANSITION
###############################################
######## End nagios log types
###############################################

###############################################
######## Begin external check types
###############################################
NAGIOS_EC_DISABLE_SVC_CHECK DISABLE_SVC_CHECK
NAGIOS_EC_ENABLE_SVC_CHECK ENABLE_SVC_CHECK
NAGIOS_EC_DISABLE_HOST_CHECK DISABLE_HOST_CHECK
NAGIOS_EC_ENABLE_HOST_CHECK ENABLE_HOST_CHECK
NAGIOS_EC_PROCESS_SERVICE_CHECK_RESULT PROCESS_SERVICE_CHECK_RESULT
NAGIOS_EC_PROCESS_HOST_CHECK_RESULT PROCESS_HOST_CHECK_RESULT
NAGIOS_EC_SCHEDULE_SERVICE_DOWNTIME SCHEDULE_SERVICE_DOWNTIME
NAGIOS_EC_SCHEDULE_HOST_DOWNTIME SCHEDULE_HOST_DOWNTIME
NAGIOS_EC_DISABLE_HOST_SVC_NOTIFICATIONS DISABLE_HOST_SVC_NOTIFICATIONS
NAGIOS_EC_ENABLE_HOST_SVC_NOTIFICATIONS ENABLE_HOST_SVC_NOTIFICATIONS
NAGIOS_EC_DISABLE_HOST_NOTIFICATIONS DISABLE_HOST_NOTIFICATIONS
NAGIOS_EC_ENABLE_HOST_NOTIFICATIONS ENABLE_HOST_NOTIFICATIONS
NAGIOS_EC_DISABLE_SVC_NOTIFICATIONS DISABLE_SVC_NOTIFICATIONS
NAGIOS_EC_ENABLE_SVC_NOTIFICATIONS ENABLE_SVC_NOTIFICATIONS
###############################################
######## End external check types
###############################################
NAGIOS_WARNING Warning:%{SPACE}%{GREEDYDATA:message}

NAGIOS_CURRENT_SERVICE_STATE %{NAGIOS_TYPE_CURRENT_SERVICE_STATE:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{DATA:[nagios][log][state_type]};%{INT:[nagios][log][attempt]:int};%{GREEDYDATA:message}
NAGIOS_CURRENT_HOST_STATE %{NAGIOS_TYPE_CURRENT_HOST_STATE:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][state]};%{DATA:[nagios][log][state_type]};%{INT:[nagios][log][attempt]:int};%{GREEDYDATA:message}

NAGIOS_SERVICE_NOTIFICATION %{NAGIOS_TYPE_SERVICE_NOTIFICATION:[nagios][log][type]}: %{DATA:[user][name]};%{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{DATA:[nagios][log][notification_command]};%{GREEDYDATA:message}
NAGIOS_HOST_NOTIFICATION %{NAGIOS_TYPE_HOST_NOTIFICATION:[nagios][log][type]}: %{DATA:[user][name]};%{DATA:[host][hostname]};%{DATA:[service][state]};%{DATA:[nagios][log][notification_command]};%{GREEDYDATA:message}

NAGIOS_SERVICE_ALERT %{NAGIOS_TYPE_SERVICE_ALERT:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{DATA:[nagios][log][state_type]};%{INT:[nagios][log][attempt]:int};%{GREEDYDATA:message}
NAGIOS_HOST_ALERT %{NAGIOS_TYPE_HOST_ALERT:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][state]};%{DATA:[nagios][log][state_type]};%{INT:[nagios][log][attempt]:int};%{GREEDYDATA:message}

NAGIOS_SERVICE_FLAPPING_ALERT %{NAGIOS_TYPE_SERVICE_FLAPPING_ALERT:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{GREEDYDATA:message}
NAGIOS_HOST_FLAPPING_ALERT %{NAGIOS_TYPE_HOST_FLAPPING_ALERT:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][state]};%{GREEDYDATA:message}

NAGIOS_SERVICE_DOWNTIME_ALERT %{NAGIOS_TYPE_SERVICE_DOWNTIME_ALERT:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{GREEDYDATA:[nagios][log][comment]}
NAGIOS_HOST_DOWNTIME_ALERT %{NAGIOS_TYPE_HOST_DOWNTIME_ALERT:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][state]};%{GREEDYDATA:[nagios][log][comment]}

NAGIOS_PASSIVE_SERVICE_CHECK %{NAGIOS_TYPE_PASSIVE_SERVICE_CHECK:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{GREEDYDATA:[nagios][log][comment]}
NAGIOS_PASSIVE_HOST_CHECK %{NAGIOS_TYPE_PASSIVE_HOST_CHECK:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][state]};%{GREEDYDATA:[nagios][log][comment]}

NAGIOS_SERVICE_EVENT_HANDLER %{NAGIOS_TYPE_SERVICE_EVENT_HANDLER:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{DATA:[nagios][log][state_type]};%{DATA:[nagios][log][event_handler_name]}
NAGIOS_HOST_EVENT_HANDLER %{NAGIOS_TYPE_HOST_EVENT_HANDLER:[nagios][log][type]}: %{DATA:[host][hostname]};%{DATA:[service][state]};%{DATA:[nagios][log][state_type]};%{DATA:[nagios][log][event_handler_name]}

NAGIOS_TIMEPERIOD_TRANSITION %{NAGIOS_TYPE_TIMEPERIOD_TRANSITION:[nagios][log][type]}: %{DATA:[service][name]};%{NUMBER:[nagios][log][period_from]:int};%{NUMBER:[nagios][log][period_to]:int}

####################
#### External checks
####################

#Disable host & service check
NAGIOS_EC_LINE_DISABLE_SVC_CHECK %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_DISABLE_SVC_CHECK:[nagios][log][command]};%{DATA:[host][hostname]};%{DATA:[service][name]}
NAGIOS_EC_LINE_DISABLE_HOST_CHECK %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_DISABLE_HOST_CHECK:[nagios][log][command]};%{DATA:[host][hostname]}

#Enable host & service check
NAGIOS_EC_LINE_ENABLE_SVC_CHECK %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_ENABLE_SVC_CHECK:[nagios][log][command]};%{DATA:[host][hostname]};%{DATA:[service][name]}
NAGIOS_EC_LINE_ENABLE_HOST_CHECK %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_ENABLE_HOST_CHECK:[nagios][log][command]};%{DATA:[host][hostname]}

#Process host & service check
NAGIOS_EC_LINE_PROCESS_SERVICE_CHECK_RESULT %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_PROCESS_SERVICE_CHECK_RESULT:[nagios][log][command]};%{DATA:[host][hostname]};%{DATA:[service][name]};%{DATA:[service][state]};%{GREEDYDATA:[nagios][log][check_result]}
NAGIOS_EC_LINE_PROCESS_HOST_CHECK_RESULT %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_PROCESS_HOST_CHECK_RESULT:[nagios][log][command]};%{DATA:[host][hostname]};%{DATA:[service][state]};%{GREEDYDATA:[nagios][log][check_result]}

#Disable host & service notifications
NAGIOS_EC_LINE_DISABLE_HOST_SVC_NOTIFICATIONS %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_DISABLE_HOST_SVC_NOTIFICATIONS:[nagios][log][command]};%{GREEDYDATA:[host][hostname]}
NAGIOS_EC_LINE_DISABLE_HOST_NOTIFICATIONS %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_DISABLE_HOST_NOTIFICATIONS:[nagios][log][command]};%{GREEDYDATA:[host][hostname]}
NAGIOS_EC_LINE_DISABLE_SVC_NOTIFICATIONS %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_DISABLE_SVC_NOTIFICATIONS:[nagios][log][command]};%{DATA:[host][hostname]};%{GREEDYDATA:[service][name]}

#Enable host & service notifications
NAGIOS_EC_LINE_ENABLE_HOST_SVC_NOTIFICATIONS %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_ENABLE_HOST_SVC_NOTIFICATIONS:[nagios][log][command]};%{GREEDYDATA:[host][hostname]}
NAGIOS_EC_LINE_ENABLE_HOST_NOTIFICATIONS %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_ENABLE_HOST_NOTIFICATIONS:[nagios][log][command]};%{GREEDYDATA:[host][hostname]}
NAGIOS_EC_LINE_ENABLE_SVC_NOTIFICATIONS %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_ENABLE_SVC_NOTIFICATIONS:[nagios][log][command]};%{DATA:[host][hostname]};%{GREEDYDATA:[service][name]}

#Schedule host & service downtime
NAGIOS_EC_LINE_SCHEDULE_HOST_DOWNTIME %{NAGIOS_TYPE_EXTERNAL_COMMAND:[nagios][log][type]}: %{NAGIOS_EC_SCHEDULE_HOST_DOWNTIME:[nagios][log][command]};%{DATA:[host][hostname]};%{NUMBER:[nagios][log][start_time]};%{NUMBER:[nagios][log][end_time]};%{NUMBER:[nagios][log][fixed]};%{NUMBER:[nagios][log][trigger_id]};%{NUMBER:[nagios][log][duration]:int};%{DATA:[user][name]};%{DATA:[nagios][log][comment]}

#End matching line
NAGIOSLOGLINE %{NAGIOSTIME} (?:%{NAGIOS_WARNING}|%{NAGIOS_CURRENT_SERVICE_STATE}|%{NAGIOS_CURRENT_HOST_STATE}|%{NAGIOS_SERVICE_NOTIFICATION}|%{NAGIOS_HOST_NOTIFICATION}|%{NAGIOS_SERVICE_ALERT}|%{NAGIOS_HOST_ALERT}|%{NAGIOS_SERVICE_FLAPPING_ALERT}|%{NAGIOS_HOST_FLAPPING_ALERT}|%{NAGIOS_SERVICE_DOWNTIME_ALERT}|%{NAGIOS_HOST_DOWNTIME_ALERT}|%{NAGIOS_PASSIVE_SERVICE_CHECK}|%{NAGIOS_PASSIVE_HOST_CHECK}|%{NAGIOS_SERVICE_EVENT_HANDLER}|%{NAGIOS_HOST_EVENT_HANDLER}|%{NAGIOS_TIMEPERIOD_TRANSITION}|%{NAGIOS_EC_LINE_DISABLE_SVC_CHECK}|%{NAGIOS_EC_LINE_ENABLE_SVC_CHECK}|%{NAGIOS_EC_LINE_DISABLE_HOST_CHECK}|%{NAGIOS_EC_LINE_ENABLE_HOST_CHECK}|%{NAGIOS_EC_LINE_PROCESS_HOST_CHECK_RESULT}|%{NAGIOS_EC_LINE_PROCESS_SERVICE_CHECK_RESULT}|%{NAGIOS_EC_LINE_SCHEDULE_HOST_DOWNTIME}|%{NAGIOS_EC_LINE_DISABLE_HOST_SVC_NOTIFICATIONS}|%{NAGIOS_EC_LINE_ENABLE_HOST_SVC_NOTIFICATIONS}|%{NAGIOS_EC_LINE_DISABLE_HOST_NOTIFICATIONS}|%{NAGIOS_EC_LINE_ENABLE_HOST_NOTIFICATIONS}|%{NAGIOS_EC_LINE_DISABLE_SVC_NOTIFICATIONS}|%{NAGIOS_EC_LINE_ENABLE_SVC_NOTIFICATIONS})
)nagios-patterns",

// postgresql
R"postgresql(
# Default postgresql pg_log format pattern
POSTGRESQL %{DATESTAMP:timestamp} %{TZ:[event][timezone]} %{DATA:[user][name]} %{GREEDYDATA:[postgresql][log][connection_id]} %{POSINT:[process][pid]:int}
)postgresql",

// rails
R"rails-patterns(
RUUID \h{32}
# rails controller with action
RCONTROLLER (?<[rails][controller][class]>[^#]+)#(?<[rails][controller][action]>\w+)

# this will often be the only line:
RAILS3HEAD (?m)Started %{WORD:[http][request][method]} "%{URIPATHPARAM:[url][original]}" for %{IPORHOST:[source][address]} at (?<timestamp>%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:%{MINUTE}:%{SECOND} %{ISO8601_TIMEZONE})
# for some a strange reason, params are stripped of {} - not sure that's a good idea.
RPROCESSING \W*Processing by %{RCONTROLLER} as (?<[rails][request][format]>\S+)(?:\W*Parameters: {%{DATA:[rails][request][params]}}\W*)?
RAILS3FOOT Completed %{POSINT:[http][response][status_code]:int}%{DATA} in %{NUMBER:[rails][request][duration][total]:float}ms %{RAILS3PROFILE}%{GREEDYDATA}
RAILS3PROFILE (?:\(Views: %{NUMBER:[rails][request][duration][view]:float}ms \| ActiveRecord: %{NUMBER:[rails][request][duration][active_record]:float}ms|\(ActiveRecord: %{NUMBER:[rails][request][duration][active_record]:float}ms)?

# putting it all together
RAILS3 %{RAILS3HEAD}(?:%{RPROCESSING})?(?<[rails][request][explain][original]>(?:%{DATA}\n)*)(?:%{RAILS3FOOT})?
)rails-patterns",

// redis
R"redis-patterns(
REDISTIMESTAMP %{MONTHDAY} %{MONTH} %{TIME}
REDISLOG \[%{POSINT:[process][pid]:int}\] %{REDISTIMESTAMP:timestamp} \*
REDISMONLOG %{NUMBER:timestamp} \[%{INT:[redis][database][id]} %{IP:[client][ip]}:%{POSINT:[client][port]:int}\] "%{WORD:[redis][command][name]}"\s?%{GREEDYDATA:[redis][command][args]}
)redis-patterns",

// ruby
R"ruby-patterns(
RUBY_LOGLEVEL (?:DEBUG|FATAL|ERROR|WARN|INFO)
RUBY_LOGGER [DFEWI], \[%{TIMESTAMP_ISO8601:timestamp} #%{POSINT:[process][pid]:int}\] *%{RUBY_LOGLEVEL:[log][level]} -- +%{DATA:[process][name]}: %{GREEDYDATA:message}
)ruby-patterns",

// squid
R"squid-patterns(
# Pattern squid3
# Documentation of squid3 logs formats can be found at the following link:
# http://wiki.squid-cache.org/Features/LogFormat
SQUID3_STATUS (?:%{POSINT:[http][response][status_code]:int}|0|000)
SQUID3 %{NUMBER:timestamp}\s+%{NUMBER:[squid][request][duration]:int}\s%{IP:[source][ip]}\s%{WORD:[event][action]}/%{SQUID3_STATUS}\s%{INT:[http][response][bytes]:int}\s%{WORD:[http][request][method]}\s%{NOTSPACE:[url][original]}\s(?:-|%{NOTSPACE:[user][name]})\s%{WORD:[squid][hierarchy_code]}/(?:-|%{IPORHOST:[destination][address]})\s(?:-|%{NOTSPACE:[http][response][mime_type]})
# :long - %{INT:[http][response][bytes]:int}
)squid-patterns",

// zeek
R"zeek-patterns(
# updated Zeek log matching, for legacy matching see the patters/ecs-v1/bro

ZEEK_BOOL [TF]
ZEEK_DATA [^\t]+

# http.log - the 'new' format (compared to BRO_HTTP)
# has *version* and *origin* fields added and *filename* replaced with *orig_filenames* + *resp_filenames*
ZEEK_HTTP %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{INT:[zeek][http][trans_depth]:int}\t(?:-|%{WORD:[http][request][method]})\t(?:-|%{ZEEK_DATA:[url][domain]})\t(?:-|%{ZEEK_DATA:[url][original]})\t(?:-|%{ZEEK_DATA:[http][request][referrer]})\t(?:-|%{NUMBER:[http][version]})\t(?:-|%{ZEEK_DATA:[user_agent][original]})\t(?:-|%{ZEEK_DATA:[zeek][http][origin]})\t(?:-|%{NUMBER:[http][request][body][bytes]:int})\t(?:-|%{NUMBER:[http][response][body][bytes]:int})\t(?:-|%{POSINT:[http][response][status_code]:int})\t(?:-|%{DATA:[zeek][http][status_msg]})\t(?:-|%{POSINT:[zeek][http][info_code]:int})\t(?:-|%{DATA:[zeek][http][info_msg]})\t(?:\(empty\)|%{ZEEK_DATA:[zeek][http][tags]})\t(?:-|%{ZEEK_DATA:[url][username]})\t(?:-|%{ZEEK_DATA:[url][password]})\t(?:-|%{ZEEK_DATA:[zeek][http][proxied]})\t(?:-|%{ZEEK_DATA:[zeek][http][orig_fuids]})\t(?:-|%{ZEEK_DATA:[zeek][http][orig_filenames]})\t(?:-|%{ZEEK_DATA:[http][request][mime_type]})\t(?:-|%{ZEEK_DATA:[zeek][http][resp_fuids]})\t(?:-|%{ZEEK_DATA:[zeek][http][resp_filenames]})\t(?:-|%{ZEEK_DATA:[http][response][mime_type]})
# :long - %{NUMBER:[http][request][body][bytes]:int}
# :long - %{NUMBER:[http][response][body][bytes]:int}

# dns.log - 'updated' BRO_DNS format (added *zeek.dns.rtt*)
ZEEK_DNS %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{WORD:[network][transport]}\t(?:-|%{INT:[dns][id]:int})\t(?:-|%{NUMBER:[zeek][dns][rtt]:float})\t(?:-|%{ZEEK_DATA:[dns][question][name]})\t(?:-|%{INT:[zeek][dns][qclass]:int})\t(?:-|%{ZEEK_DATA:[zeek][dns][qclass_name]})\t(?:-|%{INT:[zeek][dns][qtype]:int})\t(?:-|%{ZEEK_DATA:[dns][question][type]})\t(?:-|%{INT:[zeek][dns][rcode]:int})\t(?:-|%{ZEEK_DATA:[dns][response_code]})\t%{ZEEK_BOOL:[zeek][dns][AA]}\t%{ZEEK_BOOL:[zeek][dns][TC]}\t%{ZEEK_BOOL:[zeek][dns][RD]}\t%{ZEEK_BOOL:[zeek][dns][RA]}\t%{NONNEGINT:[zeek][dns][Z]:int}\t(?:-|%{ZEEK_DATA:[zeek][dns][answers]})\t(?:-|%{DATA:[zeek][dns][TTLs]})\t(?:-|%{ZEEK_BOOL:[zeek][dns][rejected]})

# conn.log - the 'new' format (requires *zeek.connection.local_resp*, handles `(empty)` as `-` for tunnel_parents, and optional mac adresses)
ZEEK_CONN %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{WORD:[network][transport]}\t(?:-|%{ZEEK_DATA:[network][protocol]})\t(?:-|%{NUMBER:[zeek][connection][duration]:float})\t(?:-|%{INT:[zeek][connection][orig_bytes]:int})\t(?:-|%{INT:[zeek][connection][resp_bytes]:int})\t(?:-|%{ZEEK_DATA:[zeek][connection][state]})\t(?:-|%{ZEEK_BOOL:[zeek][connection][local_orig]})\t(?:-|%{ZEEK_BOOL:[zeek][connection][local_resp]})\t(?:-|%{INT:[zeek][connection][missed_bytes]:int})\t(?:-|%{ZEEK_DATA:[zeek][connection][history]})\t(?:-|%{INT:[source][packets]:int})\t(?:-|%{INT:[source][bytes]:int})\t(?:-|%{INT:[destination][packets]:int})\t(?:-|%{INT:[destination][bytes]:int})\t(?:-|%{ZEEK_DATA:[zeek][connection][tunnel_parents]})(?:\t(?:-|%{COMMONMAC:[source][mac]})\t(?:-|%{COMMONMAC:[destination][mac]}))?
# :long - %{INT:[zeek][connection][orig_bytes]:int}
# :long - %{INT:[zeek][connection][resp_bytes]:int}
# :long - %{INT:[zeek][connection][missed_bytes]:int}
# :long - %{INT:[source][packets]:int}
# :long - %{INT:[source][bytes]:int}
# :long - %{INT:[destination][packets]:int}
# :long - %{INT:[destination][bytes]:int}

# files.log - updated BRO_FILES format (2 new fields added at the end)
ZEEK_FILES_TX_HOSTS (?:-|%{IP:[server][ip]})|(?<[zeek][files][tx_hosts]>%{IP:[server][ip]}(?:[\s,]%{IP})+)
ZEEK_FILES_RX_HOSTS (?:-|%{IP:[client][ip]})|(?<[zeek][files][rx_hosts]>%{IP:[client][ip]}(?:[\s,]%{IP})+)
ZEEK_FILES %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][files][fuid]}\t%{ZEEK_FILES_TX_HOSTS}\t%{ZEEK_FILES_RX_HOSTS}\t(?:-|%{ZEEK_DATA:[zeek][files][session_ids]})\t(?:-|%{ZEEK_DATA:[zeek][files][source]})\t(?:-|%{INT:[zeek][files][depth]:int})\t(?:-|%{ZEEK_DATA:[zeek][files][analyzers]})\t(?:-|%{ZEEK_DATA:[file][mime_type]})\t(?:-|%{ZEEK_DATA:[file][name]})\t(?:-|%{NUMBER:[zeek][files][duration]:float})\t(?:-|%{ZEEK_DATA:[zeek][files][local_orig]})\t(?:-|%{ZEEK_BOOL:[zeek][files][is_orig]})\t(?:-|%{INT:[zeek][files][seen_bytes]:int})\t(?:-|%{INT:[file][size]:int})\t(?:-|%{INT:[zeek][files][missing_bytes]:int})\t(?:-|%{INT:[zeek][files][overflow_bytes]:int})\t(?:-|%{ZEEK_BOOL:[zeek][files][timedout]})\t(?:-|%{ZEEK_DATA:[zeek][files][parent_fuid]})\t(?:-|%{ZEEK_DATA:[file][hash][md5]})\t(?:-|%{ZEEK_DATA:[file][hash][sha1]})\t(?:-|%{ZEEK_DATA:[file][hash][sha256]})\t(?:-|%{ZEEK_DATA:[zeek][files][extracted]})(?:\t(?:-|%{ZEEK_BOOL:[zeek][files][extracted_cutoff]})\t(?:-|%{INT:[zeek][files][extracted_size]:int}))?
# :long - %{INT:[zeek][files][seen_bytes]:int}
# :long - %{INT:[file][size]:int}
# :long - %{INT:[zeek][files][missing_bytes]:int}
# :long - %{INT:[zeek][files][overflow_bytes]:int}
# :long - %{INT:[zeek][files][extracted_size]:int}
)zeek-patterns"
}
