#!/usr/bin/python
import socket, sys, os, optparse, shutil, io
from _multiprocessing import sendfd, recvfd

usage = "usage: dscat [-lrw] <uds> [file]"
parser = optparse.OptionParser(usage=usage)
parser.add_option("-l", "--listen",
                  action="store_true",dest="listen", default=False,
                  help="listen on <uds> and serve <file>")
parser.add_option("-w", "--write",
                  action="store_true", dest="write", default=False,
                  help="open <file> for writing")
parser.add_option("-r", "--read",
                  action="store_true",dest="read", default=False,
                  help="open <file> for reading")


options, remainder = parser.parse_args()

l = len(remainder)
if l > 1:
  uds_name = remainder[0]
  filename = remainder[1]
elif l == 1:
  filename = "-"
  uds_name = remainder[0]
else:
  parser.print_help(sys.stderr)
  sys.exit(2)

if not options.read and not options.write:
  print >>sys.stderr, "need to specify either read (-r) or write (-w) mode"
  sys.exit(2)
if options.read and options.write and filename == "-":
  print >>sys.stderr,"cannot open standard input or output in read/write mode"
  sys.exit(2)

if options.listen:
  # Make sure the socket does not already exist
  try:
      os.unlink(uds_name)
  except OSError:
      if os.path.exists(uds_name):
	  raise

  # Create a UDS socket
  sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  sock.bind(uds_name)
  sock.listen(1)
  mode = "R" if options.read else ""
  mode = mode+"W" if options.write else mode
  print >>sys.stderr, 'listening on ' + uds_name + ' to serve ' + filename + ' (' + mode + ')'

  if filename == "-":
    f_no = sys.stdout.fileno()
  else:
    if options.read and options.write:
      mode = "r+"
    elif options.read:
      mode = "r"
    else:
      mode = "w"

    try:
      f = open(filename,mode)
      f_no = f.fileno()
    except:
      print >>sys.stderr, 'failed to open file ' + filename
      sys.exit(2)

  # Wait for a connection
  try:
    connection, client_address = sock.accept()
  except:
    print >>sys.stderr, 'failed to accept connection'
    sys.exit(2)

  print >>sys.stderr, 'sending file descriptor ' + str(f_no)
  try:
    sendfd(connection.fileno(), f_no)
  except:
    print >>sys.stderr, 'failed to send file descriptor'
    sys.exit(2)

  try:
    connection.close()
  except:
    pass
else:
  try:
    print >>sys.stderr, 'connecting to ' + uds_name
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection = sock.connect(uds_name)
  except:
    print >>sys.stderr, 'failed to connect'
    raise
    sys.exit(2)

  try:
    print >>sys.stderr, 'receiving file descriptor'
    fd = recvfd(sock.fileno())
  except:
    print >>sys.stderr, 'failed to receive file descriptor'
    sys.exit(2)

  print >>sys.stderr, 'dumping contents'
  istream = io.open(fd,"rb",closefd=True)  
  shutil.copyfileobj(istream, sys.stdout)
  istream.close()
