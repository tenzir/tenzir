#!/usr/bin/env python
import subprocess,sys,csv,optparse,string,shelve,tempfile,os,glob
from cStringIO import StringIO

usage = "usage: %prog [-r] [-s ] [-n node] [-e limit] [-t timefield] <query>"
version="%prog version 0.0"
epilog="""
Splunk external search command script
-------------------------------------

To enable in Splunk perform de following tasks:

1- Copy this script to [SPLUNKHOME]/etc/searchscripts/
2- Edit [SPLUNKHOME]/etc/system/local/commands.conf and add these lines:
  
   [vast]
   filename = splunk-search.py
   generating = true
   supports_multivalues = true
   streaming = true
  
   [vaststats]
   filename = splunk-search.py
   generating = true
   supports_multivalues = true
  
3- Restart Splunk
  
In splunk Search, perform queries using vast or vaststats commands.
(vast returns results as events. vaststats returns results as stats)
  
Example queries:
---------------
-       | vast ":addr==10.0.0.53" -r
-       | vast "&type == 'bro::intel'" -sr
-       | vaststats "'Evil' in :string"
-       | vaststats "&time < now" -e 5000 -n 127.0.0.1:42000
  
Note: Splunk "eats" double quotes when parsing input, so the VAST query
requires replacing double with single quotes and vice versa.
  
CAVEATS:
-------
- Use stream mode only when results have a single type.
  Results from other sources other than the first will be discarted.
  Also, stream mode will not split multivalue fields in Splunk.
- Limits in the Splunk GUI may limit the number of displayed rows.
  Use the stats mode (by using the vast2 command) to avoid these limits.

"""

class MyOptionParser(optparse.OptionParser):
  def error(self, msg):
    if "splunk" in sys.executable:
      print(msg)
    return optparse.OptionParser.error(self,msg)

optparse.OptionParser.format_epilog = lambda self, formatter: self.epilog

parser = MyOptionParser(usage=usage,version=version,epilog=epilog,add_help_option=False)
parser.add_option('-h', '--help',
                  action="store_true", dest="help", default=False,
                  help='show this help message')
parser.add_option("-r", "--raw",
                  action="store_true",dest="raw", default=False,
                  help="include _raw field in results.")
parser.add_option("-s", "--stream",
                  action="store_true",dest="stream", default=False,
                  help="stream input/output line-by-line.\n"+
                       "works only if results are from single source type.\n"+
                       "multivalues are not processed.")
parser.add_option("-t", "--timefield",
                  action="store",dest="tfield", default="ts",metavar="FIELD",
                  help="override timestamp field in Vast results."\
                       " (default=%default).")
parser.add_option("-e", "--limit",
                  action="store",dest="limit", default=0,metavar="LIMIT",
                  help="limit number of results returned. (default=no limit).")
parser.add_option("-n", "--node",
                  action="store",dest="node", default='',metavar="IP:PORT",
                  help="node to query. (default=local).")

'''
Interplunk format for multivalues: values are wrapped in '$' 
and separated using ';'. Literal '$' values are represented with'$$'
'''
def getEncodedMV(vals):
    s = ""
    for val in vals:
        val = val.replace('$', '$$')
        if len(s):
            s += ';'
        s += '$' + val + '$'
    return s

def outputResults(results, fields = None, mvdelim = '\n',
                  outputfile = sys.stdout):
    '''
    Outputs the contents of a result set to STDOUT in Interplunk format.
    '''
    
    if results == None:
        return
    
    s = set()

    '''
    Check each entry to see if it is a list (multivalued). If so, set
    the multivalued key to the proper encoding. Replace the list with a
    newline separated string of the values
    '''
    for i in range(1,len(results)+1):
        for key in results[str(i)].keys():
            if(isinstance(results[str(i)][key], list)):
                results[str(i)]['__mv_' + key] = \
                  getEncodedMV(results[str(i)][key])
                results[str(i)][key] = \
                  string.join(results[str(i)][key], mvdelim)
                if not fields.count('__mv_' + key):
                  fields.append('__mv_' + key)
        s.update(results[str(i)].keys())

    if fields is None:
        h = list(s)
    else:
        h = fields
    
    dw = csv.DictWriter(outputfile, h)

    dw.writerow(dict(zip(h, h)))
    for i in range(1,len(results)+1):
      dw.writerow(results[str(i)])

def printHelp(parser):
  # Print help message diferently from cmdline and from Splunk
  if "splunk" in sys.executable:
    print("ERROR,__mv_ERROR")
    out = StringIO()
    parser.print_help(out)
    str_out = out.getvalue()
    out.close()
    writer = csv.writer(sys.stdout)
    writer.writerow([str_out, getEncodedMV(\
      str_out.replace("splunk-search.py [","|vast [").replace("  ","..")\
      .split("\n"))])
    print("-,-")
    sys.exit(0)
  else:
    parser.print_help(sys.stderr)
    sys.exit(2)

opt, remainder = parser.parse_args()
if len(remainder) == 1:
  # Splunk eats double quotes when parsing input, so the VAST query requires
  # preprocessing: replace double with single quotes and vice versa.
  query = remainder[0].replace("'",'"')
  if opt.node == '':
    full_cmd = ['/usr/local/bin/vast', 'export', 'csv', '-h', '-e', 
                str(opt.limit), query]
  else:
    full_cmd = ['/usr/local/bin/vast', '-e', str(opt.node), 'export', 'csv', 
                '-h', '-e', str(opt.limit), query]
else:
  printHelp(parser)

if opt.help:
  printHelp(parser)
  
p = subprocess.Popen(full_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                     stderr=sys.stderr)

# Print cmd line to stderr to show in splunk query log.
print >> sys.stderr,full_cmd

fullheader =[]
currentheader = []
eventlist = []
results = []

stream_valid_source = True
stream_header = ""

if not opt.stream:
  fd,dfname = tempfile.mkstemp(suffix='.SplunkVastShelve')
  os.close(fd)
  os.remove(dfname)
  d = shelve.open(dfname,flag='n',writeback=True)
  di = 1
  modcounter = 1
  d.clear()

while True:
  out = p.stdout.readline()
  if out == '' and p.poll() != None:
    break
  if out != '':
    if not out.startswith("type,"):
      # not header. get event in dictinary list

      # strip and replace incorrect set separator for string sets      
      ll=[out.strip().replace("\n","").replace("\" | \""," | ")]
      csvline=csv.reader(ll, quotechar='"', delimiter=',', 
                         quoting=csv.QUOTE_MINIMAL, skipinitialspace=True)
      if opt.stream:
        if stream_valid_source == True:
          linelist=next(csvline)
          data = StringIO()
          writer = csv.writer(data, quotechar='"', delimiter=',', 
                              quoting=csv.QUOTE_MINIMAL)
          writer.writerow(linelist)
          line=data.getvalue()
          data.close()
          if opt.raw:
            raw = line.replace('"','""')
            sys.stdout.write(line.strip() + ',"' + raw.strip() + '"\n')
          else:
            sys.stdout.write(line.strip() + '\n')
      else:
	dictionary = dict(zip(currentheader, next(csvline)))
        if opt.raw == True:
          dictionary['_raw']=ll
	for key in dictionary:
	  if " | " in dictionary[key]:
            dictionary[key] = (dictionary[key]).split(" | ")
            dictionary[key] += dictionary[key]
        d[str(di)]=dictionary
        di += 1
        modcounter += 1
        if modcounter == 50000 : #don't want to use modulus
          d.sync()
    else:
      # new header
      if opt.stream:
        if stream_header == "":
          if opt.raw:
            stream_header = out.strip().replace(","+opt.tfield+",",",_time,")\
                            +",_raw\n"
          else:
            stream_header = out.strip().replace(","+opt.tfield+",",",_time,")\
                           +"\n"
          sys.stdout.write(stream_header)
        elif stream_header == \
               out.strip().replace(","+opt.tfield+",",",_time,")+",_raw\n" \
             or \
             stream_header == \
               out.strip().replace(","+opt.tfield+",",",_time,")+"\n":
          stream_valid_source = True
        else:
          stream_valid_source = False
      else:
        currentheader=out.strip().replace(","+opt.tfield+",",",_time,")\
                      .split(",")
        [fullheader.append(i) for i in currentheader \
          if not fullheader.count(i)]

# output results if not streaming line-by-line
if not opt.stream:
  if opt.raw:
    fullheader.append("_raw")
  outputResults(d,fields = fullheader)
  for filename in glob.glob(tempfile.gettempdir()+"/*SplunkVastShelve*") :
    try:
      os.remove( filename )
    except:
      pass
