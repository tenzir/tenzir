#!/usr/bin/env python3
#
# This script performs post-setup customizations of a MISP instance.
# It requires the presence of several environment variables being set,
# typically passed in as part of the container environment.
#
# The exit codes signal different conditions:
#   0 = initialization took place succesfully
#   1 = an error occurred during initialization

import os
import socket
import subprocess
import sys
import time
import urllib3
import yaml

from pymisp import PyMISP

CAKE = '/var/www/MISP/app/Console/cake'

# Wait until the web server is up an running.
print('waiting until MISP web server becomes available')
while True:
    try:
        socket.socket().connect(('127.0.0.1', 443))
        break
    except Exception as e:
        time.sleep(5)

# When MISP runs for the first time, an admin user doesn't exist yet. Browsing
# to the frontend triggers creation of an admin user lazily. But want eager
# behavior on startup, so we must trigger 'cake UserInit' to perform the
# intilization. The output includes an ephemeral API key that we can use
# subsequently to connect via PyMISP.
result = subprocess.run([CAKE, 'UserInit'], capture_output=True)
api_key = result.stdout.decode('utf-8').splitlines()[-1]
if 'MISP instance already initialised' in api_key:
    print('skipping initialization of already existing admin user')
    api_key = os.environ['MISP_API_KEY']
else:
    print(f'using ephemeral API key to initialize MISP: {api_key}')
# Clean up the mess we left after invoking 'cake' as root.
os.system('chown -R www-data:www-data /var/www/MISP')

# Connect to MISP.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
misp = PyMISP("https://127.0.0.1:443", api_key, False)
misp.toggle_global_pythonify()

# Execute YAML settings.
with open('/misp-config.yaml', 'r') as config:
    try:
        settings = yaml.safe_load(config)
        for category, values in settings.items():
            for key, value in values.items():
                setting = f'{category}.{key}'
                print(f'updating setting: {setting} = {value}')
                put = lambda x: misp.set_server_setting(setting, x, force=True)
                if type(value) == bool:
                    put('1' if value else '0')
                else:
                    put(value)
        # Ugly hack: kill the ZeroMQ subsystem so that MISP brings it up correctly
        # after touching ZeroMQ plugin config settings. For some reason, the startup
        # process is so broken that it typically requires a manual reset of the
        # subsystem through the web UI. So we kill any lingering processes
        # prior to adjusting a ZeroMQ setting. ¯\_(ツ)_/¯
        try:
            if settings['Plugin']['ZeroMQ_enable']:
                print('resetting ZeroMQ subsystem')
                os.system('pkill -f mispzmq.py')
                misp.set_server_setting('Plugin.ZeroMQ_enable', '1')
        except KeyError as error:
            pass # do nothing if ZeroMQ shouldn't be enabled
    except yaml.YAMLError as error:
        print(error)
        sys.exit(1)

# Rename admin user and reset password. We do this after applying the YAML
# configuration because success execution hinges on the password policy.
if api_key != os.environ['MISP_API_KEY']:
    print('adjusting admin email, password, and API key')
    admin = misp.get_user(1)
    admin.change_pw = '0'
    admin.email = os.environ['MISP_ADMIN_USER']
    admin.password = os.environ['MISP_ADMIN_PASSWORD']
    admin.authkey = os.environ['MISP_API_KEY']
    misp.update_user(admin, user_id=1)

print('completed initilization successfully')
sys.exit(0)
