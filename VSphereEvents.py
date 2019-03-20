#!/usr/bin/env python

from datetime import datetime, timedelta
from pyVim.connect import SmartConnect
from pyVmomi import vim
from ConfigParser import ConfigParser
import requests
import ssl
import logging
import argparse

def request_filter( hours=24, no_verify=True ):

	logger = logging.getLogger( 'request.filter' )

	# Disable certificate verification if needed.
	if no_verify:
		requests.packages.urllib3.disable_warnings()
		ssl._create_default_https_context = ssl._create_unverified_context
		context = ssl.create_default_context()
		context.check_hostname = False
		context.verify_mode = ssl.CERT_NONE

	# Build the filter spec.
	time_filter = vim.event.EventFilterSpec.ByTime()
	now = datetime.now()
	time_filter.beginTime = now - timedelta( hours=hours )
	time_filter.endTime = now
	filter_spec = vim.event.EventFilterSpec( time=time_filter )

	return filter_spec

def request_events( filter_spec, username, password ):

	logger = logging.getLogger( 'request.events' )

	# Connect to the VCSA.
	si = SmartConnect(
		host='172.30.1.29', user=username, pwd=password )

	# Setup pager/filter.
	event_manager = si.content.eventManager
	event_collector = event_manager.CreateCollectorForEvents( filter_spec )
	page_size = 1000
	events = []

	while True:
		try:
			events_in_page = event_collector.ReadNextEvents( page_size )
			events_in_page_len = len( events_in_page )
			if 0 == events_in_page_len:
				break
			events.extend( events_in_page )
		except Exception as e:
			logger.error( type( e ) )

	return events

def main():

	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-r', '--hours', action='store', type=int, default=24,
		help='Number of hours back to retrieve.' )
	parser.add_argument(
		'-c', '--config', action='store', type=str,
		default='Local/vsphere.ini',
		help='Path to config file.' )

	args = parser.parse_args()

	logging.basicConfig( level=logging.INFO )
	logger = logging.getLogger( 'main' )

	config = ConfigParser()
	config.read( args.config )
	username = config.get( 'auth', 'username' )
	password = config.get( 'auth', 'password' )

	filter_spec = request_filter( hours=args.hours )
	events = request_events( filter_spec, username, password )

	for e in events:
		logger.info( '[{}] {}'.format( e.createdTime, e.fullFormattedMessage ) )

if '__main__' == __name__:
	main()

