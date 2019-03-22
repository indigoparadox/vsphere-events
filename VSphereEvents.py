#!/usr/bin/env python

from datetime import datetime, timedelta
from pyVim.connect import SmartConnect
from pyVmomi import vim, vmodl
from ConfigParser import ConfigParser, NoSectionError
import requests
import ssl
import logging
import argparse
import jsonpickle
import os
import calendar
import syslog
from logging.handlers import SysLogHandler

FILTER_TASKS=1
FILTER_EVENTS=2

def request_filter( hours, no_verify, f_type=FILTER_TASKS, duration=None ):

	logger = logging.getLogger( 'request.filter' )

	# Disable certificate verification if needed.
	if no_verify:
		requests.packages.urllib3.disable_warnings()
		ssl._create_default_https_context = ssl._create_unverified_context
		context = ssl.create_default_context()
		context.check_hostname = False
		context.verify_mode = ssl.CERT_NONE

	# Build the filter spec.
	if FILTER_TASKS == f_type:
		time_filter = vim.TaskFilterSpec.ByTime()
	elif FILTER_EVENTS == f_type:
		time_filter = vim.event.EventFilterSpec.ByTime()
	now = datetime.now()
	time_filter.timeType = vim.TaskFilterSpec.TimeOption.startedTime
	time_filter.beginTime = now - timedelta( hours=hours )
	if duration:
		time_filter.endTime = time_filter.beginTime + timedelta( hours=duration )
	else:
		time_filter.endTime = now
	if FILTER_TASKS == f_type:
		filter_spec = vim.TaskFilterSpec( time=time_filter )
	elif FILTER_EVENTS == f_type:
		filter_spec = vim.event.EventFilterSpec( time=time_filter )

	return filter_spec

def request_events( filter_spec, username, password ):

	logger = logging.getLogger( 'request.events' )

	# Connect to the VCSA.
	try:
		si = SmartConnect(
			host=hostname, user=username, pwd=password )
	except ssl.SSLError as e:
		logger.error( e )
		return []

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

def request_tasks( filter_spec, username, password, hostname, persist ):

	logger = logging.getLogger( 'request.tasks' )

	# Connect to the VCSA.
	try:
		si = SmartConnect(
			host=hostname, user=username, pwd=password )
	except ssl.SSLError as e:
		logger.error( e )
		return []

	# Setup pager/filter.
	task_manager = si.content.taskManager
	task_collector = task_manager.CreateCollectorForTasks( filter_spec )
	page_size = 1000
	tasks = []

	while True:
		try:
			tasks_in_page = task_collector.ReadNextTasks( page_size )
			tasks_in_page_len = len( tasks_in_page )
			if 0 == tasks_in_page_len:
				break
			tasks.extend( tasks_in_page )
		except Exception as e:
			logger.error( e )

	return tasks

class Persist():
	def __init__( self, persist_path ):
		self._logger = logging.getLogger( 'open.persist' )
		self._persist_path = persist_path
		self._persist = ConfigParser()
		if os.path.exists( persist_path ):
			try:
				self._persist.read( persist_path )
				test = self._persist.get( 'tasks', 'current' )
			except NoSectionError as e:
				self._logger.error( 'Tasks section not found; creating...' )
				self._persist.add_section( 'tasks' )
				self._persist.set( 'tasks', 'running', '' )
				self._persist.set( 'tasks', 'current', '' )
				self._persist.set( 'tasks', 'current_tasks', '' )
		else:
			self._persist.add_section( 'tasks' )
			self._persist.set( 'tasks', 'running', '' )
			self._persist.set( 'tasks', 'current', '' )
			self._persist.set( 'tasks', 'current_tasks', '' )

	def get_running_tasks( self ):
		return self._persist.get( 'tasks', 'running' ).split( ',' )

	def get_last_pass_epoch( self ):
		try:
			return int( self._persist.get( 'tasks', 'current' ) )
		except ValueError as e:
			return 0

	def get_last_pass_epoch_tasks( self ):
		return self._persist.get( 'tasks', 'current_tasks' ).split( ',' )

	def remove_running_task( self, task_id ):
		running_tasks = self._persist.get( 'tasks', 'running' ).split( ',' )
		running_tasks.remove( task_id )
		self._persist.set( 'tasks', 'running', ','.join( running_tasks ) )

	def add_running_task( self, task_id ):
		running_tasks = self._persist.get( 'tasks', 'running' ).split( ',' )
		running_tasks.append( task_id )
		self._persist.set( 'tasks', 'running', ','.join( running_tasks ) )

	def add_current_task( self, task_id ):
		tasks = self._persist.get( 'tasks', 'current_tasks' ).split( ',' )
		tasks.append( task_id )
		self._persist.set( 'tasks', 'current_tasks', ','.join( tasks ) )

	def reset_epoch( self, new_epoch ):
		self._persist.set( 'tasks', 'current', str( new_epoch ) )
		self._persist.set( 'tasks', 'current_tasks', '' )

	def save( self ):
		with open( self._persist_path, 'w' ) as persist_file:
			self._persist.write( persist_file )

def main():

	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-r', '--hours', action='store', type=int, default=24,
		help='Number of hours back to retrieve.' )
	parser.add_argument(
		'-u', '--duration', action='store', type=int, default=None,
		help='Number of hours to retrieve from HOURS.' )
	parser.add_argument(
		'-c', '--config', action='store', type=str,
		default='Local/vsphere.ini',
		help='Path to config file.' )
	parser.add_argument(
		'-o', '--output', action='store', type=str, default='log',
		help='Output format (log|syslog|json)' )
	parser.add_argument(
		'-n', '--noverify', action='store_true',
		help='Do not verify SSL certificates.' )
	parser.add_argument(
		'-p', '--persist', action='store', type=str,
		default='/tmp/vspheretasks.lock',
		help='Persistence record for reported tasks.' )
	parser.add_argument(
		'-v', '--verbose', action='store_true',
		help='Debug output.' )

	args = parser.parse_args()

	if args.verbose:
		loglevel = logging.DEBUG
	else:
		loglevel = logging.INFO
	logging.basicConfig( level=loglevel )
	logger = logging.getLogger( 'main' )

	if 'syslog' == args.output:
		print( 'Logging to syslog...' )
		handler = SysLogHandler( address='/dev/log' )
		logger.addHandler( handler )

	config = ConfigParser()
	config.read( args.config )
	username = config.get( 'auth', 'username' )
	password = config.get( 'auth', 'password' )
	hostname = config.get( 'auth', 'hostname' )

	filter_spec = request_filter(
		hours=args.hours, no_verify=args.noverify, duration=args.duration )
	tasks = request_tasks(
		filter_spec, username, password, hostname, args.persist )
	tasks.sort( key=lambda x: calendar.timegm( x.startTime.timetuple() ) )

	# Grab the current persist state if any.
	#last_pass_epoch=0
	#last_pass_epoch_tasks=[]
	#running_tasks=[]
	persist = Persist( args.persist )

	for e in tasks:

		task_id = e.key.split( '-' )[1]
		task_epoch = calendar.timegm( e.startTime.timetuple() )
		
		if task_id in persist.get_running_tasks() and 'running' == e.state:
			# Come back to this next run. No news is good news.
			continue
		elif task_id in persist.get_running_tasks() and 'running' != e.state:
			# The task has completed.
			persist.remove_running_task( task_id )
		elif task_id not in persist.get_running_tasks() and 'running' == e.state:
			# This must be a new running task.
			persist.add_running_task( task_id )
		
		if int( task_epoch ) < persist.get_last_pass_epoch():
			# Ancient history.
			logger.debug( 'Old task {} in epoch: {} (LPE: {})'.format(
				task_id, task_epoch, persist.get_last_pass_epoch() ) )
			continue
		elif int( task_epoch ) == persist.get_last_pass_epoch():
			if task_id in persist.get_last_pass_epoch_tasks():
				# This task was already closed and reported.
				logger.debug( 'Skipping task {} in epoch: {}'.format(
					task_id, task_epoch ) )
				continue
			else:
				# A new task for this epoch.
				logger.debug( 'Adding task {} to epoch: {}'.format(
					task_id, task_epoch ) )
				persist.add_current_task( task_id )
		else:
			# Task with a new epoch, so the previous is probably closed.
			logger.debug( 'Opening new epoch: {}'.format( task_epoch ) )
			persist.reset_epoch( task_epoch )
			persist.add_current_task( task_id )

		# Output the current task.
		if 'json' == args.output:
			print( jsonpickle.encode( e ) )
		elif 'log' == args.output or 'syslog' == args.output:
			if vim.TaskReasonSchedule == type( e.reason ):
				reason = 'schedule'
			elif vim.TaskReasonUser == type( e.reason ):
				reason = e.reason.userName
			start_time = str( e.startTime ).split( '+' )[0]
			end_time = str( e.completeTime ).split( '+' )[0]
			task_id = int( str( e.task ).split( '-' )[1][:-1] )
			logger.info(
				'vcsatask VCSATask {} started {} completed {}: {}: {}: {} by {}'
			.format(
				task_id,
				start_time,
				end_time,
				e.entityName,
				e.state,
				e.descriptionId,
				reason ) )

	persist.save()

if '__main__' == __name__:
	main()

