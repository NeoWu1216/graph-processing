from collections import OrderedDict,defaultdict
import socket
import threading
import subprocess
from time import localtime, strftime
import time
import datetime
import logging

import os, sys, select
import os.path as osp
import argparse
import struct

import json # for serialize heatbeat messages
import pdb
import pprint
import random
from dfs import distributed_file_system


VM_DICT={}
VM_DICT.update({socket.gethostname():'localhost'})

## helper functions ###
def FDinstruction():
	instr={}
	instr['memb']='list the membership list'
	instr['self'] = 'list self\'s id'
	instr['join'] = 'join the group'
	instr['leave'] = 'leave the group'
	instr['help'] = 'display possible cmds'
	instr['misc'] = 'display misc info'
	instr['store'] = 'list the set of filenames stored here'
	instr['ls <filename>'] = 'list all VMs where file is replicated'
	instr['put <filename>'] = 'upload file to DFS'
	instr['get <filename>'] = 'download file from DFS'
	instr['delete <filename>'] = 'delete file in DFS'
	return instr

def findNeighbors(n, hostNameId, membershipIds):
	# N -- find N succesors and N predisussors
	# input  -- membershipIds must be a sorted list
	assert type(membershipIds) == list
	if sorted(membershipIds) != membershipIds:
		membershipIds = sorted(membershipIds)

	idx = membershipIds.index(hostNameId) if hostNameId in membershipIds else None

	if idx == None:
		membershipIds.append(hostNameId)
		membershipIds = sorted(membershipIds)
		idx = membershipIds.index(hostNameId)

	leng =  len(membershipIds)

	start = idx-n
	end = idx+n+1
	neighbors=[]


	for i in range(start,end):
		neighbors.append(membershipIds[i%leng])
	# pdb.set_trace()
	# neighbors = membershipIds[start:idx]+membershipIds[(idx+1)%leng:end]

	# final check neighbors, must not include duplicate members and host itself
	neighbors = [n for n in neighbors if n != hostNameId]
	neighbors = sorted(list(set(neighbors)))

	return neighbors

def stampedMsg(msg):
	return strftime("[%Y-%m-%d %H:%M:%S] ", localtime())+msg


### customized timer function ###
class Timer(object):
    """A simple timer."""
    def __init__(self):
        self.total_time = 0.
        self.calls = 0
        self.start_time = 0.
        self.diff = 0.
        self.average_time = 0.

    def tic(self):
        # using time.time instead of time.clock because time time.clock
        # does not normalize for multithreading
        self.start_time = time.time()

    def toc(self, average=False):
        self.diff = time.time() - self.start_time
        self.total_time += self.diff
        self.calls += 1
        self.average_time = self.total_time / self.calls
        if average:
            return self.average_time
        else:
            return round(self.diff,8)



### Heartbeat failure detector object ###
class heartbeat_detector(object):
	#  added membList and changed host name, tFail
	def __init__(self, hostName, VM_DICT,tFail, tick ,introList,port, randomthreshold, messageInterval, dfsPort, num_N =3):
		## input hostName -- this node's host name e.g 'fa17-cs425-g48-01.cs.illinois.edu'
		## VM_DICT -- mapping host name to node name
		## tFail -- FD detector protocal period time
		## introList -- pre-selected introducer list by node names
		self.file_sys = None
		self.dfsPort = dfsPort

		self.hostName=hostName
		self.VM_DICT = VM_DICT
		self.VM_INV = {v:k for k,v in VM_DICT.items()} # inverse dict of VM_DICT
		self.nodeName = self.VM_DICT[self.hostName]
		self.introList = introList
		self.host = socket.gethostbyname(self.hostName)
		logging.debug("{} has port {}".format(hostName, port))
		self.port = port
		self.randomthreshold = randomthreshold
		self.bufsize = 4096
		# membership list, stored within the node to keep track of heartbeating and active members
		self.membList = defaultdict(dict)
		# messageInterval, time to rest when sending/receiving file 200KB a piece
		self.messageInterval = messageInterval

		# misc settings
		self.tFail = tFail
		self.tCleanUp = 2*self.tFail
		self.timer = Timer() # use custimized timer instead of time.time

		#only a introducer can add node to system
		self.isIntro = True if self.nodeName in self.introList else False

		self.num_N=num_N
		self.neighbors = []
		self.tick = tick

		# initlize group id, later to be changed by self.joinGrp()
		self.groupID = self.hostName
		# token for introduction and leave
		self.introduction = 'Hello'
		self.leave = 'Goodbye'

	# strip necessary info from membership list
	def prepMsg(self):
		strippedMembList = defaultdict(dict)
		for memb in self.membList:
			strippedMembList[memb] = {'count': self.membList[memb]['count']}
		
		msg=self.encodeMsg(strippedMembList)
		return msg


	def monitor(self):
		#first, start local timer, the rest of the process follows this timer
		self.timer.tic()


		# a monitor receive message, check and response, also multicase failure message
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.port))
		# self.monitor.listen(10) # UDP doesn't support this
		logging.info(stampedMsg('Monitoring process opens.'))
		
		# keep receiving msgs from other VMs
		# receiving heartbeat and other messages 

		# pdb.set_trace()
		while True:
			try:
				data, addr = self.monitor.recvfrom(self.bufsize) # receive string json string				
				rmtHost= socket.gethostbyaddr(addr[0])[0]
				logging.debug(stampedMsg('Monitor recieve msg from {}').format(rmtHost))
			
			except socket.error, e:
				logging.info("Caught exception socket.error : %s" %e)
				logging.info(stampedMsg('Fail to receive signal from clients {}'.format(rmtHost)))
				break #TODO: should we break listening if UDP reception has troubles?

			if not data: # possibly never called in UDP					
				logging.info(stampedMsg('Receiving stop signal from clients {}'.format(rmtHost)))
				break

			# log whatever recieved
			logging.debug(stampedMsg(data))
			# decode json string
			data = self.decodeMsg(data)


			# process incoming msg
			if self.isIntro and (data == self.introduction): 
			    # if self is an intro, and imcoming msg is an introduction 
				# return current membership list to the new node
				logging.debug("{} is an introducer".format(self.hostName))

				membListMsg = self.prepMsg()
				logging.debug("Introducer {} sending memebrship list to {}".format(self.hostName, rmtHost))
				logging.debug(membListMsg)
				self.monitor.sendto(membListMsg,addr)
			
			elif data == self.leave: # if receive leave signal
				# pop this node id in membList
				leaving_node = ''
				for nodeId in self.membList.keys():
					if rmtHost in nodeId:
						leaving_node = nodeId

				if leaving_node != '':
					logging.info(stampedMsg('receiving leave signal, dropping node {}'.format(leaving_node)))
					# self.membList.pop(leaving_node)
					## instead of remove, mark the leaving node as failure
					self.membList[leaving_node]['isFailure']=True

			else:
				# update self.membList
				logging.debug("{} receives heartbeat msg from {}".format(self.hostName, rmtHost))			
				self.updateMembList(data)

		return None 

	# encode node's membership list into a string to be sent
	def encodeMsg(self,membList):
		em = json.JSONEncoder().encode(membList)
		return em
	# decode the recieved string into a json dict
	def decodeMsg(self,msg):
		receivedMembList = json.loads(msg)
		return receivedMembList

	'''multicast message to a list of targets
				input:	msg - message to be sent
						targets - list of targets'''
	def multicast(self, msg, targets):
		assert type(targets) == list
		#setup socket
		sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		port = self.port
		for target in targets:
			target_hostname = target.split('_')[0]
			target_host = socket.gethostbyname(target_hostname)
			target_nodeName = self.VM_DICT[target_hostname]
			logging.debug(stampedMsg('{} connecting to node {}({})'.format(self.nodeName,target_host, target_nodeName)))
			ran_number = random.random()
			if ran_number >= self.randomthreshold:
				sckt.sendto(msg,(target_host, port)) # UDP is connectionless


	def initSelf(self):
		self.count = 0
		self.membList[self.groupID]['count'] = self.count
		self.membList[self.groupID]['isFailure'] = False
		self.membList[self.groupID]['localtime'] = self.timer.toc()	


	def startupGrp(self): # called by the first introducer in the group
		self.initSelf()

	#create a membershiplist using the information in recieved msg fron introducer
	def createMembList(self,membList):
		#initialize server info
		self.initSelf()
		for key in membList:
			self.membList[key]['count'] = membList[key]['count']
			self.membList[key]['isFailure'] = False
			self.membList[key]['localtime'] = self.timer.toc()	
		# membList is the decoded message from introducer
	
	def updateMembList(self, nmembList):
		# nmembList -- a dictionary decoded from json string
		for i in list(nmembList):
			if i in list(self.membList):
				# if incoming sequence counter is higher then update local time
				if nmembList[i]['count'] > self.membList[i]['count']:
					# logging for debug updated information
					temp_m = self.encodeMsg(self.membList[i])
					logging.debug('**'+i)
					logging.debug('**old: {}'.format(temp_m))

					self.membList[i]['count'] = nmembList[i]['count']
					self.membList[i]['localtime'] = self.timer.toc()

					logging.debug('**update: {}'.format(self.encodeMsg(self.membList[i])))
					
			else:
				self.membList[i]['count'] = nmembList[i]['count']
				self.membList[i]['isFailure'] = False
				self.membList[i]['localtime'] = self.timer.toc()
				logging.debug('**'+i)
				logging.debug('**new node: {}'.format(self.encodeMsg(self.membList[i])))

		
	# function to check if any node has failed
	def check(self):	
		#first pull out all the time 
		lts = [value['localtime'] for key, value in self.membList.items()]
		#find most recent localtime
		maxTime = max(lts)

		# detecting failure node(s)
		for i in list(self.membList):
			# if tFail has been exceeded
			if (maxTime- self.membList[i]['localtime']) >= self.tFail:
				if not self.membList[i]['isFailure']:
					logging.info("{} has been labeled as failed ".format(i))
					self.membList[i]['isFailure'] = True
				elif (maxTime - self.membList[i]['localtime'] >= 2*self.tFail) and (self.membList[i]['isFailure']):
					if not self.really_failed(i):
						logging.warning("False detection for {} due to heavy traffic".format(i))
						self.membList[i]['localtime'] = maxTime
					else:
						logging.info("{} will be deleted from membership list".format(i))
						self.membList.pop(i)
						self.file_sys.onProcessFail(i)
						self.fail_callback(i)

			# if tFail was not exceeded
			elif self.membList[i]['isFailure']:
				logging.info("{} was labeled failed but has responded again".format(i))
				self.membList[i]['isFailure'] = False
			#else: nothing happends


	# the logic of the operation is:
	# first check if tFail has passed, if so then pass my own membership list to neighbors

	def heartbeating(self):
		prev_time = self.timer.toc()
		sleep_time = (self.tick)/3
		logging.info(stampedMsg('entering heartbeating at {}'.format(prev_time)))
		logging.info('sleep time interval is: {}'.format(sleep_time))


		while True:
			time.sleep(sleep_time) # delay for checking, sleep in sec (tick in micro sec)
			cur_time = self.timer.toc()
			if(cur_time-prev_time>self.tick): #send heartbeating every period
				prev_time = cur_time
				#increment own counter and update localtime for self
				self.membList[self.groupID]['count'] += 1
				self.membList[self.groupID]['localtime'] = self.timer.toc()
				#now to send information to the neighbors
				#first find the neighbors
				#check own member list function
				self.check()
				neighbors = findNeighbors(self.num_N, self.groupID, sorted(self.membList.keys()))
				self.neighbors = neighbors
				#encode message to send
				membListMsg=self.prepMsg()

				if int(self.timer.toc())% 50 ==0:
					logging.debug(stampedMsg('heartbeat msg multicast to {}'.format(neighbors)))
					logging.debug(stampedMsg('msg to send: {}'.format(membListMsg)))

				#loop through neighbors to send the message
				self.multicast(membListMsg, neighbors)


	def joinGrp(self):
		# the logic of this operation is:
		# first check if self is an introducer
		#   if true, check if other introducers are active, then join the group
		#   if False, initialize the group
		# if self is not an introducer
		#   iterate all introducers in the list and wait for group membership list from introducer
		# update self membership list an send heartbeats to neighbors

		# invoke group Id at every join group operation
		# incarnation timestamp
		self.incarTime = str(self.timer.toc())
		# unique id in the group
		self.groupID = self.hostName+'_'+self.incarTime
		self.groupID = self.groupID.decode('utf-8')

		# clear up membership list
		self.membList = defaultdict(dict)


		sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		active_intro_feedback = '' # introducer feedback
		port = self.port
		Intros = self.introList
		introduction = self.encodeMsg(self.introduction)


		if self.isIntro and self.nodeName != 'localhost':
			# other introducers
			Intros = [intro for intro in self.introList if intro != self.nodeName]

		# send message to introducers
		for Intro in Intros:
			Intro_host = socket.gethostbyname(self.VM_INV[Intro])

			# connect to server
			logging.info(stampedMsg('{} connecting to introducer {}({})'.format(self.nodeName,Intro_host, Intro)))
			sckt.sendto(introduction, (Intro_host,port)) # UDP is connectionless
			
			try:			
				sckt.settimeout(self.tFail/2) # contact introducer time out 0.5 s
				data, addr = sckt.recvfrom(1024)
				#	recieved data should be a string of json representing a membership list
				#	decode recieved data
				active_intro_feedback = data
				logging.info(stampedMsg('introducer connected'))
				break
			except socket.error, e:
				logging.info("Caught exception socket.error : %s" %e)
				logging.info(stampedMsg('Can not connect to introducer {}\n'.format(Intro_host)))
		
		if active_intro_feedback == '': # found no active intro
			if self.isIntro: # self is an introducer, startup the group
				logging.info(stampedMsg('self is the first member in the group, initilize it '))
				self.startupGrp()		
			else:
				logging.info(stampedMsg('self is not an introducer and no active introducer found, quit function'))
				return -1
		else: # there is at least one introducer alive, join as a regular node
			active_intro_feedback = self.decodeMsg(active_intro_feedback)
			self.createMembList(active_intro_feedback)
			logging.debug("{} is updating its own membList with recieved list".format(self.hostName))
			#self.membList = self.decodeMsg(active_intro_feedback)


			# run heartbeating as a daemon process, heart beating should find out self's neighbors given the memebrship list
		logging.info(stampedMsg('start heartbeating'))

		# while True:
		# 	self.heartbeating() # note this is for debug, at run time hearbeating should be a daemon process

		# daemon heartbeating
		self.t_hb = threading.Thread(target=self.heartbeating)
		self.t_hb.daemon=True
		self.t_hb.start()

		self.file_sys = distributed_file_system(self.hostName, self.groupID, self.VM_DICT, self.membList, self.messageInterval, self.dfsPort)

	def multicast_stopSignal(self):
		leave = self.encodeMsg(self.leave)
		self.multicast(leave, self.neighbors)


	def leaveGrp(self):
		# gracefully leaving the group and notify neighbors
		
		# check if already joined a group
		if hasattr(self,'t_hb'):
			# stop heartbeating thread
			self.multicast_stopSignal()
			# clear up membership list
			self.membList = defaultdict(dict)
			sys.exit()
		else:
			print 'node not join yet'

