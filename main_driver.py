from collections import OrderedDict,defaultdict
import logging
import socket
import threading
from multiprocessing import Process, Queue
import os, time, sys, argparse
from heartbeat import heartbeat_detector
from message import send_all_encrypted, send_all_from_file
from message import receive_all_decrypted, receive_all_to_target
from master import Master
from worker import Worker
from time import sleep

class Driver(object):
	def __init__(self, host_name, port, worker_port, alive_port, master_port, membList, dfs, messageInterval, result_file, num_threads, undirected):
		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.port = port
		self.worker_port = worker_port
		self.alive_port = alive_port
		self.master_port = master_port
		self.membList = membList
		self.dfs = dfs
		self.message_input = 'User has already inputted'
		self.message_output = 'I am done with processing file'
		self.message_fail = 'Some worker failed'
		self.message_congrats = 'Congrats! you are the new master'
		self.messageInterval = messageInterval
		self.result_file = result_file
		self.worker_num_threads = num_threads
		self.is_undirected = undirected

		self.client_ip = None
		self.role = 'unknown'
		self.master = None # make sense only if role == 'master'
		self.filename_pair = [None, None]
		self.app_file = -1
		self.app_args = -1
		self.dup_check = {}

	def drive(self):
		newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
		queue = Queue()

		# a monitor receive message, check and response, also multicase failure message
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.server_sock.bind((self.host, self.port))
		self.server_sock.listen(5)

		self.input_task = Process(target=self.get_input, args=(newstdin, queue))
		self.input_task.daemon = True
		self.input_task.start()

		self.server_task = Process(target=self.background_server, args=(queue,))
		self.server_task.start()

		self.alive_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.alive_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.alive_sock.bind((self.host, self.alive_port))
		self.alive_sock.listen(10)

		self.alive_task = Process(target=self.Im_alive)
		self.alive_task.daemon = True
		self.alive_task.start()

		self.app_file, self.app_args, self.filename_pair, self.role, self.client_ip, self.masters_workers, self.is_undirected = queue.get()

		if (self.role == 'client'):
			self.start_as_client()
		elif (self.role == 'master'):
			self.start_as_master()
		elif (self.role == 'worker'):
			self.start_as_worker()
		elif (self.role == 'standby'):
			self.start_as_standby()

	# assert no one fails during input time
	def get_input(self, newstdin, queue):
		sys.stdin = newstdin

		print 'Files to choose from: '
		os.system('rm -f checkpt_file_*')
		os.system('rm -f file_piece_*')
		os.system('ls')
		print 

		self.input_ready = False
		while(not self.input_ready):
			self.input_ready = True

			try:
				argv = raw_input('Input graph_file app_file app_args, or enter help: ').strip().split()
				graph_file, app_file = argv[:2] 
				app_args = argv[2:]

			except:
				print 'Example input: com-amazon.ungraph.txt pr_vertex.py 20'
				self.input_ready = False
				continue

			for filename in (graph_file, app_file):
				if not os.path.exists(filename):
					print 'File {} does not exist'.format(filename)
					self.input_ready = False

			if len(app_file) < 4 or app_file[-3:]!='.py':
				print 'Application file must be a python file'
				self.input_ready = False
					

		queue.put((app_file, app_args, (graph_file, self.result_file), 'client', self.host, None, self.is_undirected))

	def background_server(self, queue):
		conn, addr = self.server_sock.accept()				
		rmtHost= socket.gethostbyaddr(addr[0])[0]
		
		message = receive_all_decrypted(conn) # the instruction

		if message==self.message_input:
			self.input_task.terminate()
			print

			self.app_file , _ = receive_all_to_target(conn, self.messageInterval)
			self.app_args = receive_all_decrypted(conn)
			self.masters_workers = receive_all_decrypted(conn)
			self.is_undirected = receive_all_decrypted(conn)
			
			if self.host in self.masters_workers[0:2]:
				self.role = 'master'
				self.filename_pair[0] , _ = receive_all_to_target(conn, self.messageInterval)
				self.filename_pair[1] = receive_all_decrypted(conn)
				
				if self.host == self.masters_workers[1]:
					self.role = 'standby'
					print 'I am the standby master!'

			else:
				self.role = 'worker'

			queue.put((self.app_file, self.app_args, self.filename_pair, self.role, addr[0], self.masters_workers, self.is_undirected))
			return


		elif message == self.message_output: # for client and standby
			if self.role != 'standby': # a hack since self.role not updated in this process
				filename, _ = receive_all_to_target(conn, self.messageInterval)
				assert(filename == self.result_file)
				print 'Task done, result is published to {}'.format(filename)

		elif message == self.message_congrats:
			print('Hehe, now it is my turn')
			self.role = 'master'

		queue.put(self.role)

	def Im_alive(self):
		while True:
			self.alive_sock.accept()


	def start_as_client(self):
		print 'I am the client!'
		sleep(0.5)
		real_members = [host.split('_')[0] for host in sorted(self.membList.keys())]
		print 'All members: {}'.format(real_members)
		self.masters_workers = [socket.gethostbyname(host) for host in real_members if host != self.host_name]

		for host in self.masters_workers:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((host, self.port))

			send_all_encrypted(sock, self.message_input)
			send_all_from_file(sock, self.app_file, self.messageInterval)
			send_all_encrypted(sock, self.app_args)
			send_all_encrypted(sock, self.masters_workers)
			send_all_encrypted(sock, self.is_undirected)
			if host in self.masters_workers[0:2]:
				send_all_from_file(sock, self.filename_pair[0], self.messageInterval)
				send_all_encrypted(sock, self.filename_pair[1])

	def init_master(self, standby):
		self.master = Master(self.filename_pair, self.masters_workers, self.host_name, 
							(self.master_port, self.worker_port, self.port),
							(self.client_ip, self.message_output, self.message_fail), 
							self.dfs, standby)

	def start_as_master(self):
		#self.master = Master
		print 'I am the master!'
		self.init_master(False)
		self.master.execute()

	def start_as_worker(self):
		print 'I am the worker!'
		self.worker = Worker(self.app_file, self.host_name, (self.master_port, self.worker_port), 
							self.masters_workers, self.app_args, self.dfs, self.worker_num_threads, self.is_undirected)
		self.worker.start_main_server()


	def start_as_standby(self):
		# wait for either master fail or receiving a finished signal
		queue = Queue()
		self.server_task = Process(target=self.background_server, args=(queue,))
		self.server_task.start()
		self.role = queue.get()
		if (self.role == 'master'):
			self.masters_workers[0:2] = self.masters_workers[1::-1]
			self.init_master(True)
			self.master.execute()

	def really_failed(self, failed_process):
		try:
			failed_process = failed_process.split('_')[0]
			failed_ip = socket.gethostbyname(failed_process)
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((failed_ip, self.alive_port))
			return False
		except socket.error, e:
			return True

	def onProcessFail(self, failed_process):
		failed_process = failed_process.split('_')[0]
		failed_ip = socket.gethostbyname(failed_process)

		if self.role == 'master' and failed_ip in self.masters_workers[2:] and failed_ip not in self.dup_check:
			#print('One of the workers {} has left...'.format(failed_process))
			self.dup_check[failed_ip] = True
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((self.host, self.master_port))
			send_all_encrypted(sock, self.message_fail)
			send_all_encrypted(sock, failed_ip)
			

		elif self.role == 'standby' and failed_ip == self.masters_workers[0]:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((self.host, self.port))
			send_all_encrypted(sock, self.message_congrats)
			send_all_encrypted(sock, failed_ip)




#-------------------------------------------------------------------------------main-----------------------------------------------------------------------------------
if __name__ == '__main__':
	#attemp to add a port changing argument 
	ports = 2222, 3333, 4444, 5555, 6666, 7777

	parser = argparse.ArgumentParser()
	parser.add_argument("--verbose", '-v', action='store_true')
	parser.add_argument("--cleanLog", '-c', action='store_true')
	parser.add_argument("--output_file", '-o', type=str, default='processed_values.txt')
	parser.add_argument("--num_threads",'-n', type=int, default='4')
	parser.add_argument("--undirected", '-u', action='store_true')

	args = parser.parse_args()
	# update VM ip with node id
	VM_DICT = {}
	VM_DICT.update(OrderedDict({'fa17-cs425-g48-%02d.cs.illinois.edu'%i:'Node%02d'%i for i in range(1,11)}))
	
	# manually assign two introducers
	VM_INTRO = ['Node01','Node02']


	# setup logger for membership list and heartbeating count
	# failure detector log directory
	FD_dir = './FD_log'
	if not os.path.exists(FD_dir): os.makedirs(FD_dir)
	FD_log_file = 'log.txt'
	FD_log_dir = os.path.join(FD_dir,FD_log_file)
	# create DS log collector if not exists
	if not os.path.exists(FD_log_dir):
		file = open(FD_log_dir, 'w+')
	elif args.cleanLog:
		os.remove(FD_log_dir)
		file = open(FD_log_dir, 'w+')

	loggingLevel = logging.DEBUG if args.verbose else logging.INFO
	logging.basicConfig(format='%(levelname)s:%(message)s', filename=FD_log_dir,level=loggingLevel)
	

	hbd = heartbeat_detector(hostName=socket.gethostname(),
							VM_DICT=VM_DICT,
							tFail = 1.5,
							tick = 0.5,
							introList=VM_INTRO,
							port=ports[0],
							dfsPort=ports[1],
							num_N=3,
							randomthreshold = 0,
							messageInterval = 0.001)

	monitor = threading.Thread(target=hbd.monitor)
	monitor.daemon=True
	monitor.start()

	hbd.joinGrp()

	main_driver = Driver(socket.gethostname(), ports[2], ports[3], ports[4], ports[5], hbd.membList, hbd.file_sys, 
						0.001, args.output_file, args.num_threads, args.undirected)
	hbd.really_failed = main_driver.really_failed 
	hbd.fail_callback = main_driver.onProcessFail
	
	main_driver.drive()
	
	

