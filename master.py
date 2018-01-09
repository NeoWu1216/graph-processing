import random 
from time import sleep, time
from threading import Thread
import socket, json
from parser import parse_file, combine_files, collect_vertices_info
from message import receive_all_decrypted, send_all_encrypted, send_all_from_file
from commons import Commons, dfsWrapper, checkpt_file_name, checkpt_message_file_name

class Master:

	def __init__(self, filename_pair, masters_workers, host_name, port_info, client_info, dfs, is_standby):
		self.input_filename, self.output_filename = filename_pair
		self.masters_workers = masters_workers
		self.alive_workers = masters_workers[2:]
		self.num_workers = len(masters_workers)-2 # num alive workers

		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.master_port, self.worker_port, self.driver_port = port_info
		self.client_ip, self.client_message, self.fail_message = client_info
		self.dfs = dfs

		self.num_preprocess_done = 0
		self.num_process_done = 0
		self.is_standby = is_standby

		self.superstep = 0
		self.all_done = False
		self.failures = []


	def send_to_worker(self, list_of_things, worker):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((worker, self.worker_port))
		send_all_encrypted(sock, list_of_things[0])
		send_all_encrypted(sock, list_of_things[1:])
		return sock

	def background_server(self):
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.server_sock.bind((self.host, self.master_port))
		self.server_sock.listen(5)

		while True:
			conn, addr = self.server_sock.accept()				
			rmtHost= socket.gethostbyaddr(addr[0])[0]
			try:
				message = receive_all_decrypted(conn)
			except (socket.error, ValueError) as e:
				continue

			if message == Commons.ack_preprocess:
				self.num_preprocess_done += 1

			elif message == Commons.finish_compute:	
				halt = receive_all_decrypted(conn)
				self.all_done.append(halt)

			elif message == Commons.ack_result:
				self.num_process_done += 1

			elif message == self.fail_message:
				self.failures.append(receive_all_decrypted(conn))

			elif message == Commons.new_master:
				superstep, halt = receive_all_decrypted(conn)
				assert(self.superstep==0 or self.superstep==superstep)
				self.superstep = superstep
				self.all_done.append(halt)

	def initialize(self):
		if self.is_standby:
			self.regain_info()
		else:
			self.preprocess()

	def regain_info(self):
		sleep(0.5)
		self.server_task = Thread(target=self.background_server)
		self.server_task.daemon = True
		self.server_task.start()

		self.all_done = []
		for worker in list(self.alive_workers):
			try:
				sock = self.send_to_worker([Commons.new_master], worker)
			except:
				self.alive_workers.remove(worker)
				continue
		
		while len(self.all_done) < len(self.alive_workers):
			sleep(0.5)
		self.all_done = all(self.all_done)
		self.num_workers =  len(self.alive_workers)


	def preprocess(self):
		sleep(0.5)
		self.server_task = Thread(target=self.background_server)
		self.server_task.daemon = True
		self.server_task.start()

		print('I have {} workers!'.format(self.num_workers))
		self.v_to_m_dict, self.num_vertices = parse_file(self.input_filename, self.num_workers, self.masters_workers)
		print('num_vertices: ', self.num_vertices)

		for ix in range(self.num_workers):
			sock = self.send_to_worker([Commons.request_preprocess,self.input_filename, self.v_to_m_dict, self.num_vertices], self.masters_workers[ix+2])
			send_all_from_file(sock, self.input_filename, 0.001)

		while (self.num_preprocess_done < self.num_workers):
			sleep(1)

	def update_and_report(self, vertices_info):
		curr_ix = 0
		self.num_workers = len(self.alive_workers)
		self.split_vertices_info = [{} for _ in range(self.num_workers)]
		for v in vertices_info:
			machine_id = curr_ix*self.num_workers/len(vertices_info)
			assert(machine_id < self.num_workers)
			self.v_to_m_dict[v] = self.alive_workers[machine_id]
			self.split_vertices_info[machine_id][v] = vertices_info[v]
			curr_ix += 1
		for i, worker in enumerate(self.alive_workers):
			self.send_to_worker([Commons.work_change, self.superstep, self.alive_workers, self.split_vertices_info[i], self.v_to_m_dict], worker)

	def process_failure(self):
		for ix in reversed(range(len(self.failures))):
			if self.failures[ix] not in self.alive_workers:
				self.failures.pop(ix)

		if len(self.failures) == 0:
			return False

		self.superstep -= 2
		if (self.superstep%2 == 0):
			self.superstep -= 1

		vertices_info = {}
		for ix in reversed(range(len(self.failures))):
			failed_process = self.failures.pop(ix)
			if failed_process not in self.alive_workers:
				continue
			self.alive_workers.remove(failed_process)
			failed_ix = self.masters_workers.index(failed_process)
			file_edges = checkpt_file_name(failed_ix, 0)
			file_values = checkpt_file_name(failed_ix, self.superstep)
			file_messages = checkpt_message_file_name(failed_ix, self.superstep)

			print('Fetching file: ' + file_edges + ' ........')
			dfsWrapper(self.dfs.getFile, file_edges)
			print('Fetching file: ' + file_values + ' ........')
			dfsWrapper(self.dfs.getFile, file_values)
			print('Fetching file: ' + file_messages + ' ........')
			dfsWrapper(self.dfs.getFile, file_messages)
			print('All files fetched')

			collect_vertices_info(file_edges, file_values, file_messages, vertices_info)

		if len(vertices_info) != 0:
			self.update_and_report(vertices_info)
			self.all_done = False
			return True


	def process(self):
		while not self.all_done:
			start_time = time()
			self.all_done = []	
			self.compute_count = 0
			self.superstep += 1
			self.checkpt = self.superstep%2
			for worker in self.alive_workers:
				try:
					self.send_to_worker([Commons.request_compute, self.superstep, self.checkpt], worker)
				except:
					break

			while len(self.all_done)<self.num_workers and len(self.failures)==0:
				sleep(0.25)

			print('Superstep {} ended after {} seconds...'.format(self.superstep, time()-start_time))

			if len(self.failures) == 0:
				self.all_done = all(self.all_done)
				time_elapsed = time()-start_time
			else:
				sleep(5)
				if self.process_failure()==True:
					print('Recovered from worker failure, now at superstep {}'.format(self.superstep))
				print('The workers: {}'.format(self.alive_workers))
				print('The num _workers: {}'.format(self.num_workers))
				


	def remote_end_tasks(self):
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((self.masters_workers[1], self.driver_port))
			send_all_encrypted(sock, self.client_message) # actually standby_message
		except:
			pass

		for worker in self.alive_workers:
			self.send_to_worker([Commons.end_now], worker)

	def collect_results(self):
		self.result_files = [0]*self.num_workers

		for ix in range(self.num_workers):
			worker = self.alive_workers[ix]
			self.result_files[ix] = 'file_piece_'+str(ix)+'_out'
			self.send_to_worker([Commons.request_result, self.result_files[ix]], worker)

		while (self.num_process_done < self.num_workers):
			sleep(1)

		for ix in range(self.num_workers):
			dfsWrapper(self.dfs.getFile,self.result_files[ix]) 

		combine_files(self.output_filename, self.result_files)

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.client_ip, self.driver_port))
		send_all_encrypted(sock, self.client_message)
		send_all_from_file(sock, self.output_filename, 0.001)

		self.remote_end_tasks()

		
	# execute the task in 3 phases
	def execute(self):
		if (self.num_workers < 1):
			print 'Error: No worker available'
			return

		start_time = time()
		self.initialize()
		print('Initialization done, took {} seconds'.format(time()-start_time))

		start_time = time()
		self.process()
		print('Process done, took {} seconds'.format(time()-start_time))

		start_time = time()
		self.collect_results()
		print('Results collected, took {} seconds'.format(time()-start_time))

	
