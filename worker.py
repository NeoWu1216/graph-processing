from message import send_all_encrypted, receive_all_decrypted, receive_all_to_target
import threading
import json 
import sys, time 
import socket
from collections import OrderedDict,defaultdict
from commons import Commons, dfsWrapper, checkpt_file_name, checkpt_message_file_name
from parser import collect_vertices_info

class Worker(object):
	# host_name: hostname of machine
	# port_info: (master_port, worker_port)
	# masters_workers: master, standby, and workers in order
	# source_vertex: for shortest path
	# commons: information shared between Master and Worker
	
	def __init__(self, app_file, host_name, port_info, masters_workers, app_args, dfs, num_threads, is_undirected):
		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.master_port, self.worker_port = port_info
		self.app_file = app_file 
		self.app_args = app_args
		self.dfs = dfs
		self.vertices = {}
		module = __import__(self.app_file[:-3])
		self.targetVertex = module.AppVertex

		self.masters_workers = masters_workers
		self.alive_workers = masters_workers[2:]
		self.num_workers = len(masters_workers)-2
		self.machine_ix = self.masters_workers.index(self.host)
		self.superstep = 0
		self.vertex_to_messages = defaultdict(list)
		self.vertex_to_messages_next = defaultdict(list)
		self.vertex_to_messages_remote_next = defaultdict(list)

		self.remote_message_buffer = defaultdict(list) # key are hosts, vals are params
		self.num_threads = num_threads
		self.is_undirected = is_undirected

		# for debugging
		self.first_len_message = defaultdict(int)
		self.local_global = [0,0]

		# kept these because avoid receiving message before initialization
		self.send_buffer_count = defaultdict(int)
		self.receive_buffer_count = defaultdict(int)
		self.buffer_count_received = defaultdict(int)
		self.hasFailure = False

	def gethost(self, vertex):
		return self.v_to_m_dict[vertex]

	def init_vertex(self, u):
		if u not in self.vertices:
			self.vertices[u] = self.targetVertex (u, [],
				self.vertex_send_messages_to, self.vertex_edge_weight, self.app_args, self.num_vertices)

	def preprocess(self, filename):
		with open(filename, 'r') as input_file:
			for line in input_file.readlines():
				if line[0] < '0' or line[0] > '9':
					continue
				u, v = line.strip().split()

				if self.gethost(u) == self.host:
			   		self.init_vertex(u)
					self.vertices[u].neighbors.append([v, 1, self.gethost(v)])
					if self.is_undirected:
						self.first_len_message[u] += 1

				if self.gethost(v) == self.host:
					self.init_vertex(v)
					if self.is_undirected:
						self.vertices[v].neighbors.append([u, 1, self.gethost(u)])
					self.first_len_message[v] += 1

		self.sorted_vertices = map(str,sorted(map(int,self.vertices.keys())))
		print('Now we have {} vertices~'.format(len(self.sorted_vertices)))
		file_name = checkpt_file_name(self.machine_ix, 0)

		with open(file_name, 'w') as checkpt_f:
			for v in self.sorted_vertices:
				adj_str = str(v)+' '
				for n in self.vertices[v].neighbors:
					adj_str += str(n[0])+' '
				checkpt_f.write(adj_str+'\n')

		dfsWrapper(self.dfs.putFile, file_name)
		print('File '+file_name+' successfully saved')
				

	def queue_message(self, vertex, value, superstep):
		self.vertex_to_messages_next[vertex].append(value)

	def queue_remote_message(self, vertex, value, superstep):
		self.vertex_to_messages_remote_next[vertex].append(value)

	def load_to_file(self, filename):
		with open(filename, 'w') as f:
			f.write(str(self.superstep)+'\n')
			for key in self.sorted_vertices:
				v = self.vertices[key]
				f.write(str(v.vertex)+' '+str(v.value)+'\n')

	def load_messages_to_file(self, filename):
		with open(filename, 'w') as f:
			f.write(str(self.superstep)+'\n')
			for v in self.sorted_vertices:
				f.write(str(v)+' '+str(self.first_len_message[v])+' '+
					' '.join(str(x) for x in self.vertex_to_messages[v])+'\n')

	def load_and_preprocess(self, conn, addr):
		start_time = time.time()
		print('receive command to load file')
		self.addr = addr[0]
		self.input_filename, self.v_to_m_dict, self.num_vertices = receive_all_decrypted(conn)
		self.input_filename, _ = receive_all_to_target(conn, 0.001)
		self.preprocess(self.input_filename)
		print('preprocess done after {} seconds'.format(time.time()-start_time))

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.addr, self.master_port))
		send_all_encrypted(sock, Commons.ack_preprocess)

	def return_result_file(self, conn, addr):
		self.output_filename, = receive_all_decrypted(conn)
		self.load_to_file(self.output_filename)
		dfsWrapper(self.dfs.putFile, self.output_filename)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.addr, self.master_port))
		send_all_encrypted(sock, Commons.ack_result)

	def change_work(self, conn, addr):
		print('receive request to change work')
		superstep, self.alive_workers, vertices_info, v_to_m_dict = receive_all_decrypted(conn)
		self.curr_thread.join()
		self.reinit_vars()
		self.superstep, self.v_to_m_dict = superstep, v_to_m_dict
		self.hasFailure = True

		file_edges = checkpt_file_name(self.machine_ix, 0)
		file_vals = checkpt_file_name(self.machine_ix, superstep)
		file_messages = checkpt_message_file_name(self.machine_ix, superstep)
		collect_vertices_info(file_edges, file_vals, file_messages, vertices_info)
		self.vertices = {}

		self.first_len_message = defaultdict(int)
		self.vertex_to_messages = defaultdict(list)
		for v in vertices_info:
			neighbors, value, first_len, messages = vertices_info[v]
			assert(v not in self.vertices)
			self.init_vertex(v)
			for n in neighbors:
				self.vertices[v].neighbors.append([n, 1, self.gethost(n)])
			self.vertices[v].value = value
			self.first_len_message[v] = int(first_len)
			self.vertex_to_messages[v] = map(float, messages)

		self.sorted_vertices = map(str,sorted(map(int,self.vertices.keys())))
		print('Now we have {} vertices~'.format(len(self.sorted_vertices)))


	def new_master(self, addr):
		self.curr_thread.join()
		self.addr = addr[0]
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.addr, self.master_port))
		send_all_encrypted(sock, Commons.new_master)
		send_all_encrypted(sock, [self.superstep, self.all_halt])


	def new_thread_queue(self, received_params, addr):
		for params in received_params:
			self.queue_remote_message(*params)
		self.buffer_count_received[addr[0]] += 1

	def start_main_server(self):
		print('I am worker No.{}!'.format(self.machine_ix))
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.worker_port))
		self.monitor.listen(5)

		while True:
			try:
				conn, addr = self.monitor.accept()
				message = receive_all_decrypted(conn)

				if message == Commons.request_preprocess:
					self.load_and_preprocess(conn, addr)

				elif message == Commons.request_compute:
					superstep,checkpt = receive_all_decrypted(conn)
					self.curr_thread = threading.Thread(target=self.compute, args=(superstep,checkpt))
					self.curr_thread.daemon = True
					self.curr_thread.start()

				elif message == Commons.request_result: # final step
					self.return_result_file(conn, addr)

				elif message == Commons.end_now:
					sys.exit()

				elif message == None: # for inner vertex communication
					self.new_thread_queue(receive_all_decrypted(conn),addr)

				elif message == 'buffer_count':
					self.receive_buffer_count[addr[0]] = receive_all_decrypted(conn)

				if message == Commons.new_master:
					threading.Thread(target=self.new_master, args=(addr,)).start()

				elif message == Commons.work_change:
					self.change_work(conn, addr)
					

			except (socket.error, ValueError) as e:
				print(e)
				continue



	def send_and_clear_buffer(self, rmt_host):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			sock.connect((rmt_host, self.worker_port))
			send_all_encrypted(sock, None)
			send_all_encrypted(sock, self.remote_message_buffer[rmt_host])
		except (socket.error, ValueError) as e:
			pass

		self.remote_message_buffer[rmt_host] = []
		self.send_buffer_count[rmt_host] += 1



	# neighbor structure (vertex, edge_weight, ip)
	def vertex_send_messages_to(self, neighbor, value, superstep):
		data = (neighbor[0], value, superstep)
		if neighbor[2] != self.host:
			rmt_host = neighbor[2]
			self.remote_message_buffer[rmt_host].append(data)
		else:
			self.queue_message(*data)
			self.local_global[0] += 1

		self.local_global[1] += 1



	def vertex_edge_weight(self, neighbor):
		return neighbor[1]


	def compute_selected_vertex(self, thread_ix, superstep):
		start_ix = len(self.vertices)*thread_ix/self.num_threads 
		end_ix = len(self.vertices)*(thread_ix+1)/self.num_threads
		 
		for v in self.sorted_vertices[start_ix: end_ix]:
			messages = self.vertex_to_messages[v]
			# for debug only!!!
			if self.app_file=='pr_vertex.py' and self.first_len_message[v] != len(messages) and superstep > 1:
				print 'error occurs: {},{},{}'.format(v, self.first_len_message[v], len(messages))
				sys.exit()

			vertex = self.vertices[v]

			if not vertex.halt or len(messages)!=0:
				self.vertices[v].compute(messages, superstep)


	def compute_each_vertex(self, superstep):
		threads = [None]*self.num_threads

		for thread_ix in range(self.num_threads):
			threads[thread_ix] = threading.Thread(
				target=self.compute_selected_vertex, args=(thread_ix,superstep))
			threads[thread_ix].start()

		for thread_ix in range(self.num_threads):
			threads[thread_ix].join()

		for host in self.remote_message_buffer:
			self.send_and_clear_buffer(host)


	def reinit_vars(self):
		self.send_buffer_count = defaultdict(int)
		self.receive_buffer_count = defaultdict(int)
		self.buffer_count_received = defaultdict(int)

		self.vertex_to_messages = defaultdict(list)
		for v in self.vertices:
			self.vertex_to_messages[v] = self.vertex_to_messages_next[v]+self.vertex_to_messages_remote_next[v]

		self.vertex_to_messages_next = defaultdict(list)
		self.vertex_to_messages_remote_next = defaultdict(list)
		self.all_halt = all(len(m)==0 for m in self.vertex_to_messages.values())


	def send_checksum_info(self):
		try:
			for rmt_host in self.alive_workers:
				if rmt_host != self.host:
					sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sock.connect((rmt_host, self.worker_port))
					send_all_encrypted(sock, 'buffer_count')
					send_all_encrypted(sock, self.send_buffer_count[rmt_host])
		except KeyboardInterrupt:
			raise		
		except:
			pass #forfeit the current compute

	def wait_for_all_messages(self):
		print('......... Waiting .............')
		saved_alive_workers = list(self.alive_workers)
		for rmt_host in saved_alive_workers:
			if rmt_host != self.host:
				while rmt_host not in self.receive_buffer_count:
					time.sleep(1) 
					if saved_alive_workers != self.alive_workers:
						return
				if saved_alive_workers != self.alive_workers:
					return
				while self.receive_buffer_count[rmt_host] != self.buffer_count_received[rmt_host]:
					time.sleep(1)
					if saved_alive_workers != self.alive_workers:
						return
				if saved_alive_workers != self.alive_workers:
					return
		print('......... Cameback.............')
		return True

	def checkpt_file(self, superstep):
		file_name = checkpt_file_name(self.machine_ix, superstep)
		message_file_name = checkpt_message_file_name(self.machine_ix, superstep)
		self.load_to_file(file_name)
		self.load_messages_to_file(message_file_name)
		dfsWrapper(self.dfs.putFile, file_name)
		dfsWrapper(self.dfs.putFile, message_file_name)
		print('File '+ file_name +' successfully saved')
		print('File '+ message_file_name +' successfully saved')


	def reply_finished_compute(self):
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((self.addr, self.master_port))
			send_all_encrypted(sock, Commons.finish_compute)
			send_all_encrypted(sock, self.all_halt)
		except KeyboardInterrupt:
			raise		
		except:
			pass

	def compute(self, superstep, checkpt):
		print '\nCompute for Superstep {}'.format(superstep)
		start_time = time.time()
		assert(self.superstep == superstep-1)

		self.compute_each_vertex(superstep)
		self.send_checksum_info()
		print 'All messages sent after {} seconds'.format(time.time()-start_time)

		if self.wait_for_all_messages()!=True:
			print('clean up for Superstep {}'.format(superstep))
			return

		self.reinit_vars()
		for vertex in self.vertices:
			self.vertex_to_messages[vertex] = self.targetVertex.combine(self.vertex_to_messages[vertex])
		self.superstep += 1

		if checkpt:
			self.checkpt_file(superstep)
			
		assert(len(self.vertex_to_messages_next) == 0)
		print 'Compute finishes after {} seconds'.format(time.time()-start_time)

		if (self.local_global[1] == 0):
			print('No vertex processed')
		else:
			print('local_global ratio: {}'.format(1.0*self.local_global[0]/self.local_global[1]))

		
		self.reply_finished_compute()
		

