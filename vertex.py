class Vertex:
	# send_messages_to(neighbor, value): sends value to neighbor
	# edge_weight(neighbor):  returns weight of the outgoing edge to the neighbor
	def __init__(self, vertex, neighbors, send_messages_to, edge_weight, app_args, num_vertices):
		self.vertex = vertex
		self.value = float('inf')
		self.neighbors = neighbors
		self.send_messages_to = send_messages_to
		self.halt = False
		self.edge_weight = edge_weight
		self.app_args = app_args
		self.num_vertices = num_vertices
		# ...

	def vote_to_halt(self):
		self.halt = True

	def send_to_all_neighbors(self, value, super_step):
		for neighbor in self.neighbors:
			self.send_messages_to(neighbor, value, super_step)

	@staticmethod
	def combine(messages):
		return messages
