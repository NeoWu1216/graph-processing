from vertex import Vertex

# Page rank Vertex 
class AppVertex(Vertex):
	# compute at each super_step
	# messages are basically values
	def compute(self, messages, super_step):
		self.num_iterations = int(self.app_args[0])

		self.halt = False
		if super_step > 0:
			self.value = 0.15/self.num_vertices+0.85*sum(messages)

		if (super_step < self.num_iterations) and len(self.neighbors) != 0:
			self.send_to_all_neighbors(self.value/len(self.neighbors), super_step)
		elif super_step >= self.num_iterations:
			self.vote_to_halt()
