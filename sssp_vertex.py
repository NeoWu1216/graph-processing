from vertex import Vertex

# Shortest path Vertex, same function declaration as PRVertex
class AppVertex(Vertex):
		
	def compute(self, messages, super_step):
		self.is_source = int(self.app_args[0]) == int(self.vertex)

		self.halt = False
		min_dist = 0 if self.is_source else float('inf')
		for m in messages:
			min_dist = min(min_dist, m) 

		if (min_dist < self.value):
			self.value = min_dist
			for neighbor in self.neighbors:
				update_val = self.edge_weight(neighbor)+min_dist
				self.send_messages_to(neighbor, update_val, super_step)	
		self.vote_to_halt() 	


	@staticmethod
	def combine(messages):
		return [min(messages)] if len(messages)>0 else []
