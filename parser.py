import sys

def get_vertex(line):
	return int(line.split()[0])

def parse_file(graph_filename, num_machines, masters_workers):
	with open(graph_filename, 'r') as graph_file:
		lines = graph_file.readlines()
		v_to_m_dict = {}
		curr_counter = 0

		for line in lines:
			if line[0] < '0' or line[0] > '9':
					continue
			u, v = line.split()
			if u not in v_to_m_dict:
				v_to_m_dict[u] = curr_counter
				curr_counter += 1
			if v not in v_to_m_dict:
				v_to_m_dict[v] = curr_counter
				curr_counter += 1

		num_vertices = curr_counter
		v_to_m_dict = {v: masters_workers[2+num_machines*i/num_vertices] for v,i in v_to_m_dict.items()}
	
	return v_to_m_dict, num_vertices
	
# process vertices results into 1
def combine_files(output_filename, collected_files):
	supersteps = []
	unsorted_pairs = []
	for collected_file in collected_files:
		with open(collected_file, 'r') as input_file:
			lines = input_file.readlines()
			supersteps.append(int(lines[0]))
			for line in lines[1:]:
				x, y = line.split()
				unsorted_pairs.append((int(x), float(y)))
		
	with open(output_filename, 'w') as output_file:
		for x,y in sorted(unsorted_pairs, key=lambda x:-x[1]):
			output_file.write('{} {}\n'.format(x,y))

	assert(len(set(supersteps)) <= 1)


def collect_vertices_info(file_edges, file_values, file_messages, vertices_info):
	with open(file_edges, 'r') as edges:
		edge_lines = edges.readlines()
	with open(file_values, 'r') as values:
		value_lines = values.readlines()
	with open(file_messages, 'r') as messages:
		message_lines = messages.readlines()

	assert(len(edge_lines) == len(value_lines)-1 == len(message_lines)-1)
	for i in range(len(edge_lines)):
		edge_info = edge_lines[i].split()
		value_info = value_lines[i+1].split()
		message_info = message_lines[i+1].split()
		assert(edge_info[0] == value_info[0] == message_info[0])
		edges = edge_info[1:]
		value = value_info[1]
		first_len = message_info[1]
		messages = message_info[2:]
		assert(edge_info[0] not in vertices_info)	
		vertices_info[edge_info[0]] = (edges,value,first_len,messages)
