from time import sleep
import traceback

class Commons:
	request_preprocess = 'Preprocess for me'
	ack_preprocess = 'Preprocess finished'
	request_compute = 'Please Compute'
	finish_compute = 'Compute Done'
	request_result = 'Result?'
	ack_result = 'Here are the results'
	end_now = 'End now before it\'s too late'
	new_master = 'I am your new master! Be ware'
	work_change = 'Work assignment has changed due to failure'

def checkpt_file_name(machine_ix, superstep):
	return 'checkpt_file_'+str(machine_ix)+'_'+str(superstep)

def checkpt_message_file_name(machine_ix, superstep):
	return checkpt_file_name(machine_ix,superstep)+'_messages'

def dfsWrapper(dfs_opt, filename):
	try:
		result = dfs_opt(filename)
		if result == False or result == None:
			raise Exception('error in dfs operations {} on {}'.format(dfs_opt, filename))
	except Exception as e:
		print(e)
		sleep(1)
		dfsWrapper(dfs_opt, filename)
