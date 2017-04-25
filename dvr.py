import math
import sys
import json
import socket
import pickle
from multiprocessing import Process,Array,Value
from select import select
from datetime import datetime
import ctypes
import numpy as np
import time

terminate_time = 12
old_time = 0
num_nodes = 3
base_addr = 40000
near = []
ser_port = []
ans_mat_base = 0
ans_mat =0
ans_mat =0

upd_mat_base=0
upd_mat =0

#ans_mat  = [[0 for x in range(6)] for y in range(6)]

class Node():
	def __init__(self , node_num , cur_dv , nar):
		self.node_num = node_num
		self.cur_dv_base = Array(ctypes.c_float, (num_nodes+1))
		self.cur_dv = np.ctypeslib.as_array(self.cur_dv_base.get_obj())
		for r in range(0,num_nodes+1):
			self.cur_dv[r] = cur_dv[r]
		self.host = ""
		s1 = socket.socket()
		s1.bind(("" , 0))
		self.server_port = s1.getsockname()[1]
		ser_port.append(self.server_port)
		s1.close()
		self.neigh_ar = nar
		self.connected = {}
		if node_num ==1 :
			upd_mat[self.node_num] = 1
		else :
			upd_mat[self.node_num] = 1

	def node_server(self):
		s = socket.socket()
		s.bind((self.host , self.server_port))
		s.listen(5)
		global num_nodes
		while True:
			if(time.time() - old_time) > 14:
				break
			st_tim = time.time()
			fl=0
			s.settimeout(1)
			conn = 0			
			try:
				conn , addr = s.accept()
				data = conn.recv(10000)
			except socket.timeout , e:
				if conn:
					conn.close()
				fl=1
			except socket.error , e:
				if conn:
					conn.close()
				fl=1
			if fl ==1:
				break
			data = json.loads(data)
			self.update_matrix(data["node"],data["arr"] )
			conn.close()
		s.close()
			
	def node_client(self):
		while True:
			while upd_mat[self.node_num] == 0:
				if(time.time() - old_time) > 12:
					break
				continue
			upd_mat[self.node_num] = 0
			#print type(self.cur_dv)
			data = json.dumps({"node": self.node_num,"arr": self.cur_dv.tolist() })
			try:
				self.try_conn(data)
			except Exception as e:
				self.try_conn(data)
			if(time.time() - old_time) > 12:
				break

	def try_conn(self , data):
		for ver in near[self.node_num-1]:
#			print "send to server : " , ver
			s = socket.socket()
			s.connect((self.host, ser_port[int(ver)-1]))
			s.sendall(data)
#			print "Client : " , self.node_num , " , Sendto = " , ver
			s.close()

	def update_matrix(self,ver,matrix):
#		print "Server :" , self.node_num ,"Updating from: " , ver
		for k in range(1,num_nodes+1):
			snode = self.node_num
			self.cur_dv[k]=min(self.cur_dv[k],self.cur_dv[ver]+matrix[k])
			self.cur_dv[k] = self.cur_dv[k]
		upd_mat[self.node_num] = 1
		global ans_mat
		ans_mat[self.node_num - 1] = self.cur_dv
		#print ans_mat

def file_read(name):
	global terminate_time
	file = open(name,'r')
	neigh = file.read().split('\n')
	file.close()
	cost = []
	for idx,row in enumerate(neigh):
		if len(row)==0:
			continue
		arr = row.strip().split(" ")
		if idx==0:
			val=int(arr[0])
			global num_nodes 
			num_nodes = val
			for i in range(0,val+1):
				new = []
				for j in range(0,val+1):
					new.append(float('inf'))
				cost.append(new)
		if idx>0:
			val = int(arr[0])
			g = []
			for x in range(0,val):
				v = int(arr[2*x+1])
				c = float(arr[2*x+2])
				g.append(v)
				cost[idx][v]=c
			near.append(g)
	return cost
		
def main(argv):
	file_name = argv[0]
	cost = file_read(file_name)
	my_nodes = []
	threads = []
	global old_time
	old_time = time.time()
	global ans_mat
	global ans_mat_base
	global upd_mat
	global upd_mat_base
	ans_mat_base = Array(ctypes.c_float, (num_nodes+1)*(num_nodes+1))
	ans_mat = np.ctypeslib.as_array(ans_mat_base.get_obj())
	ans_mat = ans_mat.reshape((num_nodes+1),(num_nodes+1))
	for i in xrange(num_nodes+1):
		for j in xrange(num_nodes +1 ):
			ans_mat[i][j]=float(float('inf'))
	upd_mat_base = Array(ctypes.c_int, num_nodes+1)
	upd_mat = np.ctypeslib.as_array(upd_mat_base.get_obj())
	for i in xrange(num_nodes):
		x = Node(i+1 , cost[(i+1)] , near[(i)])
		t = Process(target=x.node_server)
		threads.append(t)
		my_nodes.append(x)
	for i in xrange(num_nodes):
		t = Process(target=my_nodes[i].node_client)
		threads.append(t)
	for t in threads:
		t.start()
	for t in threads:
		t.join()
	new_file = "output_"+file_name
	f = open(new_file , 'w')
	f.write(str(num_nodes)+'\n')
	for i in xrange(num_nodes):
		st = ""+str(num_nodes-1)+" "
		for j in range(1,num_nodes+1):
			if (i+1) != j:
				st += str(j) + " "+str(ans_mat[i][j])+" "
		f.write(st+'\n')
	f.close()
if __name__ == "__main__": main(sys.argv[1:])

