import socket 
import threading
import os
import time
import hashlib
import queue


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		
		self.successor = (self.host, self.port)
		self.predecessor = (host, port)
		
		self.portList = queue.Queue()
		self.hostList = queue.Queue()
		self.successorHop = (self.host, self.port)
		threading.Thread(target=self.pinging).start()


	def hasher(self, key):
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		
		try:
			data = client.recv(1024).decode('utf-8')
			soc = socket.socket()
			split = data.split()

			diction = {}
			diction['split'] = data.split()
			split = data.split()

			
			if diction['split'][0] == "SendTheSuccessor":

				for _ in range(2):
					key = int(diction['split'][2])
				where = []
				where.append(self.lookup(key))
				soc.connect((self.host, int(diction['split'][1])))
				message = f"Returner {diction['split'][1]} {where[0][0]} {where[0][1]}"
				soc.send(message.encode('utf-8'))
				soc.close()
			

			elif diction['split'][0] == "Returner":
				for _ in range(1):
					self.hostList.put(diction['split'][2])
					self.portList.put(diction['split'][3])
				client.close()

			
			elif diction['split'][0] ==  "UpdatingTheSuccessor":
				for _ in range(2):
					self.successor = (diction['split'][1], int(diction['split'][2]))
				client.close()
			
			
			elif diction['split'][0] == "UpdateThePredecessor":
				message = f"Returner {self.port} {self.predecessor[0]} {self.predecessor[1]}"
				
				selfPredecessor = {}
				selfPredecessor['pre'] = (diction['split'][1], int(diction['split'][2]))
				self.predecessor = selfPredecessor['pre']
				soc.connect(selfPredecessor['pre'])
				soc.send(message.encode('utf-8'))
				soc.close()
				
				soc = socket.socket()
				soc.connect(selfPredecessor['pre'])
				

				for _ in range(2):
					message = f"ReturnerHop {self.successor[0]} {self.successor[1]} "
				soc.send(message.encode('utf-8'))
				soc.close()

				while 1:
					for item in self.backUpFiles:
						self.backUpFiles.remove(item)
					break
						
			elif diction['split'][0] == "ReturnerHop":
				self.successorHop = (diction['split'][1],int(diction['split'][2]))
			
			elif diction['split'][0] == "PuttingTheFile":
				fileName = diction['split'][1],
				for _ in range(1):
					self.files.append(fileName[0])
				
				selfHost = self.host,
				selfPort = self.port,
				
				path = f"{os.getcwd()}/{selfHost[0]}_{selfPort[0]}/{fileName[0]}"
				self.recieveFile(client, path)
				message = f"PuttingTheFileBackup {fileName[0]}"
				path = f"{os.getcwd()}/{selfHost[0]}_{selfPort[0]}/{fileName[0]}"
				pred = {}
				pred['self'] = self.predecessor
				soc.connect(pred['self'])
				message = message.encode('utf-8')
				soc.send(message)
				time.sleep(0.1)
				self.sendFile(soc, path)
			
			
			elif diction['split'][0] == "ReturningTheFile":
				for _ in range(3):
					putting = diction['split'][1],
				self.hostList.put(putting[0])
			

			elif diction['split'][0] == "PuttingTheFileBackup":
				for _ in range(1):
					fileName = diction['split'][1],
					self.backUpFiles.append(fileName[0])
					path = f"{os.getcwd()}/{self.host}_{self.port}/{fileName[0]}"
					self.recieveFile(client,path)


			elif diction['split'][0] == "CheckingTheFile":
				fileName = diction['split'][1],
				if fileName[0] in self.files:
					soc.connect((self.host, int(diction['split'][2])))
					message = f"ReturningTheFile {fileName[0]} "
					soc.send(message.encode('utf-8'))
				else:
					soc.connect((self.host, int(diction['split'][2])))
					message = f"ReturningTheFile None "
					soc.send(message.encode('utf-8'))
			

			elif diction['split'][0] == "SendingTheFile":
				key = int(diction['split'][1])
				for item in self.files:
					sendKey = self.hasher(item)
					

					predecessor = int(diction['split'][3]),
					keys = []
					keys.append(key)

					if (sendKey < keys[0] and sendKey > predecessor[0]) or (predecessor[0] > keys[0] and sendKey < keys[0]):
						for _ in range(2):
							path = f"{os.getcwd()}/{self.host}_{self.port}/{item}"
							message = f"PuttingTheFile {item}"
						soc = socket.socket()
						selfHost = self.host,
						soc.connect((selfHost[0], int(diction['split'][2])))
						message = message.encode('utf-8') 
						soc.send(message)
						time.sleep(0.1)
						self.sendFile(soc, path)
						soc.close()
		except:
			pass

	def lookup(self, key):
		key1 = key,
		selfKey = self.key,

		dict1 = {}
		selfSuccessor = []
		selfSuccessor.append(self.successor)
		selfPredecessor = []
		selfPredecessor.append(self.predecessor)
		dict1['succHash'] = self.hasher(selfSuccessor[0][0] + str(selfSuccessor[0][1]))
		dict1['predHash'] = self.hasher(selfPredecessor[0][0] + str(selfPredecessor[0][1]))  
		host = ""
		port = 0
		
		if key1[0] == selfKey[0]:
			return (self.host, self.port)
		
		if selfKey[0] < dict1['succHash'] < key1[0]:
			selfSuc = {}
			selfSuc['suc'] = self.successor
			soc = socket.socket()
			soc.connect((selfSuc['suc']))
			selfPort = self.port,
			for _ in range(3):
				message = f"SendTheSuccessor {selfPort[0]} {key1[0]}"
			soc.send(message.encode('utf-8'))
			soc.close()

			while True:
				try:
					host = []
					port = []
					host.append(str(self.hostList.get()))
					port.append(int(self.portList.get()))
					break
				except:
					pass
			return (host[0],port[0])
			
	
		elif dict1['succHash'] < key1[0] and key1[0] < selfKey[0]:
			selfSuc = {}
			selfSuc['suc'] = self.successor
			soc = socket.socket()
			soc.connect((selfSuc['suc']))
			selfPort = self.port,
			for _ in range(3):
				message = f"SendTheSuccessor {selfPort[0]} {key1[0]}"
			soc.send(message.encode('utf-8'))
			soc.close()

			while True:
				try:
					host = []
					port = []
					host.append(str(self.hostList.get()))
					port.append(int(self.portList.get()))
					break
				except:
					pass
			return (host[0],port[0])

		elif key1[0] < selfKey[0] and selfKey[0] < dict1['succHash']:
			selfSuc = {}
			selfSuc['suc'] = self.successor
			soc = socket.socket()
			soc.connect((selfSuc['suc']))
			selfPort = self.port,
			for _ in range(3):
				message = f"SendTheSuccessor {selfPort[0]} {key1[0]}"
			soc.send(message.encode('utf-8'))
			soc.close()

			while True:
				try:
					host = []
					port = []
					host.append(str(self.hostList.get()))
					port.append(int(self.portList.get()))
					break
				except:
					pass
			return (host[0],port[0])
		
		elif dict1['predHash'] > selfKey[0] and key1[0] < selfKey[0] and dict1['succHash'] > selfKey[0]:
			host1 = self.host,
			port1 = self.port,

			return (host1[0], port1[0])
		
		else:
			successorTuple = self.successor,
			return successorTuple[0]
	
	def listener(self):
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def join(self, joiningAddr):
		try:
			joinTheAddr = []
			joinInitital = []

			joinInitital.append(joiningAddr)
			joinTheAddr.append(joiningAddr[0])
			host = joinTheAddr[0]
			joinTheAddr.append(joiningAddr[1])
			port = int(joinTheAddr[1])
			
			soc = socket.socket()
			soc.connect((joinInitital[0]))
			
			message = f"SendTheSuccessor {self.port} {self.key}"
			soc.send(message.encode('utf-8'))
			
			while True:
				try:
					host = []
					port = []
					host.append(str(self.hostList.get()))
					port.append(int(self.portList.get()))
					self.successor = (host[0], port[0])
					break
				except:
					pass
			
			soc = socket.socket()
			selfSuccessor = []
			selfSuccessor.append(self.successor)
			soc.connect((selfSuccessor[0]))
			selfHost = self.host,
			selfPort = self.port,
			for _ in range(4):
				message = f"UpdateThePredecessor {selfHost[0]} {selfPort[0]}"
			soc.send(message.encode('utf-8'))
			soc.close()
			
			while True:
				try:
					host = []
					port = []
					host.append(str(self.hostList.get()))
					port.append(int(self.portList.get()))
					self.predecessor = (host[0], port[0])
					break
				except:
					pass
			selfPredecessor = []
			selfPredecessor.append(self.predecessor)
				
			soc = socket.socket()
			soc.connect((selfPredecessor[0]))
			selfHost = self.host,
			selfPort = self.port,
			
			message = f"UpdatingTheSuccessor {selfHost[0]} {selfPort[0]}"
			soc.send(message.encode('utf-8'))
			soc.close()

			soc = socket.socket()
			succ = {}
			succ['self'] = self.successor
			soc.connect((succ['self']))
			key = self.key,
			port = self.port,
			selfPredecessor = []
			selfPredecessor.append(self.predecessor)
			message = f"SendingTheFile {key[0]} {port[0]} {self.hasher(selfPredecessor[0][0]+str(selfPredecessor[0][1]))}"
			soc.send(message.encode('utf-8'))
			soc.close()
		except:
			pass

	def put(self, fileName):
		anotherFile = {}
		anotherFile['f_name'] = fileName
		key = self.hasher(anotherFile['f_name'])
		host, port = self.lookup(key)
		host1 = host,
		port1 = port,
		foundAddr = (host1[0],port1[0])
		soc = socket.socket()
		anotherFilename = []
		anotherFilename.append(anotherFile['f_name'])	
		message = f"PuttingTheFile {anotherFilename[0]}"
		soc.connect(foundAddr)
		message = message.encode('utf-8')
		soc.send(message)
		time.sleep(0.1)
		self.sendFile(soc, anotherFilename[0])
		soc.close()
		

	def get(self, fileName):
		fileName1 = []
		fileName1.append(fileName) 
		host,port = self.lookup(self.hasher(fileName1[0]))
		host_and_port = {}
		host_and_port['host'] = host
		host_and_port['port'] = port
		foundAddr =(host_and_port['host'],host_and_port['port'])
		soc = socket.socket()
		message = f"CheckingTheFile {fileName1[0]} {self.port}"
		soc.connect(foundAddr)
		message = message.encode('utf-8')
		soc.send(message)

		while (not False):
			try:
				incomingFile = str(self.hostList.get()),
				break
			except:
				pass
		if incomingFile[0] == fileName1[0]:
			return fileName1[0]
		else:
			return None
		soc.close()

	def leave(self):
		soc = socket.socket()
		succ = self.successor,
		pred = self.predecessor,
		soc.connect((succ[0]))
		message = f"UpdateThePredecessor {pred[0][0]} {pred[0][1]}"
		soc.send(message.encode('utf-8'))
		soc.close()
		soc = socket.socket()
		soc.connect((pred[0]))
		message = f"UpdatingTheSuccessor {succ[0][0]} {succ[0][1]}"
		soc.send(message.encode('utf-8'))
		soc.close()
		
		file1 = self.files,
		for item in file1[0]:
			soc = socket.socket()
			message  = f"PuttingTheFile {item}"
			path = f"{os.getcwd()}/{self.host}_{self.port}/{item}"
			soc = socket.socket()
			succ = {}
			succ['self'] = self.successor
			soc.connect((succ['self']))
			message = message.encode('utf-8')
			soc.send(message)
			time.sleep(0.1)
			self.sendFile(soc,path)
			soc.close()
		self.kill()

	
	def pinging(self):
		true1 = True
		false1 = False
		while self.stop == (not true1):
			isSuccessor = not true1
			successor1 = {}
			successor1['succ'] = self.successor
			successor1['port'] = self.port
			if successor1['succ'][1] != successor1['port']: 
				zero = 0
				one = 1
				two = 2
				nodenum = zero
				for _ in range(1):	
					while nodenum < two:
						soc = socket.socket()
						try:
							soc.connect(self.successor)
						except:
							nodenum += one
							isSuccessor = (not true1)
						try:
							for _ in range(2):
								message = "pinging".encode('utf-8')
							soc.send(message)
							isSuccessor = true1
							break
						except:
							nodenum += one
							isSuccessor = false1
				if not isSuccessor:
					successorHop = {}
					successorHop['self'] = self.successorHop
					self.successor = successorHop['self']
					soc = socket.socket()
					soc.connect(successorHop['self'])
					host = self.host,
					port = self.port,
					for _ in range(3):
						message = f"UpdateThePredecessor {host[0]} {port[0]}"
					message = message.encode('utf-8')
					soc.send(message)
					soc.close()
					for _ in range(1):
						backUp = {}
						backUp['self'] = self.backUpFiles
						for item in backUp['self']:
							soc = socket.socket()
							for _ in range(4):
								port = self.port,
								host = self.host,
								message = f"PuttingTheFile {item}"
								path = f"{os.getcwd()}/{host[0]}_{port[0]}/{item}"

							succ = {}
							succ['self'] = self.successor
							soc.connect(succ['self'])
							message = message.encode('utf-8')
							soc.send(message)
							time.sleep(0.1)
							self.sendFile(soc, path)
							soc.close()

					
			time.sleep(0.5)

	def sendFile(self, soc, fileName):
		
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		self.stop = True

		
