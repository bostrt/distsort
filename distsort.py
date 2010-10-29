import sys
import getopt
import time
import socket
import json
from merge_sort import *

import random

PORT = 8455

class SortClient:

    def __init__(self, servers, data):
        self.data = data
        self.servers = servers
        self.sockets = []
        # sortedData stores a sorted list at each index(from each server)
        self.sortedData = []

    def start(self):
        """ This function should be called first to  """
        # Right now this is hardcodes for only
        # two computers to process the data.
        # This client and a server.

        # Split the data into equal parts for
        # each computer that is sorting.
        parts = []
        
        firstHalf = self.data[0:len(self.data)/2]
        lastHalf  = self.data[len(self.data)/2:len(self.data)]
        self.sendData(self.servers[0], firstHalf)
        self.sortedData.append(merge_sort(lastHalf))
        
    def recvSortedData(self):
        for s in self.sockets:
            try:
                raw = s.recv(16777216)
                result = json.loads(raw)
                self.sortedData.append(result)
            except ValueError,msg :
                print msg

    def sendData(self, server, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((server, PORT))
            sock.send(json.dumps(data)+'\n')
            self.sockets.append(sock)
        except socket.error:
            # Sort locally
            self.sortedData.append(merge_sort(data))

    def combineData(self):
        while len(self.sortedData) > 1:
            x = self.sortedData.pop()
            y = self.sortedData.pop()
            # Merge data and put combined data 
            # back in sorted data
            self.sortedData.append(merge(x,y))
        # Should be a length 1 list here
        return self.sortedData

    def close(self):
        for s in self.sockets:
            s.close()


class SortServer:
    
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', PORT))
        self.data = []

    def start(self):
        """ Start listening. While data is being sent receive it and 
        store in class variable """
        self.socket.listen(1)
        self.conn, addr = self.socket.accept()
        print "Connection from ", addr
        rawData = ""
        while True:
            raw = self.conn.recv(16777216)
            if not raw: break
            rawData += raw
            if raw.endswith('\n'):
                self.data = json.loads(rawData)
                break
        # We're done. Send the data back and close the connection
        print "Merge sorting..."
        sortedData = json.dumps(merge_sort(self.data))
        print "Sending sorted data to client..."
        self.conn.sendall()
        print "Data send...closing"

    def close(self):
        self.conn.close()
        self.socket.close()


if __name__ == '__main__':
    if '-server' in sys.argv:
        s = SortServer()
        s.start()
        s.close()
    elif '-client' in sys.argv:
        # Create random list
        thelist = []
        for i in range(0, 50000):
            thelist.append(random.randint(0,10000000))
        starttime = time.time()
        c = SortClient(['192.168.1.133'], thelist) 
        c.start()
        c.recvSortedData()
        c.combineData()
        c.close()
        endtime = time.time()
        print c.sortedData[0]
        print "Orig size:",len(c.data)
        print "Size:",len(c.sortedData[0])
        print "Time:",endtime-starttime
        

