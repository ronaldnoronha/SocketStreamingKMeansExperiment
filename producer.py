from sklearn.datasets import make_blobs
from datetime import datetime
import socket #import socket module
from time import time
import sys
import os
from threading import Thread



def sendMessages(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    s.bind((host, port))  # Bind to the port
    s.listen()
    path = 'data/'+str(port)+'/'
    listFiles = os.listdir(path)
    while True:
        for i in listFiles:
            t1 = time()
            c, addr = s.accept()
            print('Connection received from {}'.format(addr))
            totalFilesSent = 0
            with open(path+i,'r') as f:
                message = f.readline()
                c.send(message.encode('utf-8'))
                totalFilesSent += 1
            c.close()
            print('{} seconds to send a file by {} port'.format(time()-t1,port))



if __name__ == "__main__":

    host = sys.argv[1]
    port = 10000
    numPorts = int(sys.argv[2])
    threads = [None]*numPorts

    for i in range(numPorts):
        threads[i] = Thread(target=sendMessages,args=(host, port+i))
        threads[i].start()

    for i in threads:
        i.join()



    # listFiles = os.listdir('data/')
    # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    # s.bind((host, port))  # Bind to the port
    # s.listen()
    # totalMsgs = 0
    # while True:
    #     for i in listFiles:
    #         c, addr = s.accept()
    #         print('Connection received from {}'.format(addr))
    #         with open('data/'+i,'r') as f:
    #             message = f.readline()
    #             t1 = time()
    #             c.send(message.encode('utf-8'))
    #             print('{} seconds'.format(time()-t1))
    #             c.close()





