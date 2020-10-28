import socket
from time import time, sleep
import sys
import os
from threading import Thread
import re

def sortedAlphanumeric(data):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ]
    return sorted(data, key=alphanum_key)

def sendMessages(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    while True:
        try:
            s.bind((host, port))  # Bind to the port
            break
        except:
            os.system(repr('lsof -i :' + str(port) + ' | awk \'{system(\"kill -9 \" $2)}\''))

    path = 'data/' + str(port) + '/'
    listFiles = sortedAlphanumeric(os.listdir(path))
    totalMessagesSent = 0
    # Confirm an output log file is removed from the folder
    # if os.path.isfile('data/_'+str(port)):
    #     os.remove('data/_'+str(port))

    s.listen()
    t1 = time()
    for i in listFiles:
        c, addr = s.accept()
        t2 = time()
        print('Connection received from {}'.format(addr))
        with open(path+i,'r') as f:
            message = f.readline()
            c.send(message.encode('utf-8'))
            totalMessagesSent += len(message.split(';'))
        c.close()
        with open('data/_'+str(port),'a') as f:
            f.write('{} seconds to send {} messages by {} port\n'.format(time()*1000.0, totalMessagesSent, port))
        sleep(max(1-time()+t2, 0))


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





