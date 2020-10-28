from sklearn.datasets import make_blobs
import socket
from time import time
import sys
import os
from threading import Thread
import re
import numpy as np



def sortedAlphanumeric(data):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ]
    return sorted(data, key=alphanum_key)


def createData(n_samples, n_features, centers, std):
    features, target = make_blobs(n_samples = n_samples,
                                  # two feature variables,
                                  n_features = n_features,
                                  # four clusters,
                                  centers = centers,
                                  # with .65 cluster standard deviation,
                                  cluster_std = std,
                                  # shuffled,
                                  shuffle = True)
    return features, target

def sendMessages(host, port, size, centers):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    while True:
        try:
            s.bind((host, port))  # Bind to the port
            break
        except:
            os.system(repr('lsof -i :' + str(port) + ' | awk \'{system(\"kill -9 \" $2)}\''))

    s.listen()
    t1 = time()
    totalMessagesSent = 0
    for i in size:
        c, addr = s.accept()
        print('Connection received from {}'.format(addr))
        features, _ = createData(i, 3, centers, 0.65)
        message = ';'.join(' '.join([str(j) for j in i]) for i in features)
        c.send(message.encode('utf-8'))
        c.close()
        totalMessagesSent += i
        with open('data/_'+str(port),'w') as f:
            f.write('{} seconds to send {} messages by {} port'.format(time()-t1, totalMessagesSent, port))


if __name__ == "__main__":

    host = sys.argv[1]
    port = 10000
    numPorts = int(sys.argv[2])
    threads = [None]*numPorts

    weibullShape = float(sys.argv[3])
    scale = int(sys.argv[4])
    numFiles = int(sys.argv[5])

    size = np.random.weibull(weibullShape, numFiles) * scale
    size = [int(i) for i in size]

    centers, _ = createData(8, 3, 8, 0.65)


    for i in range(numPorts):
        threads[i] = Thread(target=sendMessages,args=(host, port+i, size, centers))
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





