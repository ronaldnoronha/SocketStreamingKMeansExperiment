from sklearn.datasets import make_blobs
from datetime import datetime
import socket #import socket module
from time import time

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

def sendMessages(numMsgs, host, port, centers):
    s = socket.socket()  # Create a socket object
    s.bind((host, port))  # Bind to the port
    s.listen()

    while True:
        c, addr = s.accept()
        print('Connection received from {}'.format(addr))
        t1 = time()
        features,_ = createData(numMsgs, 3, centers, 0.65)
        for i in features:
            message = ' '.join([str(j) for j in i]) + ';'
            c.send(message.encode())
        print('{} messages sent from {} in {} seconds'.format(numMsgs, port, time()-t1))
        c.close()

if __name__ == "__main__":
    numMsgs = int(sys.argv[1])
    host = sys.argv[2]
    numThreads = 4
    threads = [None]*numThreads
    centers,_ = createData(8, 3, 8, 0.65)
    port = 10000
    t1 = time()
    for i in range(len(threads)):
        threads[i] = Thread(target=sendMessages, args=(int(numMsgs/numThreads), host, port, centers))
        threads[i].start()
        port += 1

    for i in threads:
        i.join()

    print('{} messages sent in {} seconds'.format(numMsgs, time()-t1))



