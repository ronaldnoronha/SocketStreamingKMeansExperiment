from sklearn.datasets import make_blobs
from time import time
import sys
from threading import Thread
import os

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



def createFiles(port, numMsgs, numFiles, centers):

    path = '/home/ronald/data/'+str(port)

    if not os.path.exists(path):
        os.mkdir(path)
    t1 = time()
    for i in range(numFiles):
        features, _ = createData(numMsgs, 3, centers, 0.65)
        t2 = time()
        with open(path+'/'+str(i)+'.txt','w+') as f:
            f.write(';'.join(' '.join([str(j) for j in i]) for i in features))
        print('{} seconds'.format(time() - t2))

    print('{} seconds for {} port files create'.format(time()-t1,port))


if __name__ == "__main__":

    numFiles = int(sys.argv[1])
    numMsgs = int(sys.argv[2])
    numPorts = int(sys.argv[3])
    centers, _ = createData(8, 3, 8, 0.65)
    threads = [None]*numPorts

    if not os.path.exists('./data/'):
        os.mkdir('./data')

    for i in range(len(threads)):
        threads[i] = Thread(target=createFiles,args=(10000+i,numMsgs,numFiles, centers))
        threads[i].start()

    for i in threads:
        i.join()
