from sklearn.datasets import make_blobs
from datetime import datetime
import socket #import socket module
from time import time

def create_data(n_samples, n_features, centers, std):
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



s = socket.socket() #Create a socket object
host = '192.168.122.121'
# host = socket.gethostbyname_ex(socket.gethostname()) #Get the local machine name
print("Host: ", socket.gethostname())
port = 9999 # Reserve a port for your service
s.bind((host,port)) #Bind to the port

s.listen() #Wait for the client connection
while True:
    c,addr = s.accept() #Establish a connection with the client
    print("Got connection from"+ str(addr))
    features, target = create_data(30000, 3, 8, 0.65)
    t1 = time()
    for i in features:
        message = ' '.join([str(j) for j in i])+';'
        c.send(message.encode())
    print('Send {} messages in {} seconds'.format(len(features), time()-t1))
    c.close()
