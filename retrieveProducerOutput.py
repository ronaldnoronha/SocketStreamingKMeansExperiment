import os

if __name__ == "__main__":
    files = os.listdir('data/')
    with open('producerResult.txt','w') as p:
        for file in files:
            if file.startswith('_'):
                with open('data/'+file,'r') as f:
                    words = f.readline()
                    p.write(words+'\n')
