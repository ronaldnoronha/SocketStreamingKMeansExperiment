import os
import shutil

listOfExperiments = []

listOfExperiments.append({'numNodes':4, 'numPorts':4, 'time':60000, 'executorMem': '2g', 'batchDuration': 1})
listOfExperiments.append({'numNodes':4, 'numPorts':4, 'time':180000, 'executorMem': '2g', 'batchDuration': 1})
listOfExperiments.append({'numNodes':4, 'numPorts':4, 'time':240000, 'executorMem': '2g', 'batchDuration': 1})
listOfExperiments.append({'numNodes':4, 'numPorts':4, 'time':3000000, 'executorMem': '2g', 'batchDuration': 1})
listOfExperiments.append({'numNodes':4, 'numPorts':4, 'time':360000, 'executorMem': '2g', 'batchDuration': 1})
# listOfExperiments.append({'numNodes':4, 'numPorts':4, 'time':60000, 'executorMem': '2g', 'batchDuration': 1})


if not os.path.exists('results/'):
    os.makedirs('results')

if __name__ == "__main__":
    for i in listOfExperiments:
        os.system('fab runExperiment:'+str(i['numNodes'])+','+str(i['numPorts'])+','+str(i['time'])+' > logSpark.txt')
        print('Creating Folder')
        # Create folder based on the experiment
        path = 'results/'+str(i['numNodes'])+'_'+str(i['numPorts'])+'_'+str(i['time'])+'_'+str(i['executorMem'])+'_'\
               +str(i['batchDuration'])+'_'
        while os.path.exists(path):
            if path[len(path)-1]=='_':
                path+=str(1)
            else:
                pathBreakdown = path.split('_')
                pathBreakdown[len(pathBreakdown)-1] = str(int(pathBreakdown[len(pathBreakdown)-1])+1)
                path = '_'.join(pathBreakdown)
        os.makedirs(path)
        print('Moving log files')
        # Add all files to results directory
        for file in os.listdir():
            if file.endswith('.csv') or file.endswith('.txt'):
                shutil.move(file, path)
