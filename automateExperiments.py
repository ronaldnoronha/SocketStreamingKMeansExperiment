import os
import shutil
import time

listOfExperiments = []


listOfExperiments.append({'numNodes':1, 'numPorts':6, 'time':300000, 'executorMem': '2g', 'batchDuration': 30})
listOfExperiments.append({'numNodes':2, 'numPorts':6, 'time':300000, 'executorMem': '2g', 'batchDuration': 30})
listOfExperiments.append({'numNodes':3, 'numPorts':6, 'time':300000, 'executorMem': '2g', 'batchDuration': 30})
listOfExperiments.append({'numNodes':4, 'numPorts':6, 'time':300000, 'executorMem': '2g', 'batchDuration': 30})
listOfExperiments.append({'numNodes':5, 'numPorts':6, 'time':300000, 'executorMem': '2g', 'batchDuration': 30})
listOfExperiments.append({'numNodes':6, 'numPorts':6, 'time':300000, 'executorMem': '2g', 'batchDuration': 30})



if not os.path.exists('results/'):
    os.makedirs('results')

if __name__ == "__main__":
    for i in listOfExperiments:
        os.system('fab runExperiment:'+str(i['numNodes'])+','+str(i['numPorts'])+','+str(i['time'])+','+i['executorMem']+','+str(i['batchDuration'])+' > logSpark.txt')
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
        # time.sleep(30)
