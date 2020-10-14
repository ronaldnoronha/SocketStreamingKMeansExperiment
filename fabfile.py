# Import Fabric's API module
from fabric2 import Connection, Config
from invoke import Responder
from fabric2.transfer import Transfer
from datetime import datetime
import os

with open('./conf/master', 'r') as f:
    array = f.readline().split()
    masterHost = array[0]
    masterPort = array[1]
    user = array[2]
    host = array[3]

config = Config(overrides={'user': user})
conn = Connection(host=host, config=config)
configMaster = Config(overrides={'user': user, 'connect_kwargs': {'password': '1'}, 'sudo': {'password': '1'}})
master = Connection(host=masterHost, config=configMaster, gateway=conn)

slaveConnections = []
configSlaves = Config(overrides={'user': user, 'connect_kwargs': {'password': '1'}, 'sudo': {'password': '1'}})
with open('./conf/slaves', 'r') as f:
    array = f.readline().split()
    while array:
        slaveConnections.append(Connection(host=array[0], config=configSlaves, gateway=conn))
        array = f.readline().split()
with open('./conf/kafka', 'r') as f:
    array = f.readline().split()
    kafka = Connection(host=array[0], config=Config(overrides={'user': user,
                                                                         'connect_kwargs': {'password': '1'},
                                                                         'sudo': {'password': '1'}}), gateway=conn)
sudopass = Responder(pattern=r'\[sudo\] password:',
                     response='1\n',
                     )

def startSparkCluster(n='1'):
    # start master
    master.run('source /etc/profile && $SPARK_HOME/sbin/start-master.sh')
    # start slaves
    for i in range(int(n)):
        slaveConnections[i].run('source /etc/profile && $SPARK_HOME/sbin/start-slave.sh spark://'+str(masterHost)+':7077')


def stopSparkCluster():
    master.run('source /etc/profile && $SPARK_HOME/sbin/stop-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/stop-all.sh')


def restartAllVMs():
    for connection in slaveConnections:
        try:
            connection.sudo('shutdown -r now')
        except:
            continue
    try:
        master.sudo('shutdown -r now')
    except:
        pass
    try:
        kafka.sudo('shutdown -r now')
    except:
        pass


def stop():
    stopSparkCluster()
    stopKafka()

def runExperiment(clusters='3',numPorts='2',time='60000',executorMem='2g', batchDuration='1'):
    # transfer file
    transfer = Transfer(master)
    kafkaTransfer = Transfer(kafka)
    # Start Monitors
    transferMonitor()
    startMonitor()
    # Transfer Producer
    kafkaTransfer.put('./producer.py')
    startKafka(numPorts)
    # SBT packaging
    os.system('sbt package')
    # start start cluster
    startSparkCluster(clusters)
    # transfer jar
    transfer.put('./target/scala-2.12/socketstreamingkmeansexperiment_2.12-0.1.jar')
    try:
        master.run(
                'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
                '--class Experiment '
                '--master spark://' + str(masterHost) + ':7077 --executor-memory ' + executorMem + ' '
                '~/socketstreamingkmeansexperiment_2.12-0.1.jar '
                '192.168.122.121 '
                '10000 '
                + numPorts + ' '
                + time + ' '
                + batchDuration
            )
    except:
        print('Spark Crashed while running')
        print('Application stopped at: {}'.format(datetime.now().strftime("%H:%M:%S.%f")))
    finally:
        # transfer logs
        stopMonitor()
        transferLogs()
        # Restart all VMs
        stop()
        # restartAllVMs()

def startMonitor():
    for connection in slaveConnections+[master, kafka]:
        connection.run('nohup python3 ./monitor.py $1 >/dev/null 2>&1 &')

def stopMonitor():
    for connection in slaveConnections+[master, kafka]:
        connection.run('pid=$(cat logs/pid) && kill -SIGTERM $pid')

def transferLogs():
    counter = 1
    for connection in slaveConnections:
        transfer = Transfer(connection)
        transfer.get('logs/log.csv', 'log_slave' + str(counter) + '.csv')
        counter += 1
    transfer = Transfer(master)
    transfer.get('logs/log.csv', 'log_master.csv')
    transfer = Transfer(kafka)
    transfer.get('logs/log.csv', 'log_kafka.csv')


def transferMonitor():
    for connection in slaveConnections+[master, kafka]:
        connection.run('rm -rf logs')
        transfer = Transfer(connection)
        transfer.put('monitor.py')
        connection.run('mkdir logs')

def transferToMaster(filename):
    transfer = Transfer(master)
    transfer.put(filename)

def transferToKafka(filename):
    transfer = Transfer(kafka)
    transfer.put(filename)

def startKafka(numPorts='2'):
    kafka.run('tmux new -d -s socket')
    kafka.run('tmux send -t socket python3\ ~/producer.py\ 192.168.122.121\ '+numPorts+' ENTER')
    
def stopKafka():
    try:
        kafka.run('tmux kill-session -t socket')
    except:
        print('Socket already closed!')

def createFiles():
    transfer = Transfer(kafka)
    transfer.put('createFiles.py')
    kafka.run('python3 createFiles.py 500 100000 4')

def closeCreateFile():
    for connection in slaveConnections:
        connection.run('tmux kill-session -t createFile')

def transferFile(clusters='1'):
    transfer = []
    for i in range(int(clusters)):
        transfer.append(Transfer(slaveConnections[i]))
    for connection in transfer:
        connection.put('./transferFile.py')
    for i in range(int(clusters)):
        slaveConnections[i].run('tmux new -d -s transferFile')
        slaveConnections[i].run('tmux send -t transferFile python3\ ~/transferFile.py ENTER')

def closeTransferFile(clusters='1'):
    for i in range(int(clusters)):
        slaveConnections[i].run('tmux kill-session -t transferFile')
        slaveConnections[i].run('rm ~/data/*.txt')

