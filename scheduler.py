import time
import sys
import socket
from _thread import *
import os
import ast
import subprocess
import json
import datetime as dt

#-----------------------------------------------------------------------------------------------#

task = []  

'''
{
"starttime": "24:00 hours format",
"endtime": "24:00 hours format",
"recurring_bit" : 0/1,
"recurring_interval" : seconds in INT, 
"appname" : "string",
"sid" : "[list of sensor ids]"
}
'''

# IP PORT for Interacting with PLATFORM MANAGER
IP = "127.0.0.1"
PORT = 8081

#-----------------------------------------------------------------------------------------------#

def split_time(str1):
    d,t = str1.split(" ")
    Y,mo,d = d.split("-")
    h,m,s = t.split(":")
    #print(Y,mo,d,h,m,s)
    return int(Y), int(mo), int(d), int(h), int(m), int(s)

#-----------------------------------------------------------------------------------------------#

def process_time(st):
    Y,mo,d,h,m,s = split_time(st)

    a = dt.datetime(Y,mo,d,h,m,s)
    b = dt.datetime(1970,1,1,00,00,00)

    return (a-b).total_seconds() - 19800

#-----------------------------------------------------------------------------------------------#

def generate_algo_file(Client, appname):
    content = Client.recv(65535*1024)
    if len(content)<=0:
        print("Empty file received!\n")
        return
    
    fileName = appname
    #print("STATUS : " + fileName + " received\n")
    with open(fileName, "wb") as f:
        data = f.write(content)

    resp = "RECEIVED"
    Client.sendall(resp.encode())

#-----------------------------------------------------------------------------------------------#

def push_task(data):
    stime = data['starttime']
    etime = data['endtime']

    start_time = process_time(stime)
    end_time = process_time(etime)

    data['starttime'] = start_time
    data['endtime'] = end_time

    datalist = []

    datalist.append(data['starttime'])
    datalist.append(data['endtime'])
    datalist.append(data['recurring_bit'])
    datalist.append(data['recurring_interval'])
    datalist.append(data['appname'])
    datalist.append(ast.literal_eval(data['sid']))

    task.append(datalist)

#-----------------------------------------------------------------------------------------------#

# Function for Receiving Task from PLATFORM MANAGER

def get_data(Client):
    global task
    req = Client.recv(2048).decode()
    # print(req, type(req))
    # req=req.replace("'",'"')
    
    # print(req, type(req))
    # req=json.dumps(req)
    # req="'"+req+"'"
    # data = json.loads(req)
    # #data=ast.literal_eval(req)
    # print(data, type(data))
    lst=req.split("*")
    data={}
    data["starttime"]=lst[0]
    data["endtime"]=lst[1]
    data["recurring_bit"]=lst[2]
    data["recurring_interval"]=lst[3]
    data["appname"]=lst[4]+".py"
    data["sid"]=lst[5]
    sid = ast.literal_eval(data['sid'])

    for s in sid:
        if s == "False":
            resp = "Failed"
            Client.sendall(resp.encode())
            return

    #task.append(data)
    push_task(data)
    resp = "RECEIVED"
    Client.sendall(resp.encode())
    #temp=data["appname"]+".py"
    generate_algo_file(Client, data["appname"])

#-----------------------------------------------------------------------------------------------#

# NOT CURRENTLY IN USE

def importName(modulename, name):
    """ 
    Import a named object from a module in the context of this function.
    """
    try:
        module = __import__(modulename, globals(), locals(  ), [name])
    except ImportError:
        return None
    return vars(module)[name]

#-----------------------------------------------------------------------------------------------#

# Function for requesting HOST and PORT from SERVER_MANAGER

def server_manager():
    IP = "127.0.0.1"
    PORT = 2121    #@@@@@@
    sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sockfd.connect((IP, PORT))
    sockfd.send("get_server_instance".encode())  
    resp = sockfd.recv(1024).decode()
    sockfd.close()
    IP, PORT = resp.split(":")
    print("#-> INSTANCE ALLOCATED")
    return IP, int(PORT)

#-----------------------------------------------------------------------------------------------#

# Function for sending FREE INSTANCE REQ to SERVER_MANAGER

def free_server_instance(host, port):
    IP = "127.0.0.1"
    PORT = 2121    #@@@@@@
    sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sockfd.connect((IP, PORT))
    sockfd.send( ("free_server|" + str(host) + "|" + str(port)).encode() )  
    sockfd.close()
    print("\n#-> INSTANCE FREED")

#-----------------------------------------------------------------------------------------------#

# Function for opening TERMINAL UI for ENDUSER

def open_terminal_UI(appname, endtime, starttime):
    print(starttime)
    print(endtime)
    #os.system("gnome-terminal -x python _" + appname + " " + str(float(endtime) - float(starttime)))
    os.system("gnome-terminal -x python3 " + "tesp.py" + " " + str(float(endtime) - float(starttime)))


#-----------------------------------------------------------------------------------------------#

# Function to run the scheduled process (Will create copy of app.py -> replace IP,PORT,SID -> Call open_terminal_UI -> Call free instance)

def connect(starttime, endtime, appname, sids):
    #print("CONNECT KE ANDAR\n\n")
    HOST, PORT = server_manager()

    HOST = '127.0.0.1'   #@@@@@
    PORT = 5000          #@@@@@  

    #----> app.py ki nayi copy (myApp.py)
    #----> myApp.py IP PORT replace HOST PORT
    #----> how to send endtime to myApp.py

    s = ""
    if len(sids)>1:
        for item in sids:
            s += str (item) + "/"
        s = s[:-1]
    else:
        s = sids

    #print(appname + "connect ke andar aagya he")
    fin = open(appname, "rt")
    fout = open("_" + appname, "wt")
    
    for line in fin:
        fout.write(line.replace('IP', HOST).replace('PORT', str(PORT)).replace("sensorID1", s))
    fin.close()
    fout.close()

    tempHost = '127.0.0.1'
    tempPort = 5656

    sockfd = socket.socket()
    sockfd.bind((tempHost, tempPort))

    #print('\nListening for Server Response...')
    sockfd.listen(10)

    start_new_thread(open_terminal_UI, (appname, endtime, starttime))

    while time.time() <= float(endtime):
        print("--------- NOMPS ========")
        Client, address = sockfd.accept()
        resp = Client.recv(2048).decode()
        if resp=="4":
            break

    sockfd.close()
    print("SOCKET CLOSED\n")
    free_server_instance(HOST, PORT)

#-----------------------------------------------------------------------------------------------#

# Function will sort the task list and pick the process with start_time = current_time

def schedule():
    global task
    while len(task)!=0:
        st = time.time()
        #sort
        task.sort(key=lambda task:task[0])

        while len(task)!=0 and st >= float(task[0][0]):
            l = task[0]
            #print("WHILE KE ANDAR\n\n")

            if l[2]==1:
                #print("RECUR KE ANDAR\n\n")

                temp = task[0]
                if l[3]==-1:
                    temp[0] = temp[0] + 86400
                    temp[1] = temp[1] + 86400
                else:
                    temp[0] = temp[0] + l[3]
                    temp[1] = temp[1] + l[3]
                    print(temp[0])
                    print(temp[1])
                    print("-----------------")
                task.append(temp)

            task=task[1:]
            start_new_thread(connect, (l[0],l[1],l[4],l[5]))

#-----------------------------------------------------------------------------------------------#

# It is used to create the server side for get_data function to listen continously for task from PLATFORM MANAGER

def process_request():
    global IP
    global PORT
    sockfd = socket.socket()
    sockfd.bind((IP, PORT))

    print('\nListening ...\n')
    sockfd.listen(10)
    start_new_thread(schedule, ())

    while True:
        Client, address = sockfd.accept()
        start_new_thread(get_data, (Client,))

    sockfd.close()

#-----------------------------------------------------------------------------------------------#

# ME TO MAIN HUN

def main():
    start_new_thread(process_request, ())
    while(True):
        schedule()

#-----------------------------------------------------------------------------------------------#

main()

