import numpy as np
import pandas as pd
import scipy.linalg as la
from numpy.linalg import matrix_power
import matplotlib.pyplot as plt
import heapq
import sys
import math
import csv

# the object for Customer
# You have to change the object Customer
class Customer:
    # key is arrival time / departure time
    def __init__(self, ID, carrier, arrival_time, service_time):
        self.ID = ID #id
        self.carrier = carrier #carrier flag
        self.arrival_time = arrival_time #arrival time
        self.service_time = service_time #dining time
        self.hunger = 0 #hunger level
        
    def __lt__(self,other):
        return self.ID < other.ID
	
# the object for Server
# 1 Server = 1 Table
# You have to change the object Server
class Server: #This is server act as table
    def __init__(self, ID, size):
        self.full = False
        self.idle = True
		
        self.ID = ID
        self.size = size
		
        self.serving_list = []
        self.departure_time_list = []
        
    def push(self, cID, departure_time): #put customer into table
        self.serving_list.append(cID)
        self.departure_time_list.append(departure_time)
		
        self.idle = False
        if len(self.serving_list)>=self.size:
            self.full = True
        
    def pull(self, index): #pull customer out of table
        self.serving_list.pop(index)
        self.departure_time_list.pop(index)
		
        self.full = False
        if len(self.serving_list)<=0:
            self.idle = True
			
    def check_carrier(self, customer_list): #check and update the carrier flag of customer
        flag = 0
        for item in self.serving_list:
            if customer_list[item-1].carrier == 1:
                flag = 1
        if flag == 1:
            for item in self.serving_list:
                customer_list[item-1].carrier = 1
		
def timing_routine(tmp_customer, customer_queue, server_list, curr_time, total_server, termination_time): 
    # determine time of next event
    # please take referece to the code in introduction to basic queueing system
    # step 0: you need to change the argument list above
    
	# step 1: determine the time based on customer_list
    min_queue_time = termination_time * 100
    if len(tmp_customer) > 0:
        min_queue_time = tmp_customer[0].arrival_time
		
    # step 2: determine the time based on the queueing
    if len(customer_queue) > 0:
        min_queue_time = min(customer_queue[0].arrival_time, min_queue_time)
	
    # step 3: determine the time based on the servers (tables)
    if not min_queue_time == termination_time * 100: #if customer_queue and tmp_customer are not empty, then run this
        min_server_time = termination_time * 100
        for item in server_list:
            if not item.full:
                min_server_time = curr_time
                break
            for time in item.departure_time_list:
                if time < min_server_time:
                    min_server_time = time
					
		# return the time advanced at the end			
        if min_server_time > min_queue_time:
            curr_time = min_server_time
        else:
            curr_time = min_queue_time
	
    if min_queue_time == termination_time * 100: #if customer_queue and tmp_customer are empty, then run this
        min_server_time = termination_time * 100
        for item in server_list:
            for time in item.departure_time_list:
                if time < min_server_time:
                    min_server_time = time
                #print("***",time,"***")	
				
		# return the time advanced at the end				
        curr_time = min_server_time
		
    #print("---",curr_time,"---")

    return curr_time
	
def call_event(customer_list, tmp_customer, customer_queue, server_list, curr_time, total_server, record, day, dining, finish):
    # extract and evaulate all jobs happens at current time
    # please take referece to the code in introduction to basic queueing system
    # step 0: you need to change the argument list above
	
    # step 1: arrival events, customers arrive and wait in the queue
    while len(tmp_customer) > 0 and tmp_customer[0].arrival_time <= curr_time:
        arrival_task = tmp_customer.pop(0)
        clock_time = str(math.floor(arrival_task.arrival_time/60)+11)+":"+str(arrival_task.arrival_time%60)
        record = record.append({"Day":day, "Time":clock_time, "Event":"Customer "+str(arrival_task.ID)+" arrives", "len(Queue)":len(customer_queue), "Number_dining":dining, "Number_finish":finish}, ignore_index=True)
        customer_queue.append(arrival_task)
        print("time ", arrival_task.arrival_time, "ID", arrival_task.ID, "curr_time", curr_time)
		
    # step 2: departure events, customers finish dining and leave the servers (tables)
    k = 0
    for item in server_list:
        if not item.idle:
            h = 0
            for time in item.departure_time_list:
                if time <= curr_time:
                    dining = dining - 1
                    finish = finish + 1
                    clock_time = str(math.floor(time/60)+11)+":"+str(time%60) #convert time value to clock time hh:mm
                    record = record.append({"Day":day, "Time":clock_time, "Event":"Customer "+str(item.serving_list[h])+" dined at table "+str(k+1)+" departs", "len(Queue)":len(customer_queue), "Number_dining":dining, "Number_finish":finish}, ignore_index=True)
                    print("Departure time = ", time, "Customer ID = ", item.serving_list[h], "Table = ", k+1, "curr_time = ", curr_time)
                    item.pull(h)
                h = h + 1
        k = k + 1

    # step 3: process the customer_queue, moving customers from queue to the servers (tables)
    table = int(np.random.uniform(total_server)) #randomise the table number
    #print(customer_queue[0].arrival_time , curr_time)
    for i in np.arange(total_server):
        if not server_list[table].full and len(customer_queue) > 0 and customer_queue[0].arrival_time <= curr_time:
            arrival_task = customer_queue.pop(0)
            server_list[table].push(arrival_task.ID, arrival_task.service_time + curr_time)
            dining = dining + 1
            server_list[table].check_carrier(customer_list)
            #print("push table = ", table+1,"ID = ", arrival_task.ID,"dep_time = ", arrival_task.service_time + arrival_task.arrival_time, "service_time = ", arrival_task.service_time , "curr_time = ", curr_time)
            #print(server_list[table].serving_list, server_list[table].departure_time_list)
            #print("---")
            break;
				
        if table >= total_server-1:
            table = -1
				
        table = table + 1;
			
    return customer_list, tmp_customer, customer_queue, server_list, record, dining, finish
	
def post_process(day, customer_list, tmp_customer, customer_queue, server_list, record_result, hungry_remove, carrier_remove):
    # post-process the simulation at the end of ond day
    tmp_carrier = []
    tmp_hunger = []
    num_carrier = 0
    # step 0: you need to design the argument list above
	
    # step 1: process the customer queue, all customers in queue need to leave, they cannot dine in this canteen
    # step 1b: remember to handle the "hunger" issue, consecutive k days of "hunger"
    for item in customer_queue:
        customer_list[item.ID-1].hunger = customer_list[item.ID-1].hunger + 1
        if customer_list[item.ID-1].hunger >= k:
            hungry_remove.append(item.ID)
            tmp_hunger.append(item.ID)
        #print(item, item.hunger)
    #print(len(customer_queue))
	
    # step 2: process the servers (tables), depart the customers based on the increasing order of customers' leaving time
	#done at outside of this function (at line 411-420)
	
    # step 3: process the tests for flu, remove having p chance removing the customer from the system if they have flu
    for item in customer_list:
        if item.carrier == 1:
            if not item.arrival_time == -1:
                #print(item.ID)
                index = int(np.random.uniform(100))
                if index < p*100:
                    carrier_remove.append(item.ID)
                    tmp_carrier.append(item.ID)
                    print(item.ID)
                num_carrier = num_carrier + 1

    count = 0
    for item in customer_list:
        if not item.arrival_time == -1: 
            count = count + 1
			
    # step 4: prcoess the record for the summary of one day simulation
    summary_dict = {"Day":day, "ni":count, "Number_finish":str(count-len(tmp_hunger)), "Carrier":num_carrier-len(tmp_carrier), "Carrier_removed":tmp_carrier, "Hunger":len(customer_queue), "Hunger_removed":tmp_hunger}
	
    return customer_list, tmp_customer, customer_queue, server_list, record_result, summary_dict, hungry_remove, carrier_remove

# main program

# initialization

# setting some default numbers
# finish all default numbers
seed_number = 0
D = 100
N = 100
T_size = 12
T_detail = [2, 2, 2, 2, 2, 2, 4, 4, 4, 4, 4, 4]
p = 0.8
k = 2
customer_list = []
server_list = []
Customer_file = "customer_seed0.csv"
Result_file = "simulation_result.csv"
Summary_file = "simulation_summary.csv"

#Output the config file of seed0
open("config_seed0.csv", 'a')

rows = [ ['D', D], 
         ['N', N], 
         ['T_size', T_size], 
         ['T_detail', str(T_detail)], 
         ['p', p], 
         ['k', k],
         ['Customer_file', Customer_file],
         ['Result_file', Result_file],
         ['Summary_file', Summary_file],] 
  
with open("config_seed0.csv", 'w') as f:
      
    # using csv.writer method from CSV package
    write = csv.writer(f)
    write.writerows(rows)

# if the user specify the file name
# update the parameters based on the file
if len(sys.argv) == 2:
	config = pd.read_csv(sys.argv[1], header=None, index_col=0, skip_blank_lines=True)
    # print this out and see
	
	print(int(config[1].D))
	D = int(config[1].D)
	
	print(int(config[1].N)) 
	N = int(config[1].N)
	
	print(int(config[1].T_size)) 
	T_size = int(config[1].T_size)
	
	print((config[1].T_detail).split(",")) 
	T_detail = (config[1].T_detail).split(",")
	
	print(float(config[1].p)) 
	p = float(config[1].p)
	
	print(int(config[1].k)) 
	k = int(config[1].k)
	
	print(str(config[1].Customer_file)) 
	Customer_file = str(config[1].Customer_file)
	
	print(str(config[1].Result_file)) 
	Result_file = str(config[1].Result_file)
	
	print(str(config[1].Summary_file)) 
	Summary_file = str(config[1].Summary_file)
    
    # remember to initialize the customer based on the Customer_file
	
	customer_data = pd.read_csv(Customer_file, header=0, index_col=False, skip_blank_lines=True)
	
	#put data into the list
	for i in np.arange(N):
		#print(int(customer_data.dining_time[i]))
		tmp_customer = Customer(int(customer_data.customer_id[i]), int(customer_data.carrier[i]), 0, int(customer_data.dining_time[i]))
		customer_list.append(tmp_customer)
    
else:    
	np.random.seed(seed_number)
    
	# initialize the customer based on random
	record_customer = pd.DataFrame([], columns = ["customer_id", "dining_time", "carrier"])
	
	for i in np.arange(N):
		if i%10 == 0:
			tmp_carrier = 1
		else: 
			tmp_carrier = 0
		tmp_service_time = min(30+math.floor(np.random.exponential(20)), 120)
		record_customer = record_customer.append({"customer_id":str(i+1), "dining_time":tmp_service_time, "carrier":tmp_carrier}, ignore_index=True)
		tmp_customer = Customer(i+1, tmp_carrier, 0, tmp_service_time)
		customer_list.append(tmp_customer)
		
print(customer_list[0].ID, customer_list[0].carrier, customer_list[0].arrival_time, customer_list[0].service_time)
print(customer_list[N-1].ID, customer_list[N-1].carrier, customer_list[N-1].arrival_time, customer_list[N-1].service_time)

# You need to initialize servers (tables)

for i in np.arange(T_size):
	tmp_server = Server(i+1, int(T_detail[i]))
	server_list.append(tmp_server)
	server_list[i].serving_list = []
	server_list[i].departure_time_list = []
	
print(server_list[0].ID, server_list[0].size, server_list[0].serving_list, server_list[0].departure_time_list)
print(server_list[T_size-1].ID, server_list[T_size-1].size, server_list[T_size-1].serving_list, server_list[T_size-1].departure_time_list)

# I believe these two lines are useful
record_result = pd.DataFrame([], columns = ["Day", "Time", "Event", "len(Queue)", "Number_dining", "Number_finish"])
record_summary = pd.DataFrame([], columns = ["Day", "ni", "Number_finish", "Carrier", "Carrier_removed", "Hunger", "Hunger_removed"])

# main simulation
seed_number = 1
col_name = "arrival_time_day_"
tmp_customer = []
hungry_remove = []
carrier_remove = []

while seed_number <= D:
    np.random.seed(seed_number)
    print("Day ",seed_number)
    
    # if we use random numbers for input
    if len(sys.argv) != 2:
        time_list = []
        name = col_name + str(seed_number)
        # generate the arrival time for the customers
        for i in np.arange(N):
            customer_list[i].arrival_time = int(np.random.uniform(120))
            tmp_time = str(math.floor(customer_list[i].arrival_time/60)+11)+":"+str(customer_list[i].arrival_time%60)
			
			#set the removed customer arrival_time to -1
            for item in hungry_remove:
                if customer_list[i].ID == item:
                    tmp_time = -1
                    customer_list[i].arrival_time = -1
            for item in carrier_remove:
                if customer_list[i].ID == item:
                    tmp_time = -1
                    customer_list[i].arrival_time = -1
					
            time_list.append(tmp_time)
            #print(customer_list[i].arrival_time)
        record_customer[name] = time_list
    
    else:
		# set the arrival time for the customers, if needed
        for i in np.arange(N):
            arrival_time_name = col_name + str(seed_number)
            #print(arrival_time_name)
            #print(customer_data[arrival_time_name][i])
            (h, m) = (customer_data[arrival_time_name][i]).split(':')
            #print((int(h)-11)*60+int(m))
            customer_list[i].arrival_time = (int(h)-11)*60+int(m)
			
			#set the removed customer arrival_time to -1
            for item in hungry_remove:
                if customer_list[i].ID == item:
                    customer_list[i].arrival_time = -1
            for item in carrier_remove:
                if customer_list[i].ID == item:
                    customer_list[i].arrival_time = -1
		
    # think about in your design, do you have any initialization steps at the begining of each day
    # do the initialization for empty-and-idle state here
	
    tmp_customer = customer_list.copy()   
    tmp_customer.sort(key=lambda X: X.arrival_time)
	
	#remove the blacklisted customer from the list today
    while len(tmp_customer) > 0 and tmp_customer[0].arrival_time == -1:
        tmp_customer.pop(0)

	#server list initialize
    server_list = []
    for i in np.arange(T_size):
        tmp_server = Server(i+1, int(T_detail[i]))
        server_list.append(tmp_server)
        server_list[i].serving_list = []
        server_list[i].departure_time_list = []
	
    #for tmp in tmp_customer:
        #print("customer " + str(tmp.ID) + " arrivals at " + str(tmp.arrival_time) + " need " + str(tmp.service_time) + " time unit for the task.")
	
    curr_time = 0
    np.random.seed(seed_number+10000)
    customer_queue = []
    termination_time = 180
    remain = 0
    dining = 0
    finish = 0
	
    while curr_time < termination_time or len(customer_queue) > 0:
        # determine the time
        # you need to update the argument list
        curr_time = timing_routine(tmp_customer, customer_queue, server_list, curr_time, T_size, termination_time)
    
        # this is important, when it is 2:00 pm, customer in the queue cannot get into the canteen
        if curr_time >= termination_time:
            break
			
        # update different lists and log sheet
        # you need to update the argument list
        customer_list, tmp_customer, customer_queue, server_list, record_result, dining, finish = call_event(customer_list, tmp_customer, customer_queue, server_list, curr_time, T_size, record_result, seed_number, dining, finish)
	
	#count the number of remaining customer in table after 02:00pm
    for item in server_list:
        for time in item.departure_time_list:
            #print("***",time,"***")
            remain = remain + 1
	
	#final departure after 02:00pm
    for i in np.arange(remain):
        curr_time = timing_routine(tmp_customer, customer_queue, server_list, curr_time, T_size, termination_time)
        customer_list, tmp_customer, customer_queue, server_list, record_result, dining, finish = call_event(customer_list, tmp_customer, customer_queue, server_list, curr_time, T_size, record_result, seed_number, dining, finish)
	
    # post-processing and update the summary
    np.random.seed(seed_number+20000)

    # You need to update the arguemnt list below
    customer_list, tmp_customer, customer_queue, server_list, record_result, summary_dict, hungry_remove, carrier_remove = post_process(seed_number ,customer_list, tmp_customer, customer_queue, server_list, record_result, hungry_remove, carrier_remove)
    record_summary = record_summary.append(summary_dict, ignore_index=True)
	
    seed_number = seed_number + 1
	
if len(sys.argv) != 2:
    # output the customer record, if the customer record is generated by this program
    record_customer.to_csv(Customer_file, index = False)
	
record_result.to_csv(Result_file, index = False)
record_summary.to_csv(Summary_file, index = False)