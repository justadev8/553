import gmpy2
import random
import csv
from gmpy2 import mpfr
from gmpy2 import mpz
from gmpy2 import rint_round
from collections import deque
import bitstring
from bitstring import BitArray

gmpy2.get_context().precision = 2048*2
floating_precision = 64

number_of_processes = 30
number_of_events_process = 50
acceptable_difference =0.01
file_delimiter="|"
rerun = "Y"
event_file_name ="events.txt"

def getPrecisionWithLimit(num):
    return num
    #print(num)
    #print( mpfr(BitArray(float=num, length=floating_precision).float))
    #return mpfr(BitArray(float=num, length=floating_precision).float)

def generate_event():
    if rerun == "Y":
        return readEvent()
    else :
        futureLogicalTaskTime = 0
        internalEventCount=0
        externalEventCount=0
        eventList = []
        for eventId in range(1, (number_of_processes * number_of_events_process) + 1):
            senderProcess = getRandomProcess()
            if random.randint(1, 3) == 1:
                # internal event
                event = Event(eventId, "I", senderProcess, "", futureLogicalTaskTime)
                # print event
                eventList.append(event)
                internalEventCount+=1
            else:
                event = Event(eventId, "E", senderProcess, getRandomReceiveProcess(senderProcess), futureLogicalTaskTime)
                # print event
                eventList.append(event)
                externalEventCount+=1
            futureLogicalTaskTime = futureLogicalTaskTime + 1
        print("Count of internal events:{}".format(internalEventCount))
        print("Count of external events:{}".format(externalEventCount))
        saveEvent(eventList)
        return eventList

def saveEvent(eventList):
    file_handle=open(event_file_name, "w")

    eventListStr = []
    for event in eventList:
        file_handle.write(str(event.eventId)+ file_delimiter+ event.eventType + file_delimiter + str(event.sendProcessId)+file_delimiter+str(event.receiveProcessId)+file_delimiter+str(event.sendStartTime)+"\n")

    file_handle.close()

def readEvent():
    records =open(event_file_name, "r").read().split('\n')
    eventList = []
    for record in records:
        if record != '':
            eventId, eventType, senderProcessId, receiverProcessId, senderStartTime  = record.split("|")
            if receiverProcessId == '':
                receiverProcessId=-1
            eventList.append(Event(int(eventId), eventType, int(senderProcessId), int(receiverProcessId), int(senderStartTime)))
    #records.close()
    return eventList

def roundAntiLogAndReturn(arg):
    # if abs(rint_round(gmpy2.exp(arg))-gmpy2.exp(arg)) < acceptable_difference :
    #     return getPrecisionWithLimit(gmpy2.log(rint_round(gmpy2.exp(arg))))
    # else:
    return getPrecisionWithLimit(arg)

def getRandomProcess():
    return random.randint(1, number_of_processes)

def getRandomReceiveProcess(process_to_be_avoided):
    while True:
        receiver_process = random.randint(1, number_of_processes)
        if process_to_be_avoided != receiver_process:
            return receiver_process


def isPrime(number):
    for x in range(2, number):
        if number % x == 0:
            # print "{} is not prime".format(number)
            return False
    # print "{} is prime".format(number)
    return True


def getPrimeNumbers():
    primeNumbers = []
    count = 0
    prime_number = 2
    while count < number_of_processes:
        while isPrime(prime_number) == False:
            prime_number = prime_number + 1
        primeNumbers.append(prime_number)
        prime_number = prime_number + 1
        count = count + 1
    return primeNumbers


def copyOf(object_array):
    new_reference = []
    for x in object_array:
        new_reference.append(x)
    return new_reference

def mpfrCopyOf(object):
    return mpfr(object)

def getProcesses():
    process_list = []
    primeNumbers = getPrimeNumbers()
    primeNumberIndex = 0
    for processId in range(1, number_of_processes + 1):
        process_list.append(Process(processId, primeNumbers[primeNumberIndex]))
        primeNumberIndex = primeNumberIndex + 1
    return process_list


class TimeStamp(object):
    def __init__(self, vectorClock, primeClock, logClock):
        super(TimeStamp, self).__init__()
        self.vectorClock = vectorClock
        self.primeClock = primeClock
        self.logClock = logClock

    def __str__(self):
        return "Log Clock: {}, Prime Clock: {}, Vector Clock:{}".format(self.logClock, self.primeClock,
                                                                        self.vectorClock)

class Event(object):
    """docstring for Event"""

    def __init__(self, eventId, eventType, sendProcessId, receiveProcessId, sendStartTime):
        super(Event, self).__init__()
        self.eventId = eventId
        self.eventType = eventType
        self.sendProcessId = sendProcessId
        self.receiveProcessId = receiveProcessId
        self.sendStartTime = sendStartTime
        self.receiveStartTime = -1

    def __str__(self):
        return "Event Id:{}, Event Type: {}, Sender Process: {}, Receiver Process:{}, Start Time:{}".format(
            self.eventId, self.eventType, self.sendProcessId, self.receiveProcessId, self.sendStartTime)

def getGCD(a,b):
    return gmpy2.gcd(mpz(rint_round(a)), gmpy2.mpz(rint_round(b)))

def getLCM(a,b):
    return gmpy2.lcm(mpz(a), mpz(b))

def multiply(a,b):
    return gmpy2.mul(a,b)

def add(a,b):
    return gmpy2.add(a,b)

def sub(a,b):
    return gmpy2.sub(a,b)

def log(a):
    return gmpy2.log(a)

def antilog(a):
    return gmpy2.exp(a)

def isEventCausal_VectorClock(array1, array2):
    for i in range(len(array1)):
        if array1[i] < array2[i]:
            return False
    return True

def isEventCausal_PrimeClock(prime1, prime2):
    if prime1 % prime2 ==0:
        return True
    return False

def isEventCausal_LogClock(log1, log2):
    diff = sub(log1, log2)
    if diff < 0:
        return False
    if abs(rint_round(gmpy2.exp(diff)) - gmpy2.exp(diff)) < acceptable_difference:
        return True
    return False

def compareInternalEvent(event1, event2):
    timestamp1 = event1.SendTimeStamp
    timestamp2 = event2.SendTimeStamp

    if compareAndReturnResult(timestamp1,timestamp2) == False:
        #print("Not matched")
        compareAndReturnResult(timestamp1, timestamp2)
        return False

    return True

def compareAndReturnResult(timestamp1, timestamp2):
    isVectorClockCausal = isEventCausal_VectorClock(timestamp1.vectorClock, timestamp2.vectorClock)
    isPrimeClockCausal = isEventCausal_PrimeClock(timestamp1.primeClock, timestamp2.primeClock)
    isLogClockCausal = isEventCausal_LogClock(timestamp1.logClock, timestamp2.logClock)

    if isVectorClockCausal == isLogClockCausal:
        #matched_count+=1
        return True
    else :
        return False

def compareInternalAndExternalEvent(internalEvent, externalEvent):
    timestamps = [internalEvent.SendTimeStamp, externalEvent.SendTimeStamp, externalEvent.ReceiveTimeStamp]

    for timestamp1 in timestamps:
        for timestamp2 in timestamps:
            if timestamp1 != timestamp2:
                if compareAndReturnResult(timestamp1, timestamp2) == False:
                    #print("Not matched")
                    compareAndReturnResult(timestamp1, timestamp2)
                    return False
    return True


def compareExternalEvents(event1, event2):
    sendTimestamp1 = event1.SendTimeStamp
    sendTimestamp2 = event2.SendTimeStamp
    receiveTimestamp1 = event1.ReceiveTimeStamp
    receiveTimestamp2 = event2.ReceiveTimeStamp

    timestamps  = [sendTimestamp1, sendTimestamp2, receiveTimestamp1, receiveTimestamp2]

    for timestamp1 in timestamps:
        for timestamp2 in timestamps:
            if timestamp1 != timestamp2:
                if compareAndReturnResult(timestamp1, timestamp2) == False:
                   # print("Not matched")
                    compareAndReturnResult(timestamp1, timestamp2)
                    return False

    return  True

# def compareEvents(eventList):
#     matched_count=0
#     unmatched_count=0
#     for event1 in eventList:
#         for event2 in eventList:
#             if event1 != event2:
#                 if event1.eventType =='I' and event2.eventType =='I' :
#                     if compareInternalEvent(event1, event2) == True:
#                         matched_count+=1
#                         #print("matched count={}".format(matched_count))
#                     else:
#                         unmatched_count+=1
#                         print("unmatched count={}".format(unmatched_count))
#                 elif (event1.eventType =='E' and event2.eventType =='I') or (event1.eventType =='I' and event2.eventType =='E'):
#                     if event1.eventType=='I':
#                         if compareInternalAndExternalEvent(event1, event2) == True:
#                             matched_count+=1
#                           #  print("matched count={}".format(matched_count))
#                         else:
#                             unmatched_count+=1
#                             print("unmatched count={}".format(unmatched_count))
#                     else:
#                         if compareInternalAndExternalEvent(event2, event1) == True:
#                             matched_count+=1
#                            # print("matched count={}".format(matched_count))
#                         else:
#                             unmatched_count+=1
#                             print("unmatched count={}".format(unmatched_count))
#                 else:
#                     if compareExternalEvents(event1, event2) == True:
#                         matched_count+=1
#                       #  print("matched count={}".format(matched_count))
#                     else:
#                         unmatched_count+=1
#                         print("unmatched count={}".format(unmatched_count))
#     print("Matched count:{}".format(matched_count))
#     print("Unmatched count:{}".format(unmatched_count))

def comparePrimeAndLog(number, log):
    gmpy2.get_context().precision = 80000
    if abs(number - gmpy2.exp(log)) < acceptable_difference:
        return  True
    else:
        return  False

def compareEvents(eventList):
    matched_count=0
    unmatched_count=0
    for event in eventList:
        if event.eventType == 'I':
            if comparePrimeAndLog(event.SendTimeStamp.primeClock, event.SendTimeStamp.logClock) == True:
                matched_count+=1
            else:
                unmatched_count+=1
        else:
            if comparePrimeAndLog(event.ReceiveTimeStamp.primeClock, event.ReceiveTimeStamp.logClock) == True:
                matched_count+=1
            else:
                unmatched_count+=1

            if comparePrimeAndLog(event.SendTimeStamp.primeClock, event.SendTimeStamp.logClock) == True:
                matched_count+=1
            else:
                unmatched_count+=1


    print("Matched count:{}".format(matched_count))
    print("Unmatched count:{}".format(unmatched_count))

class Process(object):
    """docstring for Process"""
    biggest_prime_clock = 0
    biggest_log_clock = 0p

    def __init__(self, processId, primeNumber):
        super(Process, self).__init__()
        self.processId = processId
        self.primeNumber = mpfr(primeNumber)
        self.logicalTime = 0
        self.vectorClock = [0 for index in range(1, number_of_processes + 1)]
        self.primeClock = mpfr(1)
        self.logClock = gmpy2.log(1)
        self.logPrime = gmpy2.log(primeNumber)
        self.queue = deque()
        self.receiver_queue = deque()

    def set_other_processes_instances(self, instances):
        self.instances = instances

    def internal_event(self, event):
        self.vectorClock[self.processId - 1] += 1
        self.primeClock = multiply(self.primeClock, self.primeNumber)
        if self.primeClock > Process.biggest_prime_clock:
            Process.biggest_prime_clock = self.primeClock

        self.logClock = add(self.logClock ,self.logPrime)
        self.logClock = roundAntiLogAndReturn( self.logClock )
        if self.logClock > Process.biggest_log_clock:
            Process.biggest_log_clock = self.logClock

        self.logicalTime += 1
        event.SendTimeStamp = TimeStamp(copyOf(self.vectorClock), mpfrCopyOf(self.primeClock), mpfrCopyOf(self.logClock))

    def send_event(self, event):
        self.vectorClock[self.processId - 1] += 1
        self.primeClock = multiply(self.primeClock, self.primeNumber)
        if self.primeClock > Process.biggest_prime_clock:
            Process.biggest_prime_clock = self.primeClock

        self.logClock = add(self.logClock , self.logPrime)
        self.logClock = roundAntiLogAndReturn(self.logClock)
        if self.logClock > Process.biggest_log_clock:
            Process.biggest_log_clock = self.logClock

        event.SendTimeStamp = TimeStamp(copyOf(self.vectorClock), mpfrCopyOf(self.primeClock), mpfrCopyOf(self.logClock))
        event.receiveStartTime = self.logicalTime
        self.instances[event.receiveProcessId - 1].receiver_queue.append(event)
        self.logicalTime += 1

    def receive_event(self, event):
        sendTimeStamp = event.SendTimeStamp

        #setting the vector clock
        for index in range(len(sendTimeStamp.vectorClock)):
            if sendTimeStamp.vectorClock[index] > self.vectorClock[index]:
                self.vectorClock[index]=sendTimeStamp.vectorClock[index]

        self.vectorClock[self.processId - 1] += 1

        #setting prime number
        self.primeClock = getLCM(self.primeClock, sendTimeStamp.primeClock)
        self.primeClock = multiply(self.primeClock, self.primeNumber)
        if self.primeClock > Process.biggest_prime_clock:
            Process.biggest_prime_clock = self.primeClock


        #setting log clock
        gcd  = getGCD(antilog(self.logClock), antilog(sendTimeStamp.logClock))
        self.logClock = add(self.logClock, sendTimeStamp.logClock)
        self.logClock= sub(self.logClock, log(gcd))
        self.logClock = add(self.logClock, self.logPrime)
        self.logClock = roundAntiLogAndReturn(self.logClock)
        if self.logClock > Process.biggest_log_clock:
            Process.biggest_log_clock = self.logClock

        event.ReceiveTimeStamp = TimeStamp(copyOf(self.vectorClock), mpfrCopyOf(self.primeClock), mpfrCopyOf(self.logClock))
        #self.instances[event.receiveProcessId - 1].receiver_queue.append(event)
        self.logicalTime += 1

    def __str__(self):
        return "Process {} details - Prime Number : {}, Events :{}".format(self.processId, self.primeNumber,
                                                                       len(self.queue))

    def getEvent(self, logicalTime):
        if not self.queue and not self.receiver_queue:
            return None
        else:
            if self.receiver_queue:
                event = self.receiver_queue[0]
                if event.receiveStartTime <= logical_time:
                    return self.receiver_queue.popleft()
            if self.queue:
                event = self.queue[0]
                if event.sendStartTime <= logical_time:
                    return self.queue.popleft()

eventList = generate_event()
#saveEvent(eventList)
print([str(event) for event in eventList])
processList = getProcesses()

for process in processList:
    process.set_other_processes_instances(processList)

# distribute events to the process queue
for event in eventList:
    # print "event send process id :{}".format(event.sendProcessId)
    processList[event.sendProcessId - 1].queue.append(event)
print([str(process) for process in processList])

# start the processes

logical_time = 0
completed_events = 0

while logical_time <= number_of_processes * number_of_events_process:
    for process in processList:
        while True:
            event = process.getEvent(logical_time)
            if event is None:
                break

            if event.eventType == 'E':
                if process.processId == event.sendProcessId:
                    #sender process
                    process.send_event(event)
                    completed_events+=1
                else:
                    process.receive_event(event)

            else:
                process.internal_event(event)
                completed_events+=1

    logical_time += 1

print("Biggest prime clock :{}".format(Process.biggest_prime_clock))
print("Biggest log clock :{}".format(Process.biggest_log_clock.__str__()))
print("Starting with comparison")
compareEvents(eventList)