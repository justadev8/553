import gmpy2
import random
import csv
from gmpy2 import mpfr
from gmpy2 import mpz, gcd
from gmpy2 import rint_round
from collections import deque


floating_precision = 4000

number_of_processes = 10
number_of_events_process = 10
#acceptable_difference = pow(10, -15)
acceptable_difference = 0.000000000000000000001
integral_precision_scratch_space = number_of_processes*number_of_events_process*100
logarithm_precision_persistent = 64
probability_internal=20

file_delimiter="|"
rerun = "Y"
event_file_name ="events.txt"

def set_integral_precision_scratch_space():

    gmpy2.get_context().precision=integral_precision_scratch_space

def set_logarithmic_precision_persistent():
    gmpy2.get_context().precision = logarithm_precision_persistent

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
            if random.randint(1, 100) < probability_internal and internalEventCount < ((number_of_processes * number_of_events_process)*probability_internal/100):
                # internal event
                event = Event(eventId, "I", senderProcess, "", futureLogicalTaskTime)
                # print event
                eventList.append(event)
                internalEventCount+=1
            else:
                if externalEventCount >= ((number_of_processes * number_of_events_process) - 100/probability_internal):
                    event = Event(eventId, "I", senderProcess, "", futureLogicalTaskTime)
                    # print event
                    eventList.append(event)
                    internalEventCount += 1
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
    def __init__(self, vectorClock, primeClock, logClock,primeNumber, receivedPrimes):
        super(TimeStamp, self).__init__()
        self.vectorClock = vectorClock
        self.primeClock = primeClock
        self.logClock = logClock
        self.primeNumber = primeNumber
        self.receivedPrimes = receivedPrimes
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
    num1 = mpz(rint_round(a))
    num2= mpz(rint_round(b))

    # if num2%num1 ==0:
    #     return num1

    gcd_a_b =  gcd(num1, num2)
    return gcd_a_b

def getLCM(a,b):
    return gmpy2.lcm(mpz(a), mpz(b))

def multiply(a,b):
    return gmpy2.mul(a,b)

def div(a,b):
    return gmpy2.div(a,b)

def add(a,b):
    return gmpy2.add(a,b)

def sub(a,b):
    return gmpy2.sub(a,b)

def log(a):
    return gmpy2.log(a)

def antilog(a):
    set_integral_precision_scratch_space()
    result = gmpy2.exp(a)
    set_logarithmic_precision_persistent()
    return result

#vh < vk ⇔ vh ≤ vk and ∃x : vh[x] < vk[x]
def isEventCausal_VectorClock(array1, array2):
    #define a variable for some index which has to be lower
    lower_index = -1

    for i in range(len(array1)):

        #check if the
        if array1[i] < array2[i]:
            lower_index = i
        elif array2[i] < array1[i]:
            return False

    if lower_index != -1 :
        return True
    else:
        return False


def isEventCausal_PrimeClock(prime1, prime2):
    if prime2 % prime1 ==0:
        return True
    return False

def nearest_multiple(number, multiple):
    remainder = number % multiple
    if remainder > div(multiple, 2):
        number = number + (multiple - remainder)
        # round up
    else:
        number = number - remainder
        # round down
    return number

def nearest_multiple_array(number, multiple_array):
    if number == 1:
        return number

    multiple = multiply_array(multiple_array)
    return nearest_multiple(number, multiple)

def resetLogToNearestMultiple(logClock, receivedPrimes):
    dec = antilog(logClock)
    dec = nearest_multiple(dec, multiply_array(receivedPrimes))
    return log(dec)

def multiply_array(num_array):
    result = mpfr(1);
    for number in num_array:
        result  = multiply(result, number)
    return result

def isEventCausal_LogClock(log1, log2, prime1, prime2, receivedPrimes1, receivedPrimes2):

    if log1 > log2:
        return False

    set_integral_precision_scratch_space()
    #convert log1 to nearest multiple of prime1
    dec1= antilog(log1)
    dec1 = nearest_multiple(dec1, multiply_array(receivedPrimes1))

    # convert log2 to nearest multiple of prime1
    dec2 = antilog(log2)
    dec2 = nearest_multiple(dec2,  multiply_array(receivedPrimes2))

    if dec2%dec1 == 0:
        set_logarithmic_precision_persistent()
        return True
    else:
        set_logarithmic_precision_persistent()
        return False
    # closeness = 0.00000000000001 * dec1
    # remainder = dec2 % dec1
    # quotient = dec2 / dec1
    # integral_quotient = rint_round(quotient)
    # if integral_quotient - quotient < 0.0000001:
    #     return True

    # if remainder  - closeness <0 or closeness+remainder > dec1:
    #     return True
    # return False
    # if dec2 % dec1 ==0:
    #     set_logarithmic_precision_persistent()
    #     return True
    # set_logarithmic_precision_persistent()
    # return False





    # diff = sub(log2, log1)
    # if diff < 0:
    #     return False
    #
    # isCausal = False
    #
    # exp_value = gmpy2.exp(diff)
    # rounded_exp_value  = rint_round(exp_value)
    #
    # a = multiply(exp_value, gmpy2.exp(log1))
    # b = multiply(rounded_exp_value, gmpy2.exp(log1))
    #
    # if div(abs(sub(a,b)), a) < acceptable_difference :
    #     isCausal = True
    #
    # return isCausal

# def isEventCausal_LogClock(log1, log2):
#
#     diff = sub(log2, log1)
#     if diff < 0:
#         return False
#
#     isCausal = False
#     if abs(sub(rint_round(gmpy2.exp(diff)), gmpy2.exp(diff))) < acceptable_difference:
#         isCausal =  True
#     return isCausal


def compareInternalEvent(event1, event2):
    timestamp1 = event1.SendTimeStamp
    timestamp2 = event2.SendTimeStamp

    result = compareAndReturnResult(timestamp1, timestamp2)
    if result != 0:
        #print("Not matched")
        compareAndReturnResult(timestamp1, timestamp2)
        return result
    return 0

def compareAndReturnResult(timestamp1, timestamp2):
    isVectorClockCausal = isEventCausal_VectorClock(timestamp1.vectorClock, timestamp2.vectorClock)
    isPrimeClockCausal = isEventCausal_PrimeClock(timestamp1.primeClock, timestamp2.primeClock)
    isLogClockCausal = isEventCausal_LogClock(timestamp1.logClock, timestamp2.logClock, timestamp1.primeNumber, timestamp2.primeNumber, timestamp1.receivedPrimes, timestamp2.receivedPrimes)

    if isVectorClockCausal == isLogClockCausal:
        #matched_count+=1
        return 0
    else :
        #if vector clock is true, and log is false, it is a wrong result
        # if vector clock is false, and log is true, it is a false positive
        if isVectorClockCausal is True:
            return 1
        else :
            return 2

def compareInternalAndExternalEvent(internalEvent, externalEvent):
    timestamps = [internalEvent.SendTimeStamp, externalEvent.SendTimeStamp, externalEvent.ReceiveTimeStamp]

    for outerIndex in range(0,len(timestamps)):
        timestamp1 = timestamps[outerIndex]
        innerIndex = outerIndex + 1
        while(innerIndex < len(timestamps)):
            timestamp2 = timestamps[innerIndex]
            result = compareAndReturnResult(timestamp1, timestamp2)
            if  result != 0 :
                #print("Not matched")
                compareAndReturnResult(timestamp1, timestamp2)
                return result
            innerIndex = innerIndex+1
    return 0
    #return True

def compareExternalEventAndInternal(externalEvent,internalEvent):
    timestamps = [externalEvent.SendTimeStamp, externalEvent.ReceiveTimeStamp,internalEvent.SendTimeStamp]

    for outerIndex in range(0,len(timestamps)):
        timestamp1 = timestamps[outerIndex]
        innerIndex = outerIndex + 1
        while(innerIndex < len(timestamps)):
             timestamp2 = timestamps[innerIndex]
             result = compareAndReturnResult(timestamp1, timestamp2)
             if result != 0 :
                 compareAndReturnResult(timestamp1, timestamp2)
                 return result
             innerIndex = innerIndex + 1
    return 0
    #return True


def compareExternalEvents(event1, event2):
    sendTimestamp1 = event1.SendTimeStamp
    sendTimestamp2 = event2.SendTimeStamp
    receiveTimestamp1 = event1.ReceiveTimeStamp
    receiveTimestamp2 = event2.ReceiveTimeStamp

    timestamps  = [sendTimestamp1, sendTimestamp2, receiveTimestamp1, receiveTimestamp2]

    for outerIndex in range(0,len(timestamps)):
        timestamp1 = timestamps[outerIndex]
        innerIndex = outerIndex + 1
        while(innerIndex < len(timestamps)):
            timestamp2 = timestamps[innerIndex]

            result = compareAndReturnResult(timestamp1, timestamp2)
            if result != 0:
                # print("Not matched")
                compareAndReturnResult(timestamp1, timestamp2)
                return result

            innerIndex=innerIndex+1
    return 0
    # return  True

def compareEvents(eventList):
    matched_count=0
    unmatched_count=0
    outer_event_index  = 0

    false_positives = 0
    wrong_results = 0

    while outer_event_index < len(eventList):
        print(outer_event_index)
        event1 = eventList[outer_event_index]
        inner_event_index = outer_event_index+1
        while inner_event_index < len(eventList):

            event2 = eventList[inner_event_index]

            if event1.eventType =='I' and event2.eventType =='I' :
                result = compareInternalEvent(event1, event2)

                if result == 0:
                    matched_count += 1
                elif result == 1:
                    wrong_results += 1
                else:
                    false_positives += 1
            elif (event1.eventType =='E' and event2.eventType =='I') or (event1.eventType =='I' and event2.eventType =='E'):


                if event1.eventType=='I':
                    result = compareInternalAndExternalEvent(event1, event2)
                    if result == 0:
                        matched_count += 1
                    elif result == 1:
                        wrong_results += 1
                    else:
                        false_positives += 1
                else:
                    result = compareExternalEventAndInternal(event1, event2)
                    if result == 0:
                        matched_count += 1
                    elif result == 1:
                        wrong_results += 1
                    else:
                        false_positives += 1
            else:
                result = compareExternalEvents(event1, event2)
                if result == 0:
                    matched_count+=1
                elif result == 1:
                    wrong_results+=1
                else:
                    false_positives+=1

            inner_event_index+=1
        outer_event_index+=1

    print("Matched count:{}".format(matched_count))
    print("Wrong results:{}".format(wrong_results))
    print("False positives:{}".format(false_positives))
    print("Matched percent :{}".format(matched_count*100/(matched_count+wrong_results+false_positives)))

def comparePrimeAndLog(number, log):
    gmpy2.get_context().precision = 80000
    if abs(number - gmpy2.exp(log)) < acceptable_difference:
        return  True
    else:
        return  False

# def compareEvents(eventList):
#     matched_count=0
#     unmatched_count=0
#
#     error_events=  set()
#
#     outer_index = 0
#     while outer_index <  len(eventList):
#         event1 = eventList[outer_index]
#
#         first_event_detection = None
#         isLogCausal = False
#         isVectorCausal  = False
#         inner_index = outer_index+1
#
#         while inner_index < len(eventList):
#             event2 = eventList[inner_index]
#
#             if event1.eventType == 'I' and event2.eventType == 'I':
#                 if isEventCausal_VectorClock(event1.SendTimeStamp.vectorClock, event2.SendTimeStamp.vectorClock) == True:
#                    # isVectorCausal = True
#                     if isEventCausal_LogClock(event1.SendTimeStamp.logClock, event2.SendTimeStamp.logClock) == True:
#                         #matched_count+=1
#                     #    isLogCausal= True
#                         if first_event_detection == None:
#                             first_event_detection = True
#                     else:
#                         if first_event_detection == None:
#                             first_event_detection = False
#                         elif first_event_detection == True:
#                             if event2.eventId not in error_events:
#                                 error_events.add(event2.eventId)
#                      #   isLogCausal=False
#             else:
#             #elif (event1.eventType =='E' and event2.eventType =='I') or (event1.eventType =='I' and event2.eventType =='E'):
#                 #if event1.eventType=='I':
#                 if isEventCausal_VectorClock(event1.SendTimeStamp.vectorClock,
#                                              event2.SendTimeStamp.vectorClock) == True:
#                     #isVectorCausal = True
#                     if isEventCausal_LogClock(event1.SendTimeStamp.logClock,
#                                               event2.SendTimeStamp.logClock) == True:
#                         if first_event_detection == None:
#                             first_event_detection = True
#                      #   isLogCausal = True
#                       #  break
#                     else:
#                         if first_event_detection == None:
#                             first_event_detection = False
#                         elif first_event_detection == True:
#                             if event2.eventId not in error_events:
#                                 error_events.add(event2.eventId)
#                       #  isLogCausal = False
#                        # break
#                             #only care about send event of event2. if event1 is not causal with send of event2, why would it be causal with receive event of event2
#
#             inner_index+=1
#
#         if (first_event_detection == True or first_event_detection==None) and (event1.eventId not in error_events):
#             matched_count+=1
#         else:
#             unmatched_count+=1
#
#         outer_index+=1
#
#     print("Matched count:{}".format(matched_count))
#     print("Unmatched count:{}".format(unmatched_count))

# def compareEvents(eventList):
#     matched_count=0
#     unmatched_count=0
#     for event in eventList:
#         if event.eventType == 'I':
#             if comparePrimeAndLog(event.SendTimeStamp.primeClock, event.SendTimeStamp.logClock) == True:
#                 matched_count+=1
#             else:
#                 unmatched_count+=1
#         else:
#             if comparePrimeAndLog(event.ReceiveTimeStamp.primeClock, event.ReceiveTimeStamp.logClock) == True:
#                 matched_count+=1
#             else:
#                 unmatched_count+=1
#
#             if comparePrimeAndLog(event.SendTimeStamp.primeClock, event.SendTimeStamp.logClock) == True:
#                 matched_count+=1
#             else:
#                 unmatched_count+=1
#
#
#     print("Matched count:{}".format(matched_count))
#     print("Unmatched count:{}".format(unmatched_count))




class Process(object):
    """docstring for Process"""
    biggest_prime_clock = 0
    biggest_log_clock = 0

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
        self.receivedPrimes = list()
        self.receivedPrimes.append(self.primeNumber)

    def set_other_processes_instances(self, instances):
        self.instances = instances

    # def reset_log_value(self,logClock, primeNumber, senderPrimeNumber):
    #     decAntilog = antilog(logClock)
    #     remainder = decAntilog%(primeNumber*senderPrimeNumber)
    #     if remainder > div(primeNumber*senderPrimeNumber,2):
    #         decAntilog = decAntilog+((primeNumber*senderPrimeNumber)-remainder)
    #         #round up
    #     else:
    #         decAntilog = decAntilog-remainder
    #         #round down
    #     return log(decAntilog)

    def internal_event(self, event):
        self.vectorClock[self.processId - 1] += 1
        self.primeClock = multiply(self.primeClock, self.primeNumber)
        if self.primeClock > Process.biggest_prime_clock:
            Process.biggest_prime_clock = self.primeClock

        self.logClock = add(self.logClock ,self.logPrime)
        if self.logClock > Process.biggest_log_clock:
            Process.biggest_log_clock = self.logClock

        self.logClock = resetLogToNearestMultiple(self.logClock, self.receivedPrimes)

        self.logicalTime += 1
        event.SendTimeStamp = TimeStamp(copyOf(self.vectorClock), mpfrCopyOf(self.primeClock), mpfrCopyOf(self.logClock), self.primeNumber, copyOf(self.receivedPrimes))

    def send_event(self, event):
        self.vectorClock[self.processId - 1] += 1
        self.primeClock = multiply(self.primeClock, self.primeNumber)
        if self.primeClock > Process.biggest_prime_clock:
            Process.biggest_prime_clock = self.primeClock

        self.logClock = add(self.logClock , self.logPrime)
        self.logClock = resetLogToNearestMultiple(self.logClock, self.receivedPrimes)

        if self.logClock > Process.biggest_log_clock:
            Process.biggest_log_clock = self.logClock

        event.SendTimeStamp = TimeStamp(copyOf(self.vectorClock), mpfrCopyOf(self.primeClock), mpfrCopyOf(self.logClock), self.primeNumber, copyOf(self.receivedPrimes))
        event.receiveStartTime = self.logicalTime
        self.instances[event.receiveProcessId - 1].receiver_queue.append(event)
        self.logicalTime += 1

    def receive_event(self, event):
        if event.eventId == 19:
            print("19 is here")
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
        decLogClock = nearest_multiple_array(antilog(self.logClock), self.receivedPrimes)
        decSenderLogClock = nearest_multiple_array(antilog(sendTimeStamp.logClock), sendTimeStamp.receivedPrimes)
        gcd = getGCD(decLogClock, decSenderLogClock)
        #gcd  = getGCD(antilog(self.logClock), antilog(sendTimeStamp.logClock))

        self.receivedPrimes.append(event.SendTimeStamp.primeNumber)

        self.logClock = add(log(decLogClock) , log(decSenderLogClock))
        self.logClock = sub(self.logClock, log(gcd))
        self.logClock = add(self.logClock, self.logPrime)
        self.logClock = resetLogToNearestMultiple(self.logClock, self.receivedPrimes)

        # self.logClock = add(self.logClock, sendTimeStamp.logClock)
        # self.logClock= sub(self.logClock, log(gcd))
        # self.logClock = add(self.logClock, self.logPrime)

        if self.logClock > Process.biggest_log_clock:
            Process.biggest_log_clock = self.logClock

        #self.logClock =  self.reset_log_value(self.logClock, self.primeNumber, sendTimeStamp.primeNumber)
        #self.logClock = resetLogToNearestMultiple(self.logClock, self.receivedPrimes)

        event.ReceiveTimeStamp = TimeStamp(copyOf(self.vectorClock), mpfrCopyOf(self.primeClock), mpfrCopyOf(self.logClock),self.primeNumber, copyOf(self.receivedPrimes))
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


set_logarithmic_precision_persistent()
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