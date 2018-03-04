import gmpy2
import random
from gmpy2 import mpfr
from gmpy2 import mpz
from gmpy2 import rint_round

gmpy2.get_context().precision = 1000
number_of_processes = 5
number_of_events_process = 5


def generate_event():
    futureLogicalTaskTime = 0
    eventList = []
    for eventId in range(1, (number_of_processes * number_of_events_process) + 1):
        senderProcess = getRandomProcess()
        if random.randint(1, 3) == 1:
            # internal event
            event = Event(eventId, "I", senderProcess, "", futureLogicalTaskTime)
            # print event
            eventList.append(event)
        else:
            event = Event(eventId, "E", senderProcess, getRandomReceiveProcess(senderProcess), futureLogicalTaskTime)
            # print event
            eventList.append(event)
        futureLogicalTaskTime = futureLogicalTaskTime + 1
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

class Process(object):
    """docstring for Process"""

    def __init__(self, processId, primeNumber):
        super(Process, self).__init__()
        self.processId = processId
        self.primeNumber = mpfr(primeNumber)
        self.logicalTime = 0
        self.vectorClock = [0 for index in range(1, number_of_processes + 1)]
        self.primeClock = mpfr(0)
        self.logClock = mpfr(0)
        self.logPrime = gmpy2.log(primeNumber)
        self.queue = []
        self.receiver_queue = []

    def set_other_processes_instances(self, instances):
        self.instances = instances

    def internal_event(self, event):
        self.vectorClock[self.processId - 1] += self.vectorClock[self.processId - 1]
        self.primeClock = multiply(self.primeClock, self.primeNumber)
        self.logClock = add(self.logClock ,self.logPrime)
        self.logicalTime += 1
        event.SendTimeStamp = TimeStamp(self.vectorClock, self.primeClock, self.logClock)

    def send_event(self, event):
        self.vectorClock[self.processId - 1] += self.vectorClock[self.processId - 1]
        self.primeClock = multiply(self.primeClock, self.primeNumber)
        self.logClock = add(self.logClock , self.logPrime)

        event.SendTimeStamp = TimeStamp(copyOf(self.vectorClock), copyOf(self.primeClock), copyOf(self.logClock))
        self.instances[event.receiveProcessId - 1].receiver_queue.append(event)
        self.logicalTime += 1

    def receive_event(self, event):
        sendTimeStamp = event.SendTimeStamp

        #setting the vector clock
        for index in range(len(sendTimeStamp.vectorClock)):
            if sendTimeStamp.vectorClock[index] > self.vectorClock[index]:
                self.vectorClock[index]=sendTimeStamp.vectorClock[index]

        self.vectorClock[self.processId - 1] += self.vectorClock[self.processId - 1]

        #setting prime number
        self.primeClock = getLCM(self.primeClock, sendTimeStamp.primeClock)
        self.primeClock = multiply(self.primeClock, self.primeNumber)

        #setting log clock
        gcd  = getGCD(antilog(self.logClock), antilog(sendTimeStamp.logClock))
        self.logClock = add(self.logClock, sendTimeStamp.logClock)
        self.logClock= sub(self.logClock, log(gcd))

        event.ReceiveTimeStamp = TimeStamp(copyOf(self.vectorClock), copyOf(self.primeClock), copyOf(self.logClock))
        self.instances[event.receiveProcessId - 1].receiver_queue.append(event)
        self.logicalTime += 1

    def __str__(self):
        return "Process {} details - Prime Number : {}, Events :{}".format(self.processId, self.primeNumber,
                                                                       len(self.queue))

    def getEvent(self, logicalTime):
        if not self.queue and not self.receiver_queue:
            return None
        else:
            if self.queue:
                self.queue[0]


eventList = generate_event()
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

while logical_time < number_of_processes * number_of_processes:

    logical_time += 1
