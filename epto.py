# Implementation for
# EpTO: An Epidemic Total Order Algorithm for Large-Scale Distributed Systems.
# Proceedings of the 16th Annual Middleware Conference.
# Miguel Matos - miguel.marques.matos@tecnico.ulisboa.pt

from collections import OrderedDict
from collections import deque
from collections import defaultdict
import difflib
import math
import random
# import uuid
import sys
import yaml
import cPickle
import itertools
from datetime import datetime

NOW = datetime.now
from sim import sim
import utils

# node states implemented as a list with indexes for performance
# also states need to be declared in each function to remove
# python global variable access performance penalty
# CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

# do the same for msg structure
# MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0,1,2,3,4

### global true order
GLOBAL_CLOCK = 0
absoluteTotalOrder = defaultdict()

#### ordering mechanisms
SPONTANEOUS_ORDER = 0
SPONTANEOUS_ORDER_TIMESTAMP = 1
STABLE_ORDER = 2

# fraction of stable nodes
LARGEST_NODE_ID = 0
nodesSinceTheBeginning = []

DRIFT_COMPENSATION = 0

CHURN_RATE = 0.

msgsSent = None  # {} #set to None to disable logging
msgsReceived = None  # {} #set to None to disable logging
msgRelayPerRounds = None  # {} #set to None to disable logging
ROUNDS_TO_KEEP = 50

msgSentTTL = None

CACHE_SIZE = 30

relaysPerEvt = {}


def init():
    global nodeState, LARGEST_NODE_ID, probBroadcast
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

    # schedule node initial cycles
    nodeIds = nodeState.keys()
    random.shuffle(nodeIds)

    LARGEST_NODE_ID = max(nodeIds)
    # when measuring order create message only in the beginning
    if IS_MEASURE_ORDER:
        nbMsgsToBroadcast = int(probBroadcast * nbNodes * nbCycles)
        print 'Measuring order. Broadcasting : ', nbMsgsToBroadcast, ' messages'
        # use random.choice instead of random.sample because we want the possibility to have nodes bcasting more than one message
        nodesToBroadcast = [random.choice(nodeIds) for x in range(nbMsgsToBroadcast)]

        for node in nodesToBroadcast:
            BROADCAST(node)

        # schedule all measurements
        for c in range(nbCycles * 10):
            sim.schedulleExecutionFixed(MEASURE_ORDER, nodeCycle * (c + 1))

    for node in nodeIds:
        cyclon_boot(node, random.sample(nodeState, CACHE_SIZE))
        CYCLE(node)

    if IS_CHURN:
        for c in range(nbCycles):
            sim.schedulleExecutionFixed(CHURN, nodeCycle * (c + 1))

    print 'Running: ...'
    run()
    wrapup()


CYCLE_COUNT = 0


def CYCLE(myself):
    global nodeState, fanout, IS_MEASURE_ORDER, msgRelayPerRounds, ORDER_MECHANISM_FUNCTION
    global relaysPerEvt
    global CYCLE_COUNT, nbNodes
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2

    if myself not in nodeState:
        return

    CYCLE_COUNT += 1
    if CYCLE_COUNT % nbNodes == 0:
        print >> sys.stderr, 'CYCLING ', nodeState[myself][CURRENT_CYCLE], NOW()

    cyclon_cycle(myself)

    nodeState[myself][CURRENT_CYCLE] += 1

    for msgId in nodeState[myself][NEXT_BALL]:
        nodeState[myself][NEXT_BALL][msgId][MSG_TTL] += 1

    # create messages if cycle is valid and if we are not taking measurements
    if nodeState[myself][CURRENT_CYCLE] < nbCycles and not IS_MEASURE_ORDER:
        if random.random() <= probBroadcast:
            if myself > nbNodes and nodeState[myself][CLOCK] == 0:
                # prevent newly joined nodes to broadcast _before_ receiving a ball
                pass
            else:
                BROADCAST(myself)

    # schedule execution
    if nodeState[myself][CURRENT_CYCLE] < nbCycles + TTL * 10:
        # sim.schedulleExecution(CYCLE,myself)
        sim.schedulleExecutionBounded(CYCLE, myself, nodeState[myself][CURRENT_CYCLE])

    ORDER_MECHANISM_FUNCTION(myself)

    if len(nodeState[myself][NEXT_BALL]) > 0:

        # PERFECT VIEW
        # targets = random.sample(nodeState,fanout)
        # CYCLON VIEW
        try:
            targets = random.sample(nodeState[myself][CYCLON_DATA][CYCLON_CACHE], fanout)
        except:
            targets = nodeState[myself][CYCLON_DATA][CYCLON_CACHE]

        for msgId in nodeState[myself][NEXT_BALL].keys():
            if not relaysPerEvt.has_key(msgId):
                relaysPerEvt[msgId] = 0
            relaysPerEvt[msgId] += fanout
        for target in targets:
            sim.send(MSG, target, myself, nodeState[myself][NEXT_BALL])


    # clean next ball
    nodeState[myself][NEXT_BALL] = {}


def spontaneousOrderMechanism(node):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4

    for msgId, msg in nodeState[node][NEXT_BALL].iteritems():
        if msgId not in nodeState[node][DELIVERED]:
            nodeState[node][DELIVERED][msgId] = nodeState[node][CURRENT_CYCLE]
            nodeState[node][DELIVERY_DELAY].append(sim.timestamp - msg[MSG_SENT_CYCLE])


def spontaneousOrderByIdMechanism(node):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4

    for msg in sorted(nodeState[node][NEXT_BALL].itervalues(), key=lambda x: (x[MSG_TIMESTAMP], x[MSG_SOURCE])):
        msgId = msg[MSG_ID]
        if msgId not in nodeState[node][DELIVERED]:
            nodeState[node][DELIVERED][msgId] = nodeState[node][CURRENT_CYCLE]
            nodeState[node][DELIVERY_DELAY].append(sim.timestamp - msg[MSG_SENT_CYCLE])


def stableOrderMechanism(myself):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4

    # update TTL for queued messages
    for msgId in nodeState[myself][QUEUED]:
        nodeState[myself][QUEUED][msgId][MSG_TTL] += 1

    # put new messages on the queue
    for msgId, msg in nodeState[myself][NEXT_BALL].iteritems():
        if msgId not in nodeState[myself][DELIVERED]:
            if msg[MSG_TIMESTAMP] >= nodeState[myself][LAST_DELIVERED_TS]:
                if msgId in nodeState[myself][QUEUED]:
                    if nodeState[myself][QUEUED][msgId][MSG_TTL] < msg[MSG_TTL]:
                        nodeState[myself][QUEUED][msgId][MSG_TTL] = msg[MSG_TTL]
                else:
                    nodeState[myself][QUEUED][msgId] = msg[:]  # fastest way to copy msg
            else:
                print 'TO violation. node: ', myself, ' msg:', msg, ' lastDeliveredTS: ', nodeState[myself][
                    LAST_DELIVERED_TS]

    stableMessages = []
    minQueuedTS = sys.maxint
    holdingMsg = None
    for msg in nodeState[myself][QUEUED].itervalues():
        msgTS = msg[MSG_TIMESTAMP]
        if isStable(myself, msg):
            stableMessages.append(msg)
        elif minQueuedTS > msgTS:  # store min timestamp of unstable messages
            minQueuedTS = msgTS
            holdingMsg = msg

    deliverableMessages = []
    for msg in stableMessages:
        if msg[MSG_TIMESTAMP] < minQueuedTS:
            deliverableMessages.append(msg)
            nodeState[myself][QUEUED].pop(msg[MSG_ID])

    # sort first by timestamp then by the id of the source
    maxDeliveredTs = -1
    tmpDelv = sorted(deliverableMessages, key=lambda x: (x[MSG_TIMESTAMP], x[MSG_SOURCE]))
    for msg in tmpDelv:
        msgTs = msg[MSG_TIMESTAMP]
        if maxDeliveredTs < msgTs:
            maxDeliveredTs = msgTs
            nodeState[myself][LAST_DELIVERED_TS] = msgTs
        nodeState[myself][DELIVERED][msg[MSG_ID]] = (nodeState[myself][CURRENT_CYCLE], sim.timestamp)
        # nodeState[myself]['deliveredLIST'].append( (msg[MSG_ID], sim.timestamp) )
        nodeState[myself][DELIVERY_DELAY].append(sim.timestamp - msg[MSG_SENT_CYCLE])


def CHURN():
    global nodeState, nodeStateRemoved, LARGEST_NODE_ID, nodesSinceTheBeginning, CHURN_RATE
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12


    nodesToRemove = random.sample(nodeState, int(len(nodeState) * CHURN_RATE))

    # compute the node's current cycle to initialize the new node's currentCycle
    # so that we count the relays per round properly
    currentCycle = nodeState[nodeState.keys()[0]][CURRENT_CYCLE]
    # allCurrentCycles = set( [ nodeState[x][CURRENT_CYCLE] for x in nodeState.keys() ] )
    allCurrentCycles = set([nodeState[x][CURRENT_CYCLE] for x in nodeState])
    currentCycle = max(allCurrentCycles) + 1
    wipID = LARGEST_NODE_ID
    for node in nodesToRemove:
        # this forces the node to deliver all queued messages that are stable
        stableOrderMechanism(node)

        nodesSinceTheBeginning.discard(node)
        LARGEST_NODE_ID += 1
        nodeState[node][LEFT_TIMESTAMP] = sim.timestamp
        nodeStateRemoved[node] = nodeState.pop(node)

        nodeState[LARGEST_NODE_ID] = createNode()
        nodeState[LARGEST_NODE_ID][CURRENT_CYCLE] = currentCycle

        # initialze view with contents of other _correct_ node
        # cyclon_boot(LARGEST_NODE_ID, random.sample(nodeState, CACHE_SIZE))
        sim.schedulleExecutionBounded(CYCLE, LARGEST_NODE_ID, currentCycle)

    for i in xrange(len(nodesToRemove)):
        cyclon_boot(LARGEST_NODE_ID - i, random.sample(nodeState, CACHE_SIZE))


nbMessages = 0


def MSG(myself, source, receivedBall):
    global nodeState, nbMessages, msgsReceived
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4

    if myself not in nodeState or source not in nodeState:
        return

    nbMessages += 1

    nodeState[myself][NB_BALLS_RECEIVED] += 1

    for msgId, msg in receivedBall.iteritems():

        if msgsReceived != None:
            msgsReceived[msgId] += 1

        nodeState[myself][NB_MSGS_RECEIVED] += 1

        # TTL has not expired or msg is not yet known
        if msg[
            MSG_TTL] < TTL:  
            if msgId in nodeState[myself][NEXT_BALL]:  # only update TTL to largest value
                if nodeState[myself][NEXT_BALL][msgId][MSG_TTL] < msg[MSG_TTL]:
                    nodeState[myself][NEXT_BALL][msgId][MSG_TTL] = msg[MSG_TTL]
            else:
                nodeState[myself][NEXT_BALL][msgId] = msg[:]  # fastest way to copy msg

        else:
            # message TTL is the largest but it's not yet known
            if not (msgId in nodeState[myself][DELIVERED] or msgId in nodeState[myself][QUEUED]):
                nodeState[myself][QUEUED][msgId] = msg

        # update clock, only needed in for logical clock
        updateClock(myself, msg[MSG_TIMESTAMP])


broadcastedMessages = 0

def BROADCAST(myself):
    global nodeState, absoluteTotalOrder, broadcastedMessages, msgsSent, msgsReceived
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4


    msgId = broadcastedMessages
    timestamp = getClock(myself)
    # build message
    MSG_TTL = 0
    MSG_ID = msgId
    MSG_TIMESTAMP = timestamp
    MSG_SOURCE = myself
    MSG_SENT_CYCLE = sim.timestamp
    msg = [MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE]
    nodeState[myself][NEXT_BALL][msgId] = msg

    absoluteTotalOrder[msgId] = msg
    broadcastedMessages += 1

    if msgsSent != None:
        msgsSent[msgId] = 0
    if msgsReceived != None:
        msgsReceived[msgId] = 0
    if msgRelayPerRounds != None:
        msgRelayPerRounds[msgId] = {MSG_SENT_CYCLE: sim.timestamp, 'relays': [], 'relaySet': set(), 'relayCount': {},
                                    'ttlCount': {}}


def run():
    sim.run()


def MEASURE_ORDER():
    global orderEvolution
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

    absoluteTotalOrderIds = itertools.imap(lambda x: x[MSG_ID], sorted(absoluteTotalOrder.itervalues(),
                                                                       key=lambda x: (x[MSG_TIMESTAMP], x[MSG_SOURCE])))

    similarityTrue = []

    for node in nodeState:
        targetNodeOrder = list(nodeState[node][DELIVERED])
        diffTrue = difflib.SequenceMatcher(None, absoluteTotalOrderIds, targetNodeOrder).ratio()
        similarityTrue.append(diffTrue)

    similarityTrueCDF = utils.percentiles(similarityTrue, percs=range(101), paired=False)

    orderEvolution.append((sim.timestamp, similarityTrueCDF[0], similarityTrueCDF[1], similarityTrueCDF[5],
                           similarityTrueCDF[99], similarityTrueCDF[100]))


def wrapup():
    global nodeState, absoluteTotalOrder, orderEvolution, nodesSinceTheBeginning, msgsSent, msgsReceived, msgRelayPerRounds, nodeStateRemoved
    print 'WRAPPING UP'
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2

    # cyclon stuff
    cyclon_view_size = []
    cyclon_incoming_links_map = {node: 0 for node in nodeState}
    cyclon_outgoing_links = []
    for node in nodeState:
        cyclon_outgoing_links.append(len(nodeState[node][CYCLON_DATA][CYCLON_CACHE]))
        for target in nodeState[node][CYCLON_DATA][CYCLON_CACHE]:
            if target in cyclon_incoming_links_map:
                cyclon_incoming_links_map[target] += 1

    print 'view size: ', nodeState[nodeState.keys()[0]][CYCLON_DATA][CYCLON_CACHE]
    print 'outgoing: ', sorted(cyclon_outgoing_links)
    print 'incoming_links: ', sorted(cyclon_incoming_links_map.values())

    similarityTrue = []

    similarityTotalExpected = []
    idealDelivery = []

    similarityTotalAlt = []
    idealDeliveryAlt = []

    nodeSimilarity = []
    receivedLength = []
    nbBallsReceived = []
    nbMsgsReceived = []
    deliveryDelay = deque()

    for msgId in absoluteTotalOrder.keys():
        # event was relayed by a process that failed right after.
        # thus it should be ignored
        if relaysPerEvt[msgId] <= fanout:
            print 'removing invalid event', msgId
            absoluteTotalOrder.pop(msgId)

    absoluteTotalOrderIds = itertools.imap(lambda x: x[MSG_ID], sorted(absoluteTotalOrder.itervalues(),
                                                                       key=lambda x: (x[MSG_TIMESTAMP], x[MSG_SOURCE])))
    absoluteTotalOrderIdsTS = itertools.imap(lambda x: (x[MSG_SENT_CYCLE], x[MSG_ID]),
                                             sorted(absoluteTotalOrder.itervalues(),
                                                    key=lambda x: (x[MSG_TIMESTAMP], x[MSG_SOURCE])))
    averageLifeTimeTS = []
    nbDeliveries = []

    percHoles = []

    print 'nb. churned nodes:     ', len(nodeStateRemoved)
    # merge all nodes
    for k, v in nodeState.iteritems():
        nodeStateRemoved[k] = v
        nodeStateRemoved[k][LEFT_TIMESTAMP] = sim.timestamp
    nodeState.clear()
    nodeState = nodeStateRemoved
    del nodeStateRemoved

    print 'events TS: (ts,eventId) ', absoluteTotalOrderIdsTS

    for node in nodeState:
        joinedTS = nodeState[node][JOINED_TIMESTAMP]
        leftTS = nodeState[node][LEFT_TIMESTAMP]

        if IS_CLOCK_GLOBAL:
            expectedDeliveries = filter(
                lambda msgId: absoluteTotalOrder[msgId][MSG_SENT_CYCLE] > joinedTS and absoluteTotalOrder[msgId][
                                                                                           MSG_SENT_CYCLE] < leftTS - nodeCycle * (
                    TTL + 1), absoluteTotalOrder.iterkeys())
        else:
            expectedDeliveries = filter(
                lambda msgId: absoluteTotalOrder[msgId][MSG_SENT_CYCLE] > joinedTS and absoluteTotalOrder[msgId][
                                                                                           MSG_SENT_CYCLE] < leftTS - nodeCycle * (
                    TTL), absoluteTotalOrder.iterkeys())
            expectedDeliveries = sorted(expectedDeliveries, key=lambda x: (
                absoluteTotalOrder[x][MSG_TIMESTAMP], absoluteTotalOrder[x][MSG_SOURCE]))

        if len(expectedDeliveries) == 0:
            # no expected deliveries
            continue

        receivedLength.append(len(nodeState[node][DELIVERED]))
        deliveryDelay.extend(nodeState[node][DELIVERY_DELAY])

        averageLifeTimeTS.append(leftTS - joinedTS)
        deliveredList = list(nodeState[node][DELIVERED])
        deliveredRaw = deliveredList

        # cleanup events received that were sent before the process joined
        if IS_CLOCK_GLOBAL:
            try:
                deliveredList = filter(
                    lambda msgId: msgId in absoluteTotalOrder and absoluteTotalOrder[msgId][MSG_SENT_CYCLE] > joinedTS,
                    deliveredList)
                # sometimes a process might deliver more events than the expected ones
                # due to the way we consider which events are expected
                # prune these events
                deliveredList = deliveredList[: len(expectedDeliveries)]

                if len(deliveredList) < len(expectedDeliveries):
                    print "WARN DELIVERY", len(deliveredList), len(expectedDeliveries)
                    expectedDeliveries = expectedDeliveries[: len(deliveredList)]
            except:
                print " ------------ "
                print "msgId= ", msgId
                print "deliveredList= ", deliveredList
                print "joinedTS=", joinedTS
                print "absoluteTotalOrder= ", absoluteTotalOrder
                sys.exit()
        else:

            deliveredList = filter(
                lambda msgId: msgId in absoluteTotalOrder and absoluteTotalOrder[msgId][MSG_SENT_CYCLE] > joinedTS,
                deliveredList)
            # sometimes a process might deliver more events than the expected ones
            # due to the way we consider which events are expected
            # prune these events
            deliveredList = deliveredList[: len(expectedDeliveries)]

            if len(deliveredList) < len(expectedDeliveries):
                print "WARN DELIVERY", len(deliveredList), len(expectedDeliveries)
                expectedDeliveries = expectedDeliveries[: len(deliveredList)]


        deliveredORIG = list(deliveredList)

        if len(deliveredList) == 0:
            # no deliveries
            continue

        nbDeliveries.append(len(deliveredList))

        diffTrue = difflib.SequenceMatcher(None, expectedDeliveries, deliveredList).ratio()

        if diffTrue != 1:
            print '##############################################################################'
            print 'process: ', node
            print 'joinedTS: ', joinedTS
            print 'leftTS:   ', leftTS
            print 'expected:     ', expectedDeliveries
            print 'actual:       ', deliveredList
            print 'actual ORIG:  ', deliveredORIG
            print 'deliveredRaw:  ', deliveredRaw
            print 'difference:   ', diffTrue
            print '%hole:        ', float(1 - len(deliveredList) / float(len(expectedDeliveries)))
            print 'queued events:', sorted(
                itertools.imap(lambda x: (x[MSG_ID], x[MSG_TTL]), nodeState[node][QUEUED].values()))
            print 'nextBall     :', sorted(
                itertools.imap(lambda x: (x[MSG_ID], x[MSG_TTL]), nodeState[node][NEXT_BALL].values()))
            print 'relaysPerEvt :', [(msgId, relaysPerEvt[msgId]) for msgId in set(deliveredList + expectedDeliveries)]
            try:
                percHoles.append(float(1 - len(deliveredList) / float(len(expectedDeliveries))))
            except:
                print 'same delivery index'

        nbBallsReceived.append(nodeState[node][NB_BALLS_RECEIVED])
        nbMsgsReceived.append(nodeState[node][NB_MSGS_RECEIVED])

        targetNodeOrder = list(nodeState[node][DELIVERED])

        similarityTrue.append(diffTrue)

        # partial similarity
        msgToBeDelivered = []
        firstMsgToInclude = joinedTS - latencyValue * TTL
        if firstMsgToInclude < 0: firstMsgToInclude = 0
        lastMsgToInclude = leftTS - latencyValue * TTL
        if lastMsgToInclude < firstMsgToInclude: lastMsgToInclude = firstMsgToInclude

        for msgId in absoluteTotalOrderIds:
            sentTS = absoluteTotalOrder[msgId][MSG_SENT_CYCLE]
            if sentTS >= firstMsgToInclude and sentTS < lastMsgToInclude:
                msgToBeDelivered.append(msgId)

            if sentTS > leftTS:
                break
        similarityTotalExpected.append(
            difflib.SequenceMatcher(None, msgToBeDelivered, list(nodeState[node][DELIVERED])).ratio())

        if len(msgToBeDelivered) > 0:
            percValue = len(nodeState[node][DELIVERED]) / float(len(msgToBeDelivered))
            idealDelivery.append(percValue)

        msgToBeDeliveredAlt = []
        firstMsgToIncludeAlt = joinedTS
        if firstMsgToIncludeAlt < 0: firstMsgToIncludeAlt = 0
        lastMsgToIncludeAlt = leftTS - nodeCycle * TTL
        if lastMsgToIncludeAlt < firstMsgToIncludeAlt: lastMsgToIncludeAlt = firstMsgToIncludeAlt

        for msgId in absoluteTotalOrderIds:
            sentTS = absoluteTotalOrder[msgId][MSG_SENT_CYCLE]
            if sentTS >= firstMsgToIncludeAlt and sentTS < lastMsgToIncludeAlt:
                msgToBeDeliveredAlt.append(msgId)

            if sentTS > leftTS:
                break

        similarityTotalAlt.append(
            difflib.SequenceMatcher(None, msgToBeDeliveredAlt, list(nodeState[node][DELIVERED])).ratio())

        if len(msgToBeDeliveredAlt) > 0:
            percValue = len(nodeState[node][DELIVERED]) / float(len(msgToBeDeliveredAlt))
            idealDeliveryAlt.append(percValue)

    print 'Delivery delay:        ', utils.percentiles(deliveryDelay, roundPlaces=3)
    print 'Nb. received balls:    ', utils.percentiles(nbBallsReceived, roundPlaces=3)
    print 'Nb. received messages: ', utils.percentiles(nbMsgsReceived, roundPlaces=3)
    print 'similarity true:       ', utils.percentiles(similarityTrue, roundPlaces=3)
    print 'similarity expected:   ', utils.percentiles(similarityTotalExpected, roundPlaces=3)
    print 'idealDelivery:         ', utils.percentiles(idealDelivery, roundPlaces=3)
    print 'similarity expectd alt:', utils.percentiles(similarityTotalAlt, roundPlaces=3)
    print 'idealDeliveryAlt:         ', utils.percentiles(idealDeliveryAlt, roundPlaces=3)
    print 'lifeTime (time ticks): ', utils.percentiles(averageLifeTimeTS, roundPlaces=3)
    averageLifeTimeCycle = itertools.imap(lambda x: x / (nodeCycle - 1.0), averageLifeTimeTS)
    print 'lifeTime (cycles):     ', utils.percentiles(averageLifeTimeCycle, roundPlaces=3)
    print 'nb. deliveries:        ', utils.percentiles(nbDeliveries, roundPlaces=3)
    avgDeliveries = itertools.imap(lambda x: x / (broadcastedMessages + 0.), nbDeliveries)
    print 'avgDeliveries:         ', utils.percentiles(avgDeliveries, roundPlaces=3)

    # test
    print 'Number of broadcasted events:  ', broadcastedMessages
    print 'Number of delivered events:    ', nbDeliveries
    print 'Exp nb of delivered events:    ', expectedDeliveries
    print percHoles

    # print 'Dumping plots'
    nbBallsReceivedCDF = utils.percentiles(nbBallsReceived, percs=range(101), paired=False)
    nbMsgsReceivedCDF = utils.percentiles(nbMsgsReceived, percs=range(101), paired=False)
    deliveryDelayCDF = utils.percentiles(deliveryDelay, percs=range(101), paired=False)
    similarityTrueCDF = utils.percentiles(similarityTrue, percs=range(101), paired=False)

    utils.dumpAsGnuplot([nbBallsReceivedCDF], dumpPath + '/nbBallsReceived-' + str(runId) + '.gpData', ['#nb balls'])
    utils.dumpAsGnuplot([nbMsgsReceivedCDF], dumpPath + '/nbMsgsReceived-' + str(runId) + '.gpData', ['#nb msgs'])
    utils.dumpAsGnuplot([deliveryDelayCDF], dumpPath + '/deliveryDelay-' + str(runId) + '.gpData', ['#delivery delay'])
    utils.dumpAsGnuplot([similarityTrueCDF], dumpPath + '/similarityTrue-' + str(runId) + '.gpData',
                        ['#similarityTrue'])

    print '#procs with holes: ', len(percHoles), ' total procs: ', len(nodeState)
    print 'holes distr: ', utils.percentiles(percHoles, roundPlaces=3)

    with open(dumpPath + '/dumps-' + str(runId) + '.obj', 'w') as f:
        cPickle.dump(nbBallsReceived, f)
        cPickle.dump(nbMsgsReceived, f)
        cPickle.dump(deliveryDelay, f)
        cPickle.dump(similarityTrue, f)
        cPickle.dump(similarityTotalExpected, f)
        cPickle.dump(idealDelivery, f)
        cPickle.dump(similarityTotalAlt, f)
        cPickle.dump(idealDeliveryAlt, f)
        cPickle.dump(averageLifeTimeTS, f)
        # iterators cannot be pickled
        cPickle.dump(list(averageLifeTimeCycle), f)
        cPickle.dump(list(nbDeliveries), f)
        cPickle.dump(list(avgDeliveries), f)
        cPickle.dump(orderEvolution, f)
        if msgsSent:
            cPickle.dump(msgsSent.values(), f)
        if msgsReceived:
            cPickle.dump(msgsReceived.values(), f)


def verifyMessages(myself):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    for msg in nodeState[myself]['blindDelivery']:
        if not (msg in nodeState[myself][DELIVERED] or msg in nodeState[myself][QUEUED] or msg in nodeState[myself][
            NEXT_BALL]):
            print 'node: ', myself, 'LOST msg: ', msg, nodeState[myself]['blindDelivery'][msg]
            print 'blind delivery: ', nodeState[myself]['blindDelivery']
            print 'delivered: ', nodeState[myself][DELIVERED]
            print 'queued: ', nodeState[myself][QUEUED]
            print 'nextBall: ', nodeState[myself][NEXT_BALL]


# LOGICAL TIME
def isStableLogical(myself, msg):
    global DRIFT_COMPENSATION
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    return msg[MSG_TTL] > TTL + 1


def getClockLogical(myself):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    nodeState[myself][CLOCK] += 1
    return nodeState[myself][CLOCK]


def updateClockLogical(myself, ts):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    if ts > nodeState[myself][CLOCK]:
        nodeState[myself][CLOCK] = ts


# GLOBAL TIME
def isStableGlobal(myself, msg):
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    return msg[MSG_TTL] > TTL


def getClockGlobal(myself):
    global GLOBAL_CLOCK
    GLOBAL_CLOCK += 1
    return GLOBAL_CLOCK


def updateClockGlobal(myself, ts):
    pass


def checkLatencyNodes(latencyTable, nbNodes, defaultLatency=None):
    global latencyValue

    if latencyTable == None and defaultLatency != None:
        print 'WARNING: using constant latency'
        latencyTable = {n: {m: defaultLatency for m in range(nbNodes)} for n in range(nbNodes)}
        return latencyTable

    nbNodesAvailable = len(latencyTable)

    latencyList = [l for tmp in latencyTable.itervalues() for l in tmp.values()]
    latencyValue = math.ceil(utils.percentiles(latencyList, percs=[50], paired=False)[0])

    if nbNodes > nbNodesAvailable:
        nodesToPopulate = nbNodes - nbNodesAvailable

        nodeIds = range(nbNodes)

        print 'Need to add nodes to latencyTable'
        for node in range(nbNodesAvailable):
            latencyTable[node].update({target: random.choice(latencyList) for target in nodeIds[nbNodesAvailable:]})

        for node in range(nbNodesAvailable, nbNodes):
            latencyTable[node] = {target: random.choice(latencyList) for target in nodeIds}
            latencyTable[node].pop(node)  # remove itself

        with open('/tmp/latencyTable.obj', 'w') as f:
            cPickle.dump(latencyTable, f)

    return latencyTable


def createNode():
    # variable order
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

    # initialize
    CYCLON_DATA = deque()
    DELIVERED = OrderedDict()
    QUEUED = defaultdict()
    NEXT_BALL = defaultdict()
    NB_BALLS_RECEIVED = 0
    CLOCK = 0
    DELIVERY_DELAY = deque()
    NB_MSGS_RECEIVED = 0
    NB_MSGS_PER_ROUND = deque()
    CURRENT_CYCLE = 0
    JOINED_TIMESTAMP = sim.timestamp
    LEFT_TIMESTAMP = sim.timestamp
    LAST_DELIVERED_TS = 0
    return [CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED,
            NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS]


def configure(config):
    global nbNodes, nbCycles, probBroadcast, IS_CLOCK_GLOBAL, ORDER_MECHANISM, ORDER_MECHANISM_FUNCTION, fanout, TTL, bufferSize, nodeState, nodeStateRemoved, isStable, getClock, updateClock, orderEvolution, IS_MEASURE_ORDER, nodeCycle, IS_CHURN, CHURN_RATE, DRIFT_COMPENSATION, nodesSinceTheBeginning, latencyValue

    orderEvolution = []

    IS_MEASURE_ORDER = config.get('MEASURE_ORDER', False)
    IS_CHURN = config.get('CHURN', False)
    if IS_CHURN:
        CHURN_RATE = config.get('CHURN_RATE', 0.)
    MESSAGE_LOSS = float(config.get('MESSASE_LOSS', 0))
    if MESSAGE_LOSS > 0:
        sim.setMessageLoss(MESSAGE_LOSS)
    print 'send: ', sim.send

    IS_CLOCK_GLOBAL = config['IS_CLOCK_GLOBAL']
    ORDER_MECHANISM = eval(config['ORDER_MECHANISM'])

    if ORDER_MECHANISM == SPONTANEOUS_ORDER:
        ORDER_MECHANISM_FUNCTION = spontaneousOrderMechanism
    elif ORDER_MECHANISM == SPONTANEOUS_ORDER_TIMESTAMP:
        ORDER_MECHANISM_FUNCTION = spontaneousOrderByIdMechanism
    elif ORDER_MECHANISM == STABLE_ORDER:
        ORDER_MECHANISM_FUNCTION = stableOrderMechanism
    #### strategy dependent implementation
    if IS_CLOCK_GLOBAL:
        isStable, getClock, updateClock = isStableGlobal, getClockGlobal, updateClockGlobal
    else:
        isStable, getClock, updateClock = isStableLogical, getClockLogical, updateClockLogical

    nbNodes = config['nbNodes']
    probBroadcast = config['probBroadcast']
    nbCycles = config['nbCycles']

    latencyTablePath = config['LATENCY_TABLE']
    latencyValue = None
    try:
        with open(latencyTablePath, 'r') as f:
            latencyTable = cPickle.load(f)
    except:
        latencyTable = None
        latencyValue = int(latencyTablePath)
        print 'Using constant latency value: ', latencyValue

    latencyTable = checkLatencyNodes(latencyTable, nbNodes, latencyValue)
    latencyDrift = eval(config['LATENCY_DRIFT'])

    nodeCycle = int(config['NODE_CYCLE'])
    rawNodeDrift = float(config['NODE_DRIFT'])
    nodeDrift = int(nodeCycle * float(config['NODE_DRIFT']))

    ######## EpTO configuration
    # Custom values for testing
    fanout = int(config['fanout'])
    TTL = int(config['TTL'])

    nodeState = defaultdict()
    for n in xrange(nbNodes):
        nodeState[n] = createNode()
    nodeStateRemoved = defaultdict()
    nodesSinceTheBeginning = set(range(nbNodes))

    # for the manual tests
    # TTL = 5

    print 'config: ', config
    # print 'alpha: ', alpha
    print 'fanout: ', fanout
    print 'TTL: ', TTL

    if rawNodeDrift == 0:
        DRIFT_COMPENSATION = 0
    elif rawNodeDrift < 1:
        DRIFT_COMPENSATION = 1
    elif rawNodeDrift >= 1:
        DRIFT_COMPENSATION = int(math.ceil((rawNodeDrift * nodeCycle) / (nodeCycle + 0.))) + 1

    print 'DRIFT_COMPENSATION: ', DRIFT_COMPENSATION

    sim.init(nodeCycle, nodeDrift, latencyTable, latencyDrift)


####### cyclon implementation
SHUFFLE_LENGTH = 5


def cyclon_boot(myself, view):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2

    CYCLON_CACHE = {}

    # bootstrap with an accurate view, modify this later
    for node in view:
        if node != myself:
            CYCLON_CACHE[node] = 0

    CYCLON_SUBSET = {}
    CYCLON_SEEN = {}

    nodeState[myself][CYCLON_DATA] = [CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN]


def cyclon_cycle(myself):
    global nodeState, SHUFFLE_LENGTH
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2

    # node failed
    if myself not in nodeState:
        return

    if len(nodeState[myself][CYCLON_DATA][CYCLON_CACHE]) == 0:
        return

    # P initiates a shuffle:

    target = -1
    tmpAge = -1
    # 1. Increase by one the age of all neighbors.
    # the age field defines a key priority in which neighbors are contacted
    # and
    # 2. Select neighbor Q with the highest age among all neighbors,
    for n in nodeState[myself][CYCLON_DATA][CYCLON_CACHE]:
        nodeState[myself][CYCLON_DATA][CYCLON_CACHE][n] += 1
        if nodeState[myself][CYCLON_DATA][CYCLON_CACHE][n] > tmpAge:
            tmpAge = nodeState[myself][CYCLON_DATA][CYCLON_CACHE][n]
            target = n

    if len(nodeState[myself][CYCLON_DATA][CYCLON_CACHE]) >= SHUFFLE_LENGTH - 1:
        nodeState[myself][CYCLON_DATA][CYCLON_SUBSET] = {k: nodeState[myself][CYCLON_DATA][CYCLON_CACHE][k] for k in
                                                         random.sample(nodeState[myself][CYCLON_DATA][CYCLON_CACHE],
                                                                       SHUFFLE_LENGTH - 1)}
    else:
        nodeState[myself][CYCLON_DATA][CYCLON_SUBSET] = nodeState[myself][CYCLON_DATA][CYCLON_CACHE]

    # 3. Replace Q's entry with a new entry of age 0 and with P's address.
    # 4. Send the updated subset to peer Q.

    nodeState[myself][CYCLON_DATA][CYCLON_SUBSET].pop(target, None)
    # msg = [ [i,nodeState[myself][CYCLON_DATA][CYCLON_CACHE][i]] for i in nodeState[myself][CYCLON_DATA][CYCLON_SUBSET]]
    msg = {i: nodeState[myself][CYCLON_DATA][CYCLON_CACHE][i] for i in nodeState[myself][CYCLON_DATA][CYCLON_SUBSET]}
    msg[myself] = 0

    sim.send(CYCLON_P2Q, target, myself, msg)

    nodeState[myself][CYCLON_DATA][CYCLON_CACHE].pop(target)


def CYCLON_P2Q(myself, source, msg):
    global nodeState, SHUFFLE_LENGTH
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2

    # node failed
    if myself not in nodeState:
        return

    # receiving node Q
    # send back a random subset of at most l of its neighbors
    if len(nodeState[myself][CYCLON_DATA][CYCLON_CACHE]) >= SHUFFLE_LENGTH:
        entriesToSend = {k: nodeState[myself][CYCLON_DATA][CYCLON_CACHE][k] for k in
                         random.sample(nodeState[myself][CYCLON_DATA][CYCLON_CACHE], SHUFFLE_LENGTH - 1)}
    else:
        entriesToSend = {k: v for k, v in nodeState[myself][CYCLON_DATA][CYCLON_CACHE].iteritems()}

    entriesToSend.pop(source, None)
    sim.send(CYCLON_Q2P, source, myself, entriesToSend)

    # updates its own cache to accommodate all received entries.
    cyclon_updateCache(myself, msg)

    """Note that after node P has initiated a shuffling operation with its neighbor
	Q, P becomes Q's neighbor, while Q is no longer a neighbor of P. That is, the
	neighbor relation between P and Q reverses direction."""

    """in each cycle a node P is contacted by one other node
	on average to do shuffling, thus inserting one new pointer of age 0 in its cache,
	and pushing out of its cache the pointer of maximum age."""


def CYCLON_Q2P(myself, source, msg):
    # 5. Receive from Q a subset of no more than l of its own entries
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2

    # updates its own cache to accommodate all received entries
    cyclon_updateCache(myself, msg)

    """Note that after node P has initiated a shuffling operation with its neighbor
	Q, P becomes Q's neighbor, while Q is no longer a neighbor of P. That is, the
	neighbor relation between P and Q reverses direction."""


def cyclon_updateCache(myself, msg):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2

    if myself not in nodeState:
        return

    for n in msg:
        if n in nodeState[myself][CYCLON_DATA][CYCLON_SEEN]:
            nodeState[myself][CYCLON_DATA][CYCLON_SEEN][n] += 1
        else:
            nodeState[myself][CYCLON_DATA][CYCLON_SEEN][n] = 1

    # 6. Discard entries pointing at P and UPDATE(AGE)? entries already contained in P's cache.
    # 7. Update P's cache to include all remaining entries,
    for node in msg:
        # 6. Discard entries pointing at P
        if node != myself:
            if not cyclon_haveInCache(myself, node):
                #  by firstly using empty cache slots (if any),
                if len(nodeState[myself][CYCLON_DATA][CYCLON_CACHE]) < CACHE_SIZE:
                    nodeState[myself][CYCLON_DATA][CYCLON_CACHE][node] = msg[node]
                # and secondly replacing entries among the ones sent to Q.
                else:
                    # remove oldest node

                    target = -1
                    tmpAge = -1
                    # 1. Increase by one the age of all neighbors.
                    # the age field defines a key priority in which neighbors are contacted
                    # and
                    # 2. Select neighbor Q with the highest age among all neighbors,
                    for n in nodeState[myself][CYCLON_DATA][CYCLON_CACHE]:
                        if nodeState[myself][CYCLON_DATA][CYCLON_CACHE][n] > tmpAge:
                            tmpAge = nodeState[myself][CYCLON_DATA][CYCLON_CACHE][n]
                            toRemove = n

                    nodeState[myself][CYCLON_DATA][CYCLON_CACHE].pop(toRemove)
                    nodeState[myself][CYCLON_DATA][CYCLON_CACHE][node] = msg[node]
            # 6. UPDATE(AGE)? entries already contained in P's cache.
            else:
                cyclon_updateAge(myself, node, msg[node])

    # i'm not my own neighbor
    assert not cyclon_haveInCache(myself, myself)

    # cache size is respected
    assert not len(nodeState[myself][CYCLON_DATA][CYCLON_CACHE]) > CACHE_SIZE


def cyclon_haveInCache(myself, target):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2
    return target in nodeState[myself][CYCLON_DATA][CYCLON_CACHE]


def cyclon_getAge(myself, ip):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2
    return nodeState[myself][CYCLON_DATA][CYCLON_CACHE][ip]


def cyclon_updateAge(myself, node, age):
    global nodeState
    CYCLON_DATA, DELIVERED, QUEUED, NEXT_BALL, NB_BALLS_RECEIVED, CLOCK, DELIVERY_DELAY, NB_MSGS_RECEIVED, NB_MSGS_PER_ROUND, CURRENT_CYCLE, JOINED_TIMESTAMP, LEFT_TIMESTAMP, LAST_DELIVERED_TS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    MSG_TTL, MSG_ID, MSG_TIMESTAMP, MSG_SOURCE, MSG_SENT_CYCLE = 0, 1, 2, 3, 4
    CYCLON_CACHE, CYCLON_SUBSET, CYCLON_SEEN = 0, 1, 2
    nodeState[myself][CYCLON_DATA][CYCLON_CACHE][node] = age


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print sys.argv[0] + " <path> <runId> "
        sys.exit()

    dumpPath = sys.argv[1]
    confFile = dumpPath + '/conf.yaml'
    runId = int(sys.argv[2])
    f = open(confFile)
    config = yaml.load(f)
    configure(config)

    print 'Configuration done'
    init()
