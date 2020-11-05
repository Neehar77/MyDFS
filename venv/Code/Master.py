import rpyc # Remote python call makes use of object proxying, so that remote objects can be manipulated as if they were local
import uuid # These library provides unique immutable user id objects
import threading # Provides thread level parallelism
import math # Provides various Mathematical functions
import random # Generates Pseudo random numbers
import configparser # Used to write programs which can be customized by end users easily
import signal  #Allows defining custom handlers for asynchronous events to be executed when a signal is received
import pickle  #For serializing python object structure i.e. converting a python object hierarchy into a byte stream also known as marshalling
import sys # Provides access to some variables used or maintained by the interpreter
import os # Provides portable way of utilizing OS dependent functionality

from rpyc.utils.server import ThreadedServer # Create a new process when a request comes in

BLOCK_SIZE = 100
REPLICATION_FACTOR = 2
MINIONS = {"1": ("127.0.0.1", 8000),
           "2": ("127.0.0.1", 9000),}


# All the exposed methods or members of the service can be accessed by the client when it connects
# Name of the methods starts with exposed_
class MasterService(rpyc.Service):
    # We have defined an exposed class so that clients can access the class when it connects
    class exposed_master():
        file_table = {} #File to block mapping
        block_map = {} #Block to minions mapping
        minions = MINIONS #List of Minions i.e. servers

        block_size = BLOCK_SIZE #Assigning variables to there predefined values
        replication_factor = REPLICATION_FACTOR

        def exposed_read(self, filename):
            map = []
            # iterating over all the file's block
            for block in file_table[filename]:
                min_ad = []
                # Getting all the minions that contain that block
                for min_id in block_map[block]:
                    min_ad.append(minions[min_id])
            map.append({"block_id": block, "block_address": min_ad})
            return map

        def exposed_write(self, filename, size):
            file_table[filename] = []

            num_blocks = int(math.ceil(float(size) / self.block_size))
            return alloc_blocks(filename, num_blocks)

        def alloc_blocks(self, filename, num_blocks):
            blocks = []
            for i in range(num_blocks):
                block_uuid = str(uuid.uuid1()) # Generate a block
                nodes_ids = random.sample(list(minions.keys(), replication_factor)) #Allocating replication factor number of minions
                minion_adr = [minions[m] for m in nodes_ids]
                block_map[block_uuid] = nodes_ids
                file_table[filename].append(block_uuid)
                blocks.append({"block_id":block_uuid, "block_address":minion_adr})
            return blocks

if __name__ == "__main__":
    t = ThreadedServer(MasterService(), port = 8000, protocol_config={'allow_public_attrs':True,})
    t.start()


