import rpyc # Remote python call makes use of object proxying, so that remote objects can be manipulated as if they were local
import uuid # These library provides unique immutable user id objects
import os # Provides portable way of utilizing OS dependent functionality
import logging
import sys

from rpyc.utils.server import ThreadedServer # Create a new process when a request comes in

DATA_DIR = "/temp/minion/"
PORT = 8888
logging.basicConfig(level=logging.DEBUG)

class Minion(rpyc.Service):
        def exposed_put(self, block_uuid, data, minions):
            logging.debug("put block: " +block_uuid)
            with open(DATA_DIR + str(block_uuid), 'w') as f:
                f.write(data) # At first we are writing the file in local disk and after that forwarding it to the other minions
            if len(minions) > 0:
                self.forward(block_uuid, data, minions)

        def exposed_get(self, block_uuid):
            logging.debug("get block: " + block_uuid)
            block_address = DATA_DIR + str(block_uuid)
            if not os.path.isfile(block_address):
                return None
            with open(block_address) as f:
                return f.read()

        def forward(self, block_uuid, data, minions):
            logging.debug("forwarding block: " + block_uuid + str(minions))
            next_minion = minions[0]
            minions = minions[1:]
            host,port = next_minion

            rpyc.connect(host, port=port).root.put(block_uuid, data, minions)


        def delete_block(self,uuid):
            pass

if __name__ == "__main__":
    PORT = int(sys.argv[1])
    DATA_DIR = sys.argv[2]

    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    logging.debug("starting minion")
    rpyc_logger = logging.getLogger('rpyc')
    rpyc_logger.setLevel(logging.WARN)
    t = ThreadedServer(Minion(), port = PORT, logger = rpyc_logger, protocol_config = {'allow_public_attrs':True,})
    t.start()
