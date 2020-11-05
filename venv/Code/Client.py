import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
# LOG = logging.getLogger(__name__)


def get(master, fname):
    file_table = master.read(fname)
    if not file_table:
        logging.info("404: file not found")
        return

    for block in file_table:
        for host,port in block['block_address']:
            try:
                con = rpyc.connect(host, port=port).root
                data = con.get(block['block_id'])
                if data:
                    sys.stdout.write(data)
                    break
            except Exception as e:
                continue
        else:
            logging.error("No blocks found. Possibly a corrupt file")


def put(master, source, dest):
    size = os.path.getsize(source)
    blocks = master.write(dest, size)
    with open(source) as f:
        for b in blocks:
            data = f.read(master.get_block_size())
            block_uuid = block['block_id']
            minions = block['block_address']

            minion = minions[0]
            minions = minions[1:]
            host, port = minion

            con = rpyc.connect(host, port=port)
            con.root.put(block_id, data, minions)


def main(args):
    con = rpyc.connect("localhost", port=2131)
    master = con.root

    if args[0] == "get":
        get(master, args[1])
    elif args[0] == "put":
        put(master, args[1], args[2])
    else:
        logging.error("try 'put srcFile destFile OR get file'")


if __name__ == "__main__":
    main(sys.argv[1:])