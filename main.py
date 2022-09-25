import asyncio
from contextlib import AsyncExitStack, suppress
import signal
from nodes import Node
from transport import UdpTransport
import getopt
import sys
from worker import Worker
from config import Config

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def setup_logging():
    logger = logging.getLogger(__name__)  
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('log_file.log')
    formatter = logging.Formatter('%(asctime)s : %(name)s  : %(funcName)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


async def run(config: Config):

    loop = asyncio.get_running_loop()

    async with AsyncExitStack() as stack:

        # members = [Node('127.0.0.1', 8001)]
        stack.enter_context(suppress(asyncio.CancelledError))
        tranport = UdpTransport(config.node.host, config.node.port)
        # tranport = UdpTransport('127.0.0.1', 8000, members)
        worker: Worker = await stack.enter_async_context(tranport.enter())
        worker.initialize(config)
        task = asyncio.create_task(worker.run())
        loop.add_signal_handler(signal.SIGINT, task.cancel)
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        await task
    
    return 0


def parse_cmdline_args(arguments) -> Config:
        
    hostname = '127.0.0.1'
    port = 8001
    conf = None
    introducer = None

    try:
        opts, args = getopt.getopt(arguments, "h:p:i:", [
            "hostname=", "port=","introducer=" "help="])

        for opt, arg in opts:
            if opt == '--help':
                print('failure_detector.py -h <hostname> -p <port>')
                sys.exit()
            elif opt in ("-h", "--hostname"):
                hostname = arg
            elif opt in ("-p", "--port"):
                port = int(arg)
            elif opt in ("-i", "--introducer"):
                introducer = arg

        
        conf = Config(hostname, port, introducer)

    except getopt.GetoptError:
        logging.error('failure_detector.py -h <hostname> -p <port>')
        sys.exit(2)

    return conf


if __name__ == '__main__':

    # setup_logging()

    conf = parse_cmdline_args(sys.argv[1:])

    asyncio.run(run(conf))