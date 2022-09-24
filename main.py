import asyncio
from contextlib import AsyncExitStack, suppress
import signal
from nodes import Node
from transport import UdpTransport
import getopt
import sys
from worker import Worker
from config import Config


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

    try:
        opts, args = getopt.getopt(arguments, "h:p:", [
            "hostname=", "port=", "help="])

        for opt, arg in opts:
            if opt == '--help':
                print('failure_detector.py -h <hostname> -p <port>')
                sys.exit()
            elif opt in ("-h", "--hostname"):
                hostname = arg
            elif opt in ("-p", "--port"):
                port = int(arg)
        
        conf = Config(hostname, port)

    except getopt.GetoptError:
        print('failure_detector.py -h <hostname> -p <port>')
        sys.exit(2)

    return conf


if __name__ == '__main__':

    conf = parse_cmdline_args(sys.argv[1:])

    asyncio.run(run(conf))