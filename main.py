import argparse
from client import Client
from server import Server
from connection_factory import ConnectionFactory
from tqdm import tqdm


def main():
    parser = argparse.ArgumentParser(description="File sharing")
    parser.add_argument("-w", "--write", help="File to write")
    parser.add_argument("-r", "--read", help="File to read")
    parser.add_argument("-f", "--force", help="Force overwrite", action="store_true")
    parser.add_argument(
        "-p", "--port", help="Port to use", type=int, default=7777, required=True
    )
    parser.add_argument(
        "-a", "--address", help="Host to connect to or bind to", default="0.0.0.0"
    )
    parser.add_argument("-t", "--threads", help="Threads to use", type=int, default=16)
    parser.add_argument("-z", "--compress", help="Compress data", action="store_true")

    args = parser.parse_args()
    if args.read == args.write:
        print("Invalid arguments, one of -r or -w must be specified.")
        return

    connection_factory = ConnectionFactory(args.port, args.address)

    if args.read:
        server = Server(connection_factory, args.read, args.compress)
        server.run()
    elif args.write:
        client = Client(
            connection_factory,
            102400,
            args.write,
            args.force,
            args.write + ".bm",
            args.threads,
            args.compress,
        )
        t = None

        def progress_callback(current, total):
            global t
            if current == 0:
                t = tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    smoothing=0.1,  # smoothing of progress bar
                    unit_divisor=1024,  # make use of standard units e.g. KB, MB, etc.
                    miniters=1,  # recommended for network progress that might vary strongly
                )
            else:
                t.update(current)

        client.run(progress_callback)


if __name__ == "__main__":
    main()
