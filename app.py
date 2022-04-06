import logging
import os
from concurrent import futures
from typing import List, Optional

import grpc
from grpc_example_common.interceptor.server_interceptor.base import BaseInterceptor
from grpc_example_common.interceptor.server_interceptor.customer_top import CustomerTopInterceptor

from user_grpc_service.handler.user import UserService, user_service

logging.basicConfig(
    format="[%(asctime)s %(levelname)s] %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)
logger: logging.Logger = logging.getLogger()


def main(host: str = "127.0.0.1", port: str = "9000", ssl_port: Optional[str] = None) -> None:
    interceptor_list: List[BaseInterceptor] = [CustomerTopInterceptor()]
    server: grpc.server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=interceptor_list,
    )
    user_service.add_UserServicer_to_server(UserService(), server)

    if ssl_port:
        port = ssl_port
        # read in key and certificate
        with open(os.path.join(os.path.split(__file__)[0], "server.key")) as f:
            private_key = f.read().encode()
        with open(os.path.join(os.path.split(__file__)[0], "server.crt")) as f:
            certificate_chain = f.read().encode()
        # create server credentials
        server_creds = grpc.ssl_server_credentials(
            (
                (
                    private_key,
                    certificate_chain,
                ),
            )
        )
        server.add_secure_port(f"{host}:{port}", server_creds)
    else:
        server.add_insecure_port(f"{host}:{port}")
    server.start()
    try:
        for generic_handler in server._state.generic_handlers:
            logger.info(
                f"add service name:{generic_handler.service_name()} cnt:{len(generic_handler._method_handlers)}"
            )
        logger.info(f"server run in {host}:{port}")
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
