from contextlib import contextmanager
from typing import Callable, Generator, List

import grpc
import pytest
from grpc_example_common.interceptor.client_interceptor.customer_top import (
    CustomerTopInterceptor as ClientCustomerTopInterceptor,
)
from grpc_example_common.interceptor.server_interceptor.customer_top import CustomerTopInterceptor
from grpc_example_common.protos.user import user_pb2, user_pb2_grpc

from user_grpc_service.handler.user import UserService
from user_grpc_service.helper.conn_proxy import SteadyDBConnection, g_db_pool


@pytest.fixture(scope="module")
def grpc_add_to_server() -> Callable:
    return user_pb2_grpc.add_UserServicer_to_server


@pytest.fixture(scope="module")
def grpc_servicer() -> UserService:
    return UserService()


@pytest.fixture(scope="module")
def grpc_interceptors() -> List[grpc.ServerInterceptor]:
    return [CustomerTopInterceptor()]


@pytest.fixture(scope="module")
def grpc_stub(grpc_channel: grpc.Channel) -> user_pb2_grpc.UserStub:
    channel: grpc.Channel = grpc.intercept_channel(grpc_channel, ClientCustomerTopInterceptor())
    return user_pb2_grpc.UserStub(channel)


@contextmanager
def mock_user(uid: str = "666666", user_name: str = "so1n", password: str = "123456") -> Generator[None, None, None]:
    conn: SteadyDBConnection = g_db_pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO user (uid, user_name, password) VALUES (%s, %s, %s)",
                (uid, user_name, password),
            )
            conn.commit()
        yield
    finally:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM user WHERE uid=%s", (uid,))
            conn.commit()


@contextmanager
def mock_user_token(uid: str = "666666", token: str = "mock_token") -> Generator[None, None, None]:
    conn: SteadyDBConnection = g_db_pool.connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO user_token (uid, token) VALUES (%s, %s)", (uid, token))
            conn.commit()
        yield
    finally:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM user_token WHERE uid=%s", (uid,))
            conn.commit()


class TestUser:
    def test_create_user(self, grpc_stub: user_pb2_grpc.UserStub) -> None:
        try:
            request: user_pb2.CreateUserRequest = user_pb2.CreateUserRequest(
                uid="666666", user_name="so1n", password="123456"
            )
            grpc_stub.create_user(request, metadata=[])
        finally:
            conn: SteadyDBConnection = g_db_pool.connection()
            conn.begin()
            with conn.cursor() as cursor:
                ret: int = cursor.execute("DELETE FROM user WHERE uid=%s", ("666666",))
            conn.commit()
            assert ret == 1

    def test_delete_user(self, grpc_stub: user_pb2_grpc.UserStub) -> None:
        uid: str = "666666"
        request: user_pb2.DeleteUserRequest = user_pb2.DeleteUserRequest(uid=uid)
        # user not found
        with pytest.raises(RuntimeError):
            grpc_stub.delete_user(request, metadata=[])
        with mock_user(uid=uid):
            grpc_stub.delete_user(request, metadata=[])

    def test_login_user(self, grpc_stub: user_pb2_grpc.UserStub) -> None:
        uid: str = "666666"
        password: str = "123456"
        try:
            request: user_pb2.LoginUserRequest = user_pb2.LoginUserRequest(uid=uid, password=password)
            # user not found
            with pytest.raises(RuntimeError):
                grpc_stub.login_user(request, metadata=[])

            with mock_user(uid=uid, password=password):
                result: user_pb2.LoginUserResult = grpc_stub.login_user(request, metadata=[])
                assert result.uid == uid
        finally:
            conn: SteadyDBConnection = g_db_pool.connection()
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM user_token WHERE uid=%s", (uid,))
                conn.commit()

    def test_logout_user(self, grpc_stub: user_pb2_grpc.UserStub) -> None:
        uid: str = "666666"
        request: user_pb2.LogoutUserRequest = user_pb2.LogoutUserRequest(uid=uid, token="mock_token")
        # token not found
        with pytest.raises(RuntimeError):
            grpc_stub.logout_user(request, metadata=[])

        with mock_user_token(uid):
            grpc_stub.logout_user(request, metadata=[])

    def test_check_login(self, grpc_stub: user_pb2_grpc.UserStub) -> None:
        uid: str = "666666"
        request: user_pb2.LogoutUserRequest = user_pb2.LogoutUserRequest(uid=uid, token="mock_token")
        result: user_pb2.CheckLoginResult = grpc_stub.check_user_login(request, metadata=[])
        assert not result.result
        with mock_user_token(uid):
            result = grpc_stub.check_user_login(request, metadata=[])
            assert result.result
