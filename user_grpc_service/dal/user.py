from typing import Optional

from typing_extensions import TypedDict

from user_grpc_service.helper.conn_proxy import context_proxy


class UserTypedDict(TypedDict):
    uid: str
    user_name: str


class UserDal(object):
    @staticmethod
    def create_user(*, uid: str, user_name: str, password: str) -> None:
        with context_proxy.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO user (uid, user_name, password, deleted) VALUES (%s, %s, %s, 0)"
                " ON DUPLICATE KEY UPDATE password=%s, user_name=%s, deleted=0",
                (uid, user_name, password, password, user_name),
            )

    @staticmethod
    def delete_user(*, uid: str) -> None:
        with context_proxy.conn.cursor() as cursor:
            ret: int = cursor.execute("UPDATE user SET deleted=1 WHERE uid=%s and deleted=0", (uid,))
            if not ret:
                raise RuntimeError("user not found")

    @staticmethod
    def login_user(*, uid: str, password: str, token: str) -> UserTypedDict:
        with context_proxy.conn.cursor() as cursor:
            cursor.execute(
                "SELECT uid, user_name FROM user WHERE uid=%s and password=%s and deleted=0",
                (uid, password),
            )
            user_dict: UserTypedDict = cursor.fetchone()
            if not user_dict:
                raise RuntimeError("User not found")
            cursor.execute(
                "INSERT INTO user_token (uid, token, deleted) VALUES (%s, %s, 0)"
                " ON DUPLICATE KEY UPDATE token=%s, deleted=0",
                (uid, token, token),
            )
            return user_dict

    @staticmethod
    def get_uid_by_token(*, token: str) -> Optional[str]:
        with context_proxy.conn.cursor() as cursor:
            cursor.execute(
                "SELECT uid FROM user_token WHERE token=%s AND deleted=0",
                (token,),
            )
            return (cursor.fetchone() or {}).get("uid", None)

    def logout_user(self, *, uid: str, token: str) -> None:
        with context_proxy.conn.cursor() as cursor:
            if uid != self.get_uid_by_token(token=token):
                raise RuntimeError("User not login")
            cursor.execute("UPDATE user_token SET deleted=1 WHERE uid=%s", (uid,))


user_dal: UserDal = UserDal()
