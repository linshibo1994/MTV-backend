"""密码哈希 — passlib bcrypt。

登录校验与用户创建时哈希密码均走此模块，避免散落 bcrypt 细节。
"""

from passlib.context import CryptContext

# bcrypt 哈希上下文（自动处理盐与版本）
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    """对明文密码做 bcrypt 哈希。"""
    return _pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    """校验明文密码与已存哈希是否匹配。"""
    try:
        return _pwd_context.verify(password, password_hash)
    except (ValueError, TypeError):
        return False


# 预生成的合法 bcrypt 哈希，用于用户不存在/未启用时执行等价 dummy 校验，
# 抹平登录失败路径的 bcrypt 时延，避免用户名枚举的时延侧信道。
# 该哈希对应一个固定随机口令，仅用于消耗相近的 bcrypt 计算时间，不参与任何真实校验。
DUMMY_PASSWORD_HASH: str = hash_password("__login_dummy_password__")
