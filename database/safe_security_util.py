import bcrypt
import time
#实现密码加密的登录失败限制
# 登录失败限制策略配置
MAX_ATTEMPTS = 5
LOCKOUT_TIME = 300  # 锁定时间（秒），即 5 分钟

# 存储登录失败尝试记录 (在实际生产环境应存储在 Redis 或数据库中)
LOGIN_ATTEMPTS = {}


# ----------------------------------------------------
# 1. 密码加密存储
# ----------------------------------------------------

def hash_password(password):
    """对密码进行加盐哈希"""
    # bcrypt 算法，b'' 代表 bytes 类型
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(password, hashed_password):
    """验证明文密码是否与哈希值匹配"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))


# ----------------------------------------------------
# 2. 登录失败限制 (Rate Limiting)
# ----------------------------------------------------

def check_login_attempt(username):
    """
    检查用户是否处于锁定状态，并更新尝试记录。
    返回 True 表示允许尝试，False 表示被锁定。
    """
    record = LOGIN_ATTEMPTS.get(username)
    current_time = time.time()

    if record and record.get('locked_until', 0) > current_time:
        # 仍处于锁定状态
        return False

        # 清除旧的锁定状态或记录
    if record and record.get('locked_until', 0) < current_time:
        LOGIN_ATTEMPTS[username] = {'attempts': 0, 'last_attempt': current_time}

    return True


def record_failed_attempt(username):
    """记录一次登录失败，如果达到限制则锁定"""
    current_time = time.time()
    record = LOGIN_ATTEMPTS.get(username, {'attempts': 0, 'last_attempt': current_time})

    record['attempts'] += 1
    record['last_attempt'] = current_time

    if record['attempts'] >= MAX_ATTEMPTS:
        record['locked_until'] = current_time + LOCKOUT_TIME
        print(f"[{username}] 登录失败次数过多，已锁定 {LOCKOUT_TIME} 秒。")

    LOGIN_ATTEMPTS[username] = record


def reset_login_attempts(username):
    """登录成功时调用，重置记录"""
    if username in LOGIN_ATTEMPTS:
        del LOGIN_ATTEMPTS[username]