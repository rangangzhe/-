#登录和 RBAC 权限检查
import time
import sys
import pymysql.cursors  # <--- 修正1: 导入 DictCursor 所需的模块
from db_utils import db_manager
from safe_security_util import hash_password, verify_password, check_login_attempt, record_failed_attempt, \
    reset_login_attempts

# --- 会话管理配置 ---
SESSION_TIMEOUT = 3600
USER_SESSIONS = {}


# --- 辅助函数：RBAC 初始化 (解决关键问题) ---

def execute_many_updates(sql, data):
    """用于高效批量插入数据，简化代码，此处用于 RBAC 初始化"""
    conn = db_manager.get_connection()
    if not conn: return 0
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, data)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        conn.rollback()
        # 允许 IGNORE 错误（如重复键），但报告其他错误
        if e.args[0] != 1062:  # 1062 is Duplicate entry for key 'PRIMARY'
            print(f"RBAC 初始化批量更新出错: {e}")
        return 0


def ensure_rbac_dependencies():
    """
    在注册或任何依赖 RBAC 的操作前，强制检查和创建核心角色、权限和关联。
    """
    print("--- 运行 RBAC 依赖检查与初始化... ---")

    # --- 1. 确保核心角色存在 ---
    roles_data = [
        ('系统管理员', '维护账号和权限分配。'),
        ('数据管理员', '录入校验资源信息。'),
        ('区域护林员', '处理预警，记录维护。'),
        ('监管人员', '查看全系统数据。'),
        ('公众用户', '查看公开数据，提交异常反馈。')
    ]
    sql_role = "INSERT IGNORE INTO SysRole (role_name, description) VALUES (%s, %s)"
    execute_many_updates(sql_role, roles_data)

    # --- 2. 确保核心权限存在 ---
    permissions_data = [
        ('monitor:view_region',), ('resource:view_public',), ('warning:manage_rules',),
        ('equipment:maintenance_log',)  # 确保所有 RBAC 检查点的权限都存在
    ]
    sql_perm = "INSERT IGNORE INTO SysPermission (permission_code) VALUES (%s)"
    execute_many_updates(sql_perm, permissions_data)

    # --- 3. 确保 '公众用户' 的核心权限关联存在 ---
    conn = db_manager.get_connection()
    if not conn: return
    # 修正2: 更改 cursor 初始化方式
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 查找关键 IDs
        cursor.execute("SELECT role_id FROM SysRole WHERE role_name = '公众用户'")
        public_role_id = cursor.fetchone()

        cursor.execute("SELECT permission_id FROM SysPermission WHERE permission_code = 'resource:view_public'")
        public_perm_id = cursor.fetchone()

        cursor.execute("SELECT permission_id FROM SysPermission WHERE permission_code = 'monitor:view_region'")
        monitor_perm_id = cursor.fetchone()

        if public_role_id and public_perm_id:
            # 建立 '公众用户' 与 'resource:view_public' 的关联
            sql_link = "INSERT IGNORE INTO RolePermission (role_id, permission_id) VALUES (%s, %s)"
            execute_many_updates(sql_link, [(public_role_id['role_id'], public_perm_id['permission_id'])])

        if public_role_id and monitor_perm_id:
            # 建立 '公众用户' 与 'monitor:view_region' 的关联
            sql_link = "INSERT IGNORE INTO RolePermission (role_id, permission_id) VALUES (%s, %s)"
            execute_many_updates(sql_link, [(public_role_id['role_id'], monitor_perm_id['permission_id'])])

        conn.commit()
    except Exception as e:
        conn.rollback()
        # 捕获并报告错误，同时避免二次错误
        if 'cursor' in locals() and cursor:  # 检查 cursor 变量是否成功创建
            cursor.close()
        raise e  # 重新抛出错误，让主程序捕获，避免二次错误
    finally:
        # 确保游标关闭 (如果它成功创建)
        if 'cursor' in locals() and cursor:
            cursor.close()
    print("--- RBAC 依赖检查与初始化完成。---")


# ----------------------------------------------------
# A. 用户注册功能 (更新后)
# ----------------------------------------------------

def register_user(username, password, phone=""):
    """
    用户注册：在注册前确保 RBAC 依赖存在。
    """
    # 强制运行依赖检查和初始化 (解决用户报告的关键问题)
    ensure_rbac_dependencies()

    # 1. 检查用户名是否已存在
    if get_user_by_username(username):
        return False, "用户名已存在。"

    # 2. 密码加密存储
    hashed_pwd = hash_password(password)

    conn = db_manager.get_connection()
    if not conn: return False, "数据库连接失败。"

    cursor = None  # 预先定义 cursor
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # 使用 DictCursor

        # 3a. 插入 SysUser
        user_sql = "INSERT INTO SysUser (username, password_hash, phone) VALUES (%s, %s, %s)"
        cursor.execute(user_sql, (username, hashed_pwd, phone))
        new_user_id = cursor.lastrowid

        # 3b. 查询默认角色ID ('公众用户')
        role_sql = "SELECT role_id FROM SysRole WHERE role_name = '公众用户'"
        cursor.execute(role_sql)
        public_role_id = cursor.fetchone()['role_id']

        # 3c. 分配默认角色
        assign_sql = "INSERT INTO UserRole (user_id, role_id) VALUES (%s, %s)"
        cursor.execute(assign_sql, (new_user_id, public_role_id))

        conn.commit()
        return True, f"注册成功，用户ID: {new_user_id}，已分配默认角色: 公众用户。"

    except Exception as e:
        conn.rollback()
        return False, f"注册失败：{e}"
    finally:
        if cursor:
            cursor.close()


# ----------------------------------------------------
# B. 用户登录功能 (沿用)
# ----------------------------------------------------

def get_user_by_username(username):
    sql = "SELECT user_id, password_hash FROM SysUser WHERE username = %s"
    # 注意：这里需要确保 db_manager.execute_query 返回字典游标
    result = db_manager.execute_query(sql, params=(username,))
    return result[0] if result else None


def login(username, password):
    if not check_login_attempt(username):
        return None, "账户被锁定，请稍后重试。"

    user_record = get_user_by_username(username)

    if not user_record:
        record_failed_attempt(username)
        return None, "用户名或密码错误。"

    if verify_password(password, user_record['password_hash']):
        reset_login_attempts(username)
        user_id = user_record['user_id']
        USER_SESSIONS[user_id] = time.time()
        return user_id, "登录成功。"
    else:
        record_failed_attempt(username)
        return None, "用户名或密码错误。"


# ----------------------------------------------------
# C. 权限检查 (沿用)
# ----------------------------------------------------

def check_session(user_id):
    last_activity = USER_SESSIONS.get(user_id)
    if not last_activity or (time.time() - last_activity) > SESSION_TIMEOUT:
        if user_id in USER_SESSIONS:
            del USER_SESSIONS[user_id]
        return False

    USER_SESSIONS[user_id] = time.time()
    return True


def check_permission(user_id, required_permission_code):
    if not check_session(user_id):
        return False, "会话超时或无效，请重新登录。"

    sql = """
    SELECT COUNT(p.permission_id) AS has_permission
    FROM SysUser u
    JOIN UserRole ur ON u.user_id = ur.user_id
    JOIN SysRole r ON ur.role_id = r.role_id
    JOIN RolePermission rp ON r.role_id = rp.role_id
    JOIN SysPermission p ON rp.permission_id = p.permission_id
    WHERE u.user_id = %s AND p.permission_code = %s;
    """

    result = db_manager.execute_query(sql, params=(user_id, required_permission_code))

    has_permission = result[0]['has_permission'] > 0 if result else False

    if has_permission:
        return True, "权限验证通过。"
    else:
        return False, f"权限不足：缺少 {required_permission_code} 权限。"


# --- RBAC 角色管理功能  ---

def get_user_roles(user_id):
    """获取用户当前拥有的角色列表。"""
    sql = """
    SELECT r.role_name 
    FROM SysRole r JOIN UserRole ur ON r.role_id = ur.role_id 
    WHERE ur.user_id = %s
    """
    roles = db_manager.execute_query(sql, params=(user_id,))
    return [role['role_name'] for role in roles]


def grant_role(target_user_id, role_name):
    """分配角色给用户，返回成功状态和消息。"""
    role_info = db_manager.execute_query("SELECT role_id FROM SysRole WHERE role_name = %s", params=(role_name,))
    if not role_info:
        return False, f"❌ 角色 '{role_name}' 不存在。"

    role_id = role_info[0]['role_id']
    sql = "INSERT IGNORE INTO UserRole (user_id, role_id) VALUES (%s, %s)"
    rows = db_manager.execute_update(sql, params=(target_user_id, role_id))

    if rows > 0:
        return True, f"✅ 成功为用户 {target_user_id} 分配角色: {role_name}。"
    else:
        return False, f"用户 {target_user_id} 已拥有角色: {role_name}。"


def revoke_role(target_user_id, role_name):
    """从用户撤销角色，返回成功状态和消息。"""
    role_info = db_manager.execute_query("SELECT role_id FROM SysRole WHERE role_name = %s", params=(role_name,))
    if not role_info:
        return False, f"❌ 角色 '{role_name}' 不存在。"

    role_id = role_info[0]['role_id']
    sql = "DELETE FROM UserRole WHERE user_id = %s AND role_id = %s"
    rows = db_manager.execute_update(sql, params=(target_user_id, role_id))

    if rows > 0:
        return True, f"✅ 成功从用户 {target_user_id} 撤销角色: {role_name}。"
    else:
        return False, f"❌ 用户 {target_user_id} 原本就没有该角色: {role_name}。"