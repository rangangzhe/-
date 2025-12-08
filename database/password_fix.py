import sys

# 确保导入依赖
try:
    from db_utils import db_manager
    from safe_security_util import hash_password
except ImportError:
    print("错误：无法导入 db_utils.py 或 safe_security_util.py。请检查文件是否存在。")
    sys.exit(1)

# 定义您想要使用的明文密码 (所有现有用户将更新为此密码)
DEFAULT_PLAINTEXT_PASSWORD = 'Password123'


def fix_all_user_passwords():
    """
    更新 SysUser 表中所有用户的 password_hash 字段，确保其为有效的 bcrypt 哈希。
    """
    print(f"--- 开始修复密码字段 ---")

    # 1. 检查是否有用户存在
    users = db_manager.execute_query("SELECT user_id, username FROM SysUser")
    if not users:
        print("警告: SysUser 表为空，无法修复密码。请先运行 seed_users。")
        return

    # 2. 生成一次性的哈希值
    try:
        hashed_password = hash_password(DEFAULT_PLAINTEXT_PASSWORD)
    except Exception as e:
        print(f"致命错误: 无法生成 bcrypt 哈希，请确保已安装 'bcrypt' 库。错误信息: {e}")
        return

    # 3. 批量更新数据库
    update_sql = "UPDATE SysUser SET password_hash = %s"

    # 为了演示方便，我们假设所有用户的密码都更新为 DEFAULT_PLAINTEXT_PASSWORD
    # 在实际应用中，您应该只更新那些需要修复的用户。
    rows_updated = db_manager.execute_update(update_sql, params=(hashed_password,))

    if rows_updated > 0:
        print(f"✅ 成功更新 {rows_updated} 个用户的密码字段。")
        print(f"现在所有用户的登录密码为: {DEFAULT_PLAINTEXT_PASSWORD}")
    else:
        print("❌ 未找到用户或更新失败。")

    db_manager.close_connection()


if __name__ == "__main__":
    fix_all_user_passwords()
