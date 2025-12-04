import sys
import os
from auth_service_util import register_user, login, check_permission
from db_utils import db_manager

# 尝试导入 Windows 库用于字符屏蔽
try:
    import msvcrt

    IS_WINDOWS = True
except ImportError:
    IS_WINDOWS = False

# --- 全局状态 ---
current_user_id = None
current_username = None


def masked_input(prompt):
    """
    自定义密码输入函数，在 Windows 环境下实现字符屏蔽 (*)。
    若非 Windows 环境，则回退到标准 getpass 行为。
    """
    sys.stdout.write(prompt)
    sys.stdout.flush()
    password = ""

    if IS_WINDOWS:
        while True:
            # 读取单个字符
            char = msvcrt.getch()

            # 检查是否为 Enter 键 (b'\r')
            if char == b'\r' or char == b'\n':
                sys.stdout.write('\n')
                break

            # 检查是否为 Backspace 键 (b'\x08')
            elif char == b'\x08':
                if password:
                    # 倒退一格, 写空格, 倒退一格
                    sys.stdout.write('\b \b')
                    sys.stdout.flush()
                    password = password[:-1]
            # 正常的字符输入
            elif char.isalnum() or char in [b'!', b'@', b'#', b'$', b'%']:  # 限制可输入字符
                char_str = char.decode('utf-8')
                sys.stdout.write('*')
                sys.stdout.flush()
                password += char_str
        return password
    else:
        # 非 Windows 环境回退到标准 getpass，仅不回显，无 *
        try:
            # 尝试使用 getpass 库，避免直接抛出错误
            import getpass
            return getpass.getpass(prompt)
        except ImportError:
            # 如果 getpass 也没有，就直接明文输入 (不推荐)
            return input(prompt)


def handle_registration():
    """处理用户注册流程"""
    print("\n--- [用户注册] ---")
    username = input("请输入用户名 (Username): ")

    # 使用自定义函数屏蔽密码
    password = masked_input("请输入密码 (Password): ")
    confirm_password = masked_input("请再次输入密码: ")

    if password != confirm_password:
        print("❌ 两次密码输入不一致，注册失败。")
        return

    phone = input("请输入联系电话 (可选): ")

    success, message = register_user(username, password, phone)

    if success:
        print(f"\n✅ {message}")
    else:
        print(f"\n❌ 注册失败: {message}")


def handle_login():
    """处理用户登录流程"""
    global current_user_id, current_username

    print("\n--- [用户登录] ---")
    username = input("请输入用户名 (Username): ")

    # 使用自定义函数屏蔽密码
    password = masked_input("请输入密码 (Password): ")

    user_id, message = login(username, password)

    if user_id:
        print(f"\n✅ 登录成功! 欢迎回来，{username}。")
        current_user_id = user_id
        current_username = username
        application_menu()
    else:
        print(f"\n❌ 登录失败: {message}")


def handle_permission_check(user_id):
    """处理 RBAC 权限检查"""
    while True:
        print("\n--- [权限检查] ---")
        print("请选择要检查的权限码 (输入代码或序号):")
        print("  1. 预警：管理预警规则 (管理员权限)")
        print("  2. 资源：查看公开林草资源 (公众用户权限)")
        print("  3. 设备：维护记录 (护林员权限)")
        print("  4. 返回主菜单")

        choice = input("您的选择: ").strip()

        permission_map = {
            '1': '预警：管理预警规则',
            '2': '资源：查看公开林草资源',
            '3': '设备：维护记录',
            '预警：管理预警规则': '预警：管理预警规则',
            '资源：查看公开林草资源': '资源：查看公开林草资源',
            '设备：维护记录': '设备：维护记录'
        }

        if choice in ['4', 'exit', 'quit']:
            break

        permission_code = permission_map.get(choice)

        if permission_code:
            has_perm, msg = check_permission(user_id, permission_code)
            status = "✅ 拥有权限" if has_perm else "❌ 权限不足"
            print(f"\n[{permission_code}] 检查结果: {status}. {msg}")
        else:
            print("输入无效，请重新选择。")


def application_menu():
    """用户登录后的应用主菜单"""
    global current_user_id, current_username

    if not current_user_id:
        print("会话无效，请重新登录。")
        return

    while True:
        print("\n--- [应用主菜单] ---")
        print(f"当前用户: {current_username} (ID: {current_user_id})")
        print("请选择操作:")
        print("  1. RBAC 权限检查")
        print("  2. 注销 (Logout)")

        choice = input("您的选择: ").strip()

        if choice == '1':
            handle_permission_check(current_user_id)
        elif choice == '2':
            print(f"\n👋 {current_username} 已注销。")
            current_user_id = None
            current_username = None
            break
        else:
            print("输入无效，请重新输入。")


def main_menu():
    """主程序入口"""
    print("--- 智慧林草系统 - 认证模块 ---")

    while True:
        print("\n请选择:")
        print("  [1] 用户注册")
        print("  [2] 用户登录")
        print("  [3] 退出系统")

        choice = input("您的选择: ").strip()

        if choice == '1':
            handle_registration()
        elif choice == '2':
            handle_login()
        elif choice == '3':
            print("\n感谢使用，系统退出。")
            db_manager.close_connection()
            sys.exit(0)
        else:
            print("输入无效，请重新输入。")


if __name__ == "__main__":
    try:
        main_menu()
    except Exception as e:
        print(f"\n系统发生致命错误: {e}")
    finally:
        db_manager.close_connection()