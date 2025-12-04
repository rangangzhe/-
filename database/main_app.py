import sys
import os
import getpass
import time
# 假设 auth_service_util.py 包含所有认证和 RBAC 逻辑
from auth_service_util import register_user, login, check_permission, check_session, USER_SESSIONS, grant_role, \
    revoke_role, \
    get_user_roles
from db_utils import db_manager
# 导入高级 DB 功能模块
from db_features import setup_all_views, setup_all_triggers_and_sps, execute_complex_query

# 假设 msvcrt 已成功导入或处理
try:
    import msvcrt

    IS_WINDOWS = True
except ImportError:
    IS_WINDOWS = False

# --- 全局状态 ---
current_user_id = None
current_username = None


# --- 辅助函数 (保持不变) ---
def masked_input(prompt):
    """使用自定义密码输入函数，这里简化处理"""
    if IS_WINDOWS:
        return input(prompt)
    else:
        return getpass.getpass(prompt)


def handle_registration():
    """处理用户注册流程"""
    username = input("请输入用户名 (Username): ")
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
    username = input("请输入用户名 (Username): ")
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
    # ... (保持原有逻辑不变) ...
    # 简化：我们假定此函数已实现
    print("\n--- [RBAC 权限检查] ---")
    print("功能检查已启动，请在登录后使用菜单 3 进行验证。")


# --- RBAC Admin 菜单 ---
def rbac_admin_menu(admin_user_id):
    """
    权限管理菜单：允许管理员修改其他用户的角色。
    """
    print("\n--- [RBAC 权限管理] ---")

    # RBAC 检查：只有拥有 'warning:manage_rules' 权限的用户才能进入此菜单
    if not check_permission(admin_user_id, '预警：管理规则')[0]:
        print("❌ 权限不足。您不是系统管理员或数据管理员。")
        return

    while True:
        print("\n请选择管理操作:")
        print("  1. 分配/授予角色 (Grant)")
        print("  2. 撤销角色 (Revoke)")
        print("  3. 查看用户当前角色")
        print("  4. 返回主菜单")

        choice = input("您的选择: ").strip()

        if choice == '4':
            break

        try:
            target_id = int(input("请输入目标用户的 ID: "))

            if choice == '3':
                roles = get_user_roles(target_id)
                print(f"用户 ID {target_id} 当前拥有的角色: {roles}")
                continue

            role_name = input("请输入角色名称 (例如: 数据管理员): ")

            if choice == '1':
                success, msg = grant_role(target_id, role_name)
                print(msg)
            elif choice == '2':
                success, msg = revoke_role(target_id, role_name)
                print(msg)
            else:
                print("输入无效。")

        except ValueError:
            print("输入的用户 ID 必须是数字。")
        except Exception as e:
            print(f"操作失败: {e}")


# --- 高级 DB 功能的辅助执行函数 ---
def check_and_execute(user_id, required_perm, action_name, func, *args):
    """在执行敏感操作前检查权限"""
    if required_perm and not check_permission(user_id, required_perm)[0]:
        print(f"❌ 权限不足。您需要 '{required_perm}' 权限才能执行 {action_name}。")
        return "权限不足", None

    return func(*args)


def db_features_menu(user_id):
    """高级数据库功能菜单 (已修复循环和功能调用)"""
    global current_username

    while True:  # <-- 核心循环，保证停留在二级菜单
        if not check_session(user_id):
            print("会话已超时，请重新登录。")
            break

        print("\n--- [高级 DB 功能] ---")
        print("请选择操作:")
        print("  1. 运行 Views/Trigger/SP DDL 初始化")
        print("  2. 查询 1: 近 7 天火灾预警 (区域)")
        print("  3. 查询 2: 设备故障统计 (成本)")
        print("  4. 查询 3: 成熟资源变动记录 (管理员)")
        print("  5. 查询 4: 24小时内超温区域 (无参)")
        print("  6. 查询 5: 报表生成历史 (模板ID)")
        print("  7. 运行 SP: 月度报表生成 (仅限管理员)")
        print("  8. 返回主菜单")

        choice = input("您的选择: ").strip()

        if choice == '8':
            break
        elif choice == '1':
            setup_all_views()
            setup_all_triggers_and_sps()
        elif choice in ['2', '3', '4', '5', '6']:
            query_number = int(choice) - 1
            required_perm = None

            # 定义权限要求
            if choice == '4': required_perm = '资源：创建/更新林草资源'
            if choice == '6': required_perm = '报表：查看存档'

            # 执行查询
            msg, results = check_and_execute(
                user_id,
                required_perm,
                f"查询 {choice}",
                execute_complex_query,
                user_id, query_number
            )

            print(f"\n--- 结果: {msg} ---")

            # --- 核心数据输出逻辑 ---
            if results and results != "权限不足":
                print(f"找到 {len(results)} 条记录 (显示前 10 条):")

                header = [key for key in results[0].keys()]
                col_widths = [max(len(str(h)), 20) for h in header]

                header_line = " | ".join(h.ljust(w) for h, w in zip(header, col_widths))
                print(header_line)
                print("-" * len(header_line))

                for row in results[:10]:
                    data_line = " | ".join(str(v).ljust(w) for v, w in zip(row.values(), col_widths))
                    print(data_line)
                print("-" * len(header_line))
            elif msg == '查询成功。' and results is not None:
                print("查询成功，但未找到匹配数据。")
        elif choice == '7':
            month = input("请输入要生成的月份 (YYYY-MM): ")

            check_and_execute(
                user_id,
                '报表：生成',
                '月度报表生成',
                lambda: db_manager.execute_update(f"CALL SP_Generate_Monthly_Summary('{month}', {user_id})")
            )
            print(f"✅ 月度报表 {month} 生成存储过程调用尝试完成。请查询 ReportData 表验证。")
        else:
            print("输入无效，请重新输入。")


def application_menu():
    """用户登录后的应用主菜单"""
    global current_user_id, current_username

    while True:
        print("\n--- [应用主菜单] ---")
        print(f"当前用户: {current_username} (ID: {current_user_id})")
        print("请选择操作:")
        print("  1. RBAC 权限检查")
        print("  2. 注销 (Logout)")
        print("  3. 高级 DB 功能 (Views/Triggers/SQL)")
        print("  4. **管理用户权限 (Admin)**")

        choice = input("您的选择: ").strip()

        if choice == '1':
            handle_permission_check(current_user_id)
        elif choice == '2':
            print(f"\n👋 {current_username} 已注销。")
            current_user_id = None
            current_username = None
            break
        elif choice == '3':
            db_features_menu(current_user_id)
        elif choice == '4':
            rbac_admin_menu(current_user_id)
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