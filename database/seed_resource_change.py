import random
from faker import Faker
import sys
from datetime import datetime

# 确保导入 db_utils
try:
    from db_utils import db_manager
except ImportError:
    print("错误：无法导入 db_utils.py。请检查该文件是否在当前目录。")
    sys.exit(1)

# 初始化 Faker
fake = Faker('zh_CN')


# --- 辅助函数：批量执行更新 (与 seeder 脚本中定义一致) ---
def execute_many_updates(sql, data):
    """用于高效批量插入数据，简化代码"""
    conn = db_manager.get_connection()
    if not conn: return 0
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, data)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        conn.rollback()
        if e.args and e.args[0] == 1062:
            print("警告: 尝试插入重复数据，已跳过。")
            return 0
        print(f"批量更新出错: {e}")
        return 0


# --------------------------------------------------------
# ResourceChange 数据填充函数
# --------------------------------------------------------

def seed_resource_change(count=40):
    """填充 ResourceChange 表数据"""
    print(f"--- 正在插入 {count} 条 ResourceChange 记录 ---")

    # 获取外键依赖数据
    resources = db_manager.execute_query("SELECT resource_id, growth_stage FROM Resource WHERE growth_stage = 'Mature'")
    users = db_manager.execute_query("SELECT user_id FROM SysUser")

    if not resources or not users:
        print("❌ 错误：Resource 表中缺少 'Mature' 数据或 SysUser 表为空，无法关联。")
        return 0

    resource_ids = [r['resource_id'] for r in resources]
    user_ids = [u['user_id'] for u in users]

    sql = """
    INSERT INTO ResourceChange (resource_id, change_type, change_reason, operator_id, change_time)
    VALUES (%s, %s, %s, %s, %s)
    """
    insert_data = []

    change_types = ['新增/增加', '减少', '状态更新']

    for _ in range(count):
        r_id = random.choice(resource_ids)
        operator_id = random.choice(user_ids)
        c_type = random.choice(change_types)
        c_reason = f"[{c_type}] {fake.sentence(nb_words=6)}"
        c_time = fake.date_time_between(start_date='-60d', end_date='now')

        insert_data.append((
            r_id,
            c_type,
            c_reason,
            operator_id,
            c_time
        ))

    rows_inserted = execute_many_updates(sql, insert_data)
    print(f"✅ 成功插入 {rows_inserted} 条 ResourceChange 记录。")
    return rows_inserted


# --------------------------------------------------------
# 主执行模块
# --------------------------------------------------------

if __name__ == "__main__":
    try:
        seed_resource_change()
    except Exception as e:
        print(f"系统发生错误: {e}")
    finally:
        db_manager.close_connection()