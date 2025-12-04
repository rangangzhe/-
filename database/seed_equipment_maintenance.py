import random
from faker import Faker
import sys
from datetime import datetime, timedelta

# 确保导入 db_utils
try:
    from db_utils import db_manager
except ImportError:
    print("错误：无法导入 db_utils.py。请检查该文件是否在当前目录。")
    sys.exit(1)

# 初始化 Faker
fake = Faker('zh_CN')


# --- 辅助函数：批量执行更新 ---
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
        # 1062: Duplicate entry for key 'PRIMARY' (可忽略)
        if e.args and e.args[0] == 1062:
            print("警告: 尝试插入重复数据，已跳过。")
            return 0
        print(f"批量更新出错: {e}")
        return 0


# --------------------------------------------------------
# 设备和维护数据填充函数
# --------------------------------------------------------

def seed_equipment_and_maintenance():
    """填充 Equipment 和 Maintenance 表数据"""

    users = db_manager.execute_query("SELECT user_id FROM SysUser")
    regions = db_manager.execute_query("SELECT region_id FROM Region")

    if not users or not regions:
        print("❌ 错误：SysUser 或 Region 表中没有数据，无法关联外键。")
        return 0

    user_ids = [u['user_id'] for u in users]
    region_ids = [r['region_id'] for r in regions]

    print("--- 1. 插入新的 Equipment 档案 ---")

    # 插入 Equipment
    equip_sql = """
    INSERT IGNORE INTO Equipment (equip_id, equip_name, equip_type, region_id, model_spec, purchase_time, quality_guarantee_end_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    equipment_data = []
    initial_equip_ids = []

    for i in range(1, 15):  # 插入 14 个新设备
        equip_id = 200 + i
        equip_name = f"AlertUnit_{i}" if i % 3 == 0 else f"Cam_{i}"
        equip_type = 'Alarm' if i % 3 == 0 else 'Camera'

        # 修正点：使用 date_time_between 模拟过去一年的购买日期
        purchase_date = fake.date_time_between(start_date='-1y', end_date='now')
        warranty_end = purchase_date + timedelta(days=random.randint(100, 700))

        equipment_data.append((
            equip_id,
            equip_name,
            equip_type,
            random.choice(region_ids),
            f"Model-X{random.randint(10, 99)}",
            purchase_date,
            warranty_end
        ))
        initial_equip_ids.append(equip_id)

    execute_many_updates(equip_sql, equipment_data)

    print("--- 2. 插入 Maintenance 记录 ---")

    maint_sql = """
    INSERT INTO Maintenance (equip_id, maint_type, maint_time, operator_id, cost, maint_content)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    maint_data = []

    for _ in range(50):  # 插入 50 条维护记录
        maint_data.append((
            random.choice(initial_equip_ids),
            random.choice(['Repair', 'Inspection', 'Replacement']),
            fake.date_time_between(start_date='-60d', end_date='now'),  # 近期维护时间
            random.choice(user_ids),
            round(random.uniform(50, 1500), 2),
            f"Log: {random.choice(['更换电池', '固件升级', '传感器校准'])} performed."
        ))

    execute_many_updates(maint_sql, maint_data)

    print(f"✅ 成功插入 {len(equipment_data)} 条 Equipment 档案和 50 条 Maintenance 记录。")


# --------------------------------------------------------
# 主执行模块
# --------------------------------------------------------

if __name__ == "__main__":
    try:
        seed_equipment_and_maintenance()
    except Exception as e:
        print(f"系统发生错误: {e}")
    finally:
        db_manager.close_connection()