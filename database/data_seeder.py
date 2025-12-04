import random
from faker import Faker
import sys
import datetime

# 确保导入 db_utils 和其他依赖
try:
    from db_utils import db_manager
except ImportError:
    print("错误：无法导入 db_utils.py。请检查该文件是否在当前目录。")
    sys.exit(1)

# 初始化 Faker，设定为中文环境
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
# 核心业务数据填充 (Core Business Data)
# --------------------------------------------------------

def seed_users(count=15):
    """生成系统用户数据 (与 RBAC 兼容)"""
    print(f"正在生成 {count} 条 SysUser 数据...")
    sql = "INSERT IGNORE INTO SysUser (username, password_hash, phone) VALUES (%s, %s, %s)"

    insert_data = []
    insert_data.append(('admin', 'password_hash_default', '13800000001'))

    for _ in range(count):
        username = fake.user_name() + str(random.randint(100, 999))
        phone = fake.phone_number()
        insert_data.append((username, 'password_hash_default', phone))

    execute_many_updates(sql, insert_data)


def seed_regions(count=25):
    """生成区域数据"""
    print(f"正在生成 {count} 条 Region 数据...")

    users = db_manager.execute_query("SELECT user_id FROM SysUser")
    if not users: return 0
    user_ids = [u['user_id'] for u in users]

    types = ['Forest', 'Grassland']
    sql = """
    INSERT IGNORE INTO Region (region_name, region_type, latitude, longitude, manager_id) 
    VALUES (%s, %s, %s, %s, %s)
    """
    insert_data = []

    for i in range(count):
        region_name = f"{fake.city_name()}{random.choice(['东', '南', '西', '北'])}区林草站_{i + 1}号"
        r_type = random.choice(types)
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        manager = random.choice(user_ids)
        insert_data.append((region_name, r_type, lat, lon, manager))

    execute_many_updates(sql, insert_data)


def seed_resources(count=50):
    """生成林草资源数据"""
    print(f"正在生成 {count} 条 Resource 数据...")

    regions = db_manager.execute_query("SELECT region_id FROM Region")
    if not regions: return 0
    region_ids = [r['region_id'] for r in regions]

    sql = """
    INSERT IGNORE INTO Resource (region_id, res_type, species_name, amount, growth_stage, update_time) 
    VALUES (%s, %s, %s, %s, %s, NOW())
    """

    species_list = ['红松', '白桦', '云杉', '羊草', '苜蓿', '杨树', '柳树']
    insert_data = []

    for _ in range(count):
        rid = random.choice(region_ids)
        res_type = random.choice(['Tree', 'Grass'])
        name = random.choice(species_list)
        amount = random.randint(100, 5000) if res_type == 'Tree' else round(random.uniform(50.0, 1000.0), 2)
        stage = random.choice(['Seedling', 'Growing', 'Mature'])
        insert_data.append((rid, res_type, name, amount, stage))

    execute_many_updates(sql, insert_data)


def seed_sensors_and_data(sensor_count=30, data_per_sensor=20):
    """生成传感器及对应的监测数据"""
    print(f"正在生成 {sensor_count} 个 Sensor 及其 MonitorData...")

    regions = db_manager.execute_query("SELECT region_id FROM Region")
    if not regions: return 0

    sensor_sql = """
    INSERT IGNORE INTO Sensor (region_id, model, monitor_type, install_time, status) 
    VALUES (%s, %s, %s, %s, %s)
    """
    monitor_types = ['Temperature', 'Humidity', 'Image']

    sensor_data = []
    for _ in range(sensor_count):
        rid = random.choice([r['region_id'] for r in regions])
        m_type = random.choice(monitor_types)
        model = f"SENS-{fake.word().upper()}-{random.randint(100, 999)}"
        install_time = fake.date_time_this_year()
        status = random.choice(['Active', 'Active', 'Active', 'Fault'])
        sensor_data.append((rid, model, m_type, install_time, status))

    execute_many_updates(sensor_sql, sensor_data)

    # 2. 插入监测数据
    sensors = db_manager.execute_query("SELECT sensor_id, monitor_type FROM Sensor")
    data_sql = """
    INSERT INTO MonitorData (sensor_id, collect_time, value_num, image_path, is_valid) 
    VALUES (%s, %s, %s, %s, %s)
    """

    monitor_data_list = []
    for sensor in sensors:
        # Note: is_valid will be auto-corrected by Trigger_Check_Validity
        for _ in range(data_per_sensor):
            c_time = fake.date_time_between(start_date='-30d', end_date='now')
            val = None
            if sensor['monitor_type'] == 'Temperature':
                val = round(random.uniform(-10, 40), 2)
            elif sensor['monitor_type'] == 'Humidity':
                val = round(random.uniform(10, 90), 2)
            monitor_data_list.append((sensor['sensor_id'], c_time, val, None, 1))

    execute_many_updates(data_sql, monitor_data_list)


def seed_warnings_and_rules():
    """生成预警规则和记录"""
    print("正在生成 WarningRule 与 WarningRecord...")

    rule_sql = "INSERT IGNORE INTO WarningRule (rule_type, condition_expr, level, is_enabled) VALUES (%s, %s, %s, 1)"
    rules_data = [('Fire', 'Temperature >= 38 AND Humidity <= 20', 'Critical'),
                  ('Pest', 'ImageAnalysis == PestDetected', 'Heavy'), ('Drought', 'Humidity <= 15', 'General')]
    execute_many_updates(rule_sql, rules_data)

    rules = db_manager.execute_query("SELECT rule_id, rule_type FROM WarningRule")
    regions = db_manager.execute_query("SELECT region_id FROM Region")
    users = db_manager.execute_query("SELECT user_id FROM SysUser")

    if not (rules and regions and users): return 0

    rec_sql = """
    INSERT INTO WarningRecord (rule_id, region_id, trigger_time, content, status, handler_id, result)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    warning_data = []
    for _ in range(25):
        rid = random.choice([r['region_id'] for r in regions])
        uid = random.choice([u['user_id'] for u in users])
        rule = random.choice(rules)
        status = random.choice(['Unprocessed', 'Processing', 'Closed'])
        warning_data.append(
            (rule['rule_id'], rid, fake.date_time_this_month(), f"监测到{rule['rule_type']}风险", status, uid,
             "已处理完毕" if status == 'Closed' else None))

    execute_many_updates(rec_sql, warning_data)


# --------------------------------------------------------
# 辅助/日志表填充 (Auxiliary/Log Data - Required for Complex Queries)
# --------------------------------------------------------

def seed_reports_and_templates():
    """填充 ReportTemplate 和 ReportData (Q5, SP7 依赖)"""
    print("正在插入 ReportTemplate & ReportData 数据...")
    users = db_manager.execute_query("SELECT user_id FROM SysUser")
    if not users: return 0
    creator_id = random.choice([u['user_id'] for u in users])

    # 插入模板 (Q5 依赖)
    template_sql = "INSERT IGNORE INTO ReportTemplate (template_id, report_name, stat_dimension, generation_cycle, creator_id) VALUES (%s, %s, %s, %s, %s)"
    execute_many_updates(template_sql, [(1, '月度综合汇总', 'Month', 'Monthly', creator_id),
                                        (2, '设备健康报告', 'Region', 'Weekly', creator_id)])

    # 插入报表记录 (Q5 依赖)
    report_sql = """
    INSERT INTO ReportData (template_id, stat_period, generation_time, file_path, data_source_desc) 
    VALUES (%s, %s, %s, %s, %s)
    """
    report_data = []
    for i in range(5):
        period = f"2024-{10 + i:02d}"
        path = f"/reports/monthly_{period}.pdf"
        report_data.append((1, period, fake.date_time_this_year(), path, "Test summary data."))

    execute_many_updates(report_sql, report_data)


def seed_maintenance_and_logs():
    """填充 Maintenance 和 Equipment 档案 (Q2 依赖)"""
    print("正在插入 Maintenance & Equipment Data...")

    equipments = db_manager.execute_query("SELECT equip_id FROM Equipment")
    users = db_manager.execute_query("SELECT user_id FROM SysUser")
    if not equipments or not users: return 0

    equip_ids = [e['equip_id'] for e in equipments]
    user_ids = [u['user_id'] for u in users]

    # 确保 Equipment 表有数据
    equipment_sql = "INSERT IGNORE INTO Equipment (equip_id, equip_name, equip_type, region_id, model_spec, purchase_time, quality_guarantee_end_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    # 插入一些测试 Equipment 数据（假设 ID 从 100 开始，避免与 Sensor ID 冲突）
    equipment_data = []
    regions = db_manager.execute_query("SELECT region_id FROM Region")
    if regions:
        for i in range(5):
            equipment_data.append(
                (100 + i, f"Cam_{i + 1}", 'Camera', random.choice([r['region_id'] for r in regions]), 'HD-XYZ',
                 fake.date_time_this_year(), fake.date_time_this_year(end_datetime=datetime.date(2027, 1, 1))))
    execute_many_updates(equipment_sql, equipment_data)
    equip_ids = [e[0] for e in equipment_data]

    # 插入维护记录 (Q2 依赖)
    maint_sql = """
    INSERT INTO Maintenance (equip_id, maint_type, maint_time, operator_id, cost, maint_content)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    maint_data = []
    for _ in range(30):
        maint_data.append((
            random.choice(equip_ids),
            random.choice(['Repair', 'Inspection']),
            fake.date_time_this_month(),
            random.choice(user_ids),
            round(random.uniform(50, 500), 2),
            fake.text(max_nb_chars=50)
        ))
    execute_many_updates(maint_sql, maint_data)

    # Note: ResourceChange data will be filled via Trigger (DDL_TRIGGER_3_RESOURCE)


# --------------------------------------------------------
# RBAC 数据填充
# --------------------------------------------------------

def seed_roles():
    """插入核心角色数据"""
    print("正在插入 SysRole 数据...")
    roles_data = [('系统管理员', '维护账号和权限分配。'), ('数据管理员', '录入校验资源信息，处理异常，生成报表。'),
                  ('区域护林员', '处理预警，记录维护情况。'), ('监管人员', '查看全系统数据与操作记录。'),
                  ('公众用户', '查看公开数据，提交异常反馈。')]
    sql = "INSERT IGNORE INTO SysRole (role_name, description) VALUES (%s, %s)"
    execute_many_updates(sql, roles_data)


def seed_permissions():
    """插入核心权限数据"""
    print("正在插入 SysPermission 数据...")
    permissions_data = [('monitor:view_region',), ('monitor:view_all',), ('warning:handle',), ('warning:manage_rules',),
                        ('resource:create_update',), ('resource:view_public',), ('equipment:manage_archives',),
                        ('equipment:maintenance_log',), ('report:generate',), ('report:view_archive',)]
    sql = "INSERT IGNORE INTO SysPermission (permission_code) VALUES (%s)"
    execute_many_updates(sql, permissions_data)


def seed_role_permissions():
    """建立角色与权限的关联"""
    print("正在建立 RolePermission 关联...")

    roles = {r['role_name']: r['role_id'] for r in db_manager.execute_query("SELECT role_id, role_name FROM SysRole")}
    perms = {p['permission_code']: p['permission_id'] for p in
             db_manager.execute_query("SELECT permission_id, permission_code FROM SysPermission")}

    role_to_perms = {
        '系统管理员': ['monitor:view_all', 'warning:manage_rules', 'resource:create_update',
                       'equipment:manage_archives', 'report:generate', 'report:view_archive'],
        '数据管理员': ['monitor:view_all', 'resource:create_update', 'report:generate', 'report:view_archive'],
        '区域护林员': ['monitor:view_region', 'warning:handle', 'equipment:maintenance_log', 'resource:create_update'],
        '监管人员': ['monitor:view_all', 'warning:handle', 'report:view_archive'],
        '公众用户': ['resource:view_public', 'monitor:view_region']
    }

    insert_data = []
    for role_name, perm_codes in role_to_perms.items():
        role_id = roles.get(role_name)
        if role_id is None: continue
        for code in perm_codes:
            perm_id = perms.get(code)
            if perm_id:
                insert_data.append((role_id, perm_id))

    sql = "INSERT IGNORE INTO RolePermission (role_id, permission_id) VALUES (%s, %s)"
    execute_many_updates(sql, insert_data)


def seed_user_roles():
    """随机分配用户角色"""
    print("正在建立 UserRole 关联...")

    users = db_manager.execute_query("SELECT user_id FROM SysUser")
    roles = db_manager.execute_query("SELECT role_id, role_name FROM SysRole")
    if not users or not roles: return 0

    insert_data = []
    role_map = {r['role_name']: r['role_id'] for r in roles}

    # 核心角色分配 (ID 1=Admin, ID 2=DataAdmin, etc.)
    core_assignment = [(1, '系统管理员'), (2, '数据管理员'), (3, '区域护林员'), (4, '监管人员'), (5, '公众用户')]
    for user_id, role_name in core_assignment:
        role_id = role_map.get(role_name)
        if role_id and user_id <= len(users):
            insert_data.append((user_id, role_id))

    # 剩余用户随机分配
    remaining_user_ids = [u['user_id'] for u in users if u['user_id'] > len(core_assignment)]
    for user_id in remaining_user_ids:
        insert_data.append((user_id, random.choice([r['role_id'] for r in roles])))

    sql = "INSERT IGNORE INTO UserRole (user_id, role_id) VALUES (%s, %s)"
    execute_many_updates(sql, insert_data)


# --------------------------------------------------------
# 主执行模块
# --------------------------------------------------------

if __name__ == "__main__":
    print("--- 开始填充智慧林草系统数据库数据 ---")

    # --- 基础数据 ---
    seed_users(15)
    seed_regions(25)
    seed_resources(50)
    seed_sensors_and_data(30, 20)
    seed_warnings_and_rules()

    # --- 辅助/日志表数据 ---
    seed_reports_and_templates()
    seed_maintenance_and_logs()

    # --- RBAC 数据 ---
    seed_roles()
    seed_permissions()
    seed_role_permissions()
    seed_user_roles()

    print("\n✅ 所有数据填充完成！")
    db_manager.close_connection()