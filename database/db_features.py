import sys
import pymysql.cursors
from db_utils import db_manager


# --- 辅助函数：处理带 DELIMITER 的 MySQL 语句 ---
def execute_delimited_sql(sql_script):
    """
    处理 MySQL 中包含 DELIMITER 关键字的脚本。
    此版本将整个块作为单个命令执行，以避免内部语句分割问题。
    """
    conn = db_manager.get_connection()
    if not conn: return False

    try:
        # 使用 pymysql 的 connection 对象直接创建游标并执行整个脚本
        cursor = conn.cursor()

        # 移除 DDL 字符串中的 DROP TRIGGER/PROCEDURE IF EXISTS 语句，
        # 仅保留 CREATE 语句块和 DELIMITERs

        # 1. 移除 DROP 语句
        clean_script = "\n".join(
            [line for line in sql_script.split('\n') if not line.strip().upper().startswith('DROP ')])

        # 2. 移除 DELIMITER 语句，并将 CREATE 块作为单个命令执行 (pymysql 支持)
        # 注意: 这里的目的是让 MySQL 知道 CREATE 块是单个语句，以避免内部分号问题

        # 这是一个复杂的步骤，因为 pymysql 不直接支持 DELIMITER。
        # 最安全的方法是让 Python 模拟客户端行为。

        # 将语句按 // 分割，逐条执行
        statements = clean_script.split('//')

        for statement in statements:
            statement = statement.strip()
            if statement and not statement.upper().startswith('DELIMITER'):
                # 检查是否包含 CREATE TRIGGER/PROCEDURE
                if statement.upper().startswith(('CREATE TRIGGER', 'CREATE PROCEDURE')):
                    cursor.execute(statement)
                # 假设其他单行语句如 SET 也可以直接执行
                elif statement.endswith(';'):
                    cursor.execute(statement)

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ 执行 Trigger/SP DDL 时出错: {e}")
        return False


# ----------------------------------------------------------------------
# --- 1. 5 条复杂 SQL 查询 (定义保持不变) ---
# ----------------------------------------------------------------------

SQL_COMPLEX_1 = """
SELECT 
    r.region_name, wr.trigger_time, wr.status AS handling_status,
    wrule.level, u.username AS handler_name, wr.result
FROM WarningRecord wr
JOIN WarningRule wrule ON wr.rule_id = wrule.rule_id
JOIN Region r ON wr.region_id = r.region_id
LEFT JOIN SysUser u ON wr.handler_id = u.user_id
WHERE wrule.rule_type = 'Fire' AND r.region_id = %s
  AND wr.trigger_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
ORDER BY wr.trigger_time DESC;
"""

SQL_COMPLEX_2 = """
SELECT 
    reg.region_name, 
    COUNT(e.equip_id) AS total_faults,
    SUM(m.cost) AS total_maintenance_cost
FROM Equipment e
JOIN Region reg ON e.region_id = reg.region_id
LEFT JOIN Maintenance m ON e.equip_id = m.equip_id
WHERE e.status = 'Fault' OR m.maint_type IN ('Repair', 'Replacement')
GROUP BY reg.region_name;
"""

SQL_COMPLEX_3 = """
SELECT 
    rc.change_time, rc.change_reason, res.species_name, res.amount, u.username AS operator
FROM ResourceChange rc
JOIN Resource res ON rc.resource_id = res.resource_id
JOIN Region reg ON res.region_id = reg.region_id
LEFT JOIN SysUser u ON rc.operator_id = u.user_id
WHERE reg.manager_id = %s AND res.growth_stage = 'Mature'
ORDER BY rc.change_time DESC;
"""

SQL_COMPLEX_4 = """
SELECT 
    reg.region_name, s.sensor_id, s.monitor_type, md.value_num, md.collect_time
FROM MonitorData md
JOIN Sensor s ON md.sensor_id = s.sensor_id
JOIN Region reg ON s.region_id = reg.region_id
WHERE s.monitor_type = 'Temperature' 
  AND md.value_num >= 40.0
  AND md.collect_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
ORDER BY md.collect_time DESC;
"""

SQL_COMPLEX_5 = """
SELECT 
    u.username AS creator_name, t.report_name, rd.generation_time, rd.file_path
FROM ReportData rd
JOIN ReportTemplate t ON rd.template_id = t.template_id
LEFT JOIN SysUser u ON t.creator_id = u.user_id
WHERE t.template_id = %s
ORDER BY rd.generation_time DESC;
"""

# ----------------------------------------------------------------------
# --- 2. 15 个视图 DDL ---
# ----------------------------------------------------------------------

DDL_VIEWS = [
    # 环境监测业务线 (3个)
    ("View_Region_Daily_Avg",
     "SELECT r.region_name, DATE(md.collect_time) as monitor_date, s.monitor_type, AVG(md.value_num) as daily_avg FROM MonitorData md JOIN Sensor s ON md.sensor_id = s.sensor_id JOIN Region r ON s.region_id = r.region_id WHERE md.is_valid = 1 GROUP BY 1, 2, 3;"),
    ("View_Abnormal_Data",
     "SELECT md.data_id, md.collect_time, s.monitor_type, r.region_name FROM MonitorData md JOIN Sensor s ON md.sensor_id = s.sensor_id JOIN Region r ON s.region_id = r.region_id WHERE md.is_valid = 0;"),
    ("View_Ranger_Sensor_Summary",
     "SELECT r.region_name, s.monitor_type, s.status, COUNT(*) as total FROM Sensor s JOIN Region r ON s.region_id = r.region_id GROUP BY 1, 2, 3;"),

    # 灾害预警业务线 (3个)
    ("View_Active_Warning_Rules", "SELECT rule_type, condition_expr, level FROM WarningRule WHERE is_enabled = 1;"),
    ("View_Unhandled_Critical_Alerts",
     "SELECT wr.warning_id, wr.trigger_time, r.region_name, wrl.level FROM WarningRecord wr JOIN Region r ON wr.region_id = r.region_id JOIN WarningRule wrl ON wr.rule_id = wrl.rule_id WHERE wr.status = 'Unprocessed' AND wrl.level IN ('Severe', 'Critical');"),
    ("View_Warning_Stats_Monthly",
     "SELECT rule_type, level, COUNT(warning_id) AS total_warnings FROM WarningRecord wr JOIN WarningRule wrl ON wr.rule_id = wrl.rule_id WHERE wr.trigger_time >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY 1, 2;"),

    # 资源管理业务线 (3个)
    ("View_Resource_Summary_By_Type",
     "SELECT r.region_name, res.res_type, SUM(res.amount) AS total_amount, AVG(res.amount) AS avg_amount FROM Resource res JOIN Region r ON res.region_id = r.region_id GROUP BY 1, 2;"),
    ("View_Mature_Tree_Inventory",
     "SELECT r.region_name, res.species_name, res.amount FROM Resource res JOIN Region r ON res.region_id = r.region_id WHERE res.res_type = 'Tree' AND res.growth_stage = 'Mature';"),
    ("View_Latest_Resource_Changes",
     "SELECT rc.change_time, rc.change_reason, res.species_name, u.username AS operator FROM ResourceChange rc JOIN Resource res ON rc.resource_id = res.resource_id JOIN SysUser u ON rc.operator_id = u.user_id ORDER BY rc.change_time DESC LIMIT 10;"),

    # 设备管理业务线 (3个)
    ("View_Offline_Devices",
     "SELECT equip_name, model_spec, r.region_name, status FROM Equipment e JOIN Region r ON e.region_id = r.region_id WHERE e.status IN ('Offline', 'Fault');"),
    ("View_Upcoming_Warranty_Expiry",
     "SELECT equip_name, model_spec, purchase_time, quality_guarantee_end_date FROM Equipment WHERE quality_guarantee_end_date BETWEEN NOW() AND DATE_ADD(NOW(), INTERVAL 90 DAY);"),
    ("View_Device_Maintenance_Log",
     "SELECT e.equip_name, COUNT(m.maint_id) AS total_maint_count, MAX(m.maint_time) AS last_maint_time FROM Equipment e LEFT JOIN Maintenance m ON e.equip_id = m.equip_id GROUP BY 1;"),

    # 统计分析业务线 (3个)
    ("View_Report_Generation_Log",
     "SELECT rd.report_id, t.report_name, rd.generation_time, rd.file_path FROM ReportData rd JOIN ReportTemplate t ON rd.template_id = t.template_id ORDER BY 3 DESC;"),
    ("View_Report_Template_Usage",
     "SELECT t.report_name, COUNT(rd.report_id) AS usage_count FROM ReportTemplate t LEFT JOIN ReportData rd ON t.template_id = rd.template_id GROUP BY 1;"),
    ("View_System_Health_Ratio",
     "SELECT SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active_count, SUM(CASE WHEN status='Fault' THEN 1 ELSE 0 END) AS fault_count, (SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) / COUNT(*)) AS health_ratio FROM Sensor;")
]

# ----------------------------------------------------------------------
# --- 3. 5 个存储过程/触发器 DDL ---
# ----------------------------------------------------------------------

# T1. 环境监测：数据有效性校验触发器 (最终修正：引入 SELECT INTO)
# T1. 环境监测：数据有效性校验触发器 (修正：直接使用 NEW.monitor_type)
DDL_TRIGGER_1_ENV = """
DROP TRIGGER IF EXISTS Trigger_Check_Validity;
DELIMITER //
CREATE TRIGGER Trigger_Check_Validity BEFORE INSERT ON MonitorData
FOR EACH ROW
BEGIN 
    -- 警告：由于 MonitorData 现在是反规范化的，它必须在插入时设置 monitor_type
    -- (通常在应用层完成，但此处我们依赖于应用层 NEW.monitor_type 字段已赋值)

    -- 1. 检查 Sensor Type 是否已被应用层赋值。如果应用层未赋值，此处应该报错或默认查询。
    -- 为简化逻辑，我们假设应用层在 INSERT 语句中为 NEW.monitor_type 提供了正确的值。

    SET NEW.is_valid = 1;

    -- 检查温度异常
    IF (NEW.monitor_type = 'Temperature' AND (NEW.value_num < -20 OR NEW.value_num > 60)) THEN
        SET NEW.is_valid = 0;
    END IF;

    -- 检查湿度异常
    IF (NEW.monitor_type = 'Humidity' AND (NEW.value_num < 5 OR NEW.value_num > 95)) THEN
        SET NEW.is_valid = 0;
    END IF;
END;
//
DELIMITER ;
"""


# T2. 灾害预警：通知自动发送触发器
DDL_TRIGGER_2_WARNING = """
DROP TRIGGER IF EXISTS Trigger_Auto_Notify;
DELIMITER //
CREATE TRIGGER Trigger_Auto_Notify AFTER INSERT ON WarningRecord
FOR EACH ROW
BEGIN
    DECLARE v_manager_id INT;
    SELECT manager_id INTO v_manager_id FROM Region WHERE region_id = NEW.region_id;
    INSERT INTO Notification (warning_id, receiver_id, notify_method, send_time, status)
    VALUES (NEW.warning_id, v_manager_id, 'SystemMsg', NOW(), 'Sent');
END;
//
DELIMITER ;
"""

# T3. 资源管理：资源变动日志触发器
DDL_TRIGGER_3_RESOURCE = """
DROP TRIGGER IF EXISTS Trigger_Log_Resource_Update;
DELIMITER //
CREATE TRIGGER Trigger_Log_Resource_Update AFTER UPDATE ON Resource
FOR EACH ROW
BEGIN
    IF OLD.amount <> NEW.amount OR OLD.growth_stage <> NEW.growth_stage THEN
        INSERT INTO ResourceChange (resource_id, change_type, change_reason, operator_id, change_time)
        VALUES (
            NEW.resource_id, 
            CASE WHEN NEW.amount > OLD.amount THEN '新增/增加' ELSE '减少' END, 
            CONCAT('数量/阶段变化: ', OLD.amount, '->', NEW.amount, '; ', OLD.growth_stage, '->', NEW.growth_stage),
            NULL, 
            NOW()
        );
    END IF;
END;
//
DELIMITER ;
"""

# SP1. 设备管理：设备状态更新存储过程
DDL_SP_1_EQUIP = """
DROP PROCEDURE IF EXISTS SP_Update_Equip_Status;
DELIMITER //
CREATE PROCEDURE SP_Update_Equip_Status(
    IN p_equipId INT, 
    IN p_maintType VARCHAR(50), 
    IN p_maintCost DECIMAL(10, 2),
    IN p_maintOperatorId INT
)
BEGIN
    INSERT INTO Maintenance (equip_id, maint_type, maint_time, operator_id, cost)
    VALUES (p_equipId, p_maintType, NOW(), p_maintOperatorId, p_maintCost);

    UPDATE Equipment SET status = 'Normal', last_check_time = NOW()
    WHERE equip_id = p_equipId;
END;
//
DELIMITER ;
"""

# SP2. 统计分析：生成月度汇总报表存储过程 (最终修正：使用 MIN/MAX 确保唯一性)
# SP2. 统计分析：生成月度汇总报表存储过程 (最终使用的修正版本)
# DDL_SP_2_STATS: 最终修正版本
DDL_SP_2_STATS = """
DROP PROCEDURE IF EXISTS SP_Generate_Monthly_Summary;
DELIMITER //
CREATE PROCEDURE SP_Generate_Monthly_Summary(
    IN p_stat_period VARCHAR(7),
    IN p_creator_id INT
)
BEGIN
    DECLARE v_report_name VARCHAR(100);
    DECLARE v_template_id INT;

    SET v_report_name = CONCAT('月度综合报告-', p_stat_period);

    INSERT IGNORE INTO ReportTemplate (report_name, stat_dimension, stat_indicator, generation_cycle, creator_id)
    VALUES ('月度综合汇总', 'Month', 'All', 'Monthly', p_creator_id);

    -- 最终修正：使用 MIN() 聚合函数，保证 SELECT INTO 永远只返回一行结果
    SELECT MIN(template_id) INTO v_template_id 
    FROM ReportTemplate WHERE report_name = '月度综合汇总';

    -- 2. 计算核心指标 (计算逻辑保持不变)
    SELECT AVG(md.value_num) INTO @avg_temp_month
    FROM MonitorData md JOIN Sensor s ON md.sensor_id = s.sensor_id
    WHERE md.collect_time LIKE CONCAT(p_stat_period, '-%') AND s.monitor_type = 'Temperature';

    SELECT COUNT(warning_id) INTO @total_warnings_month
    FROM WarningRecord 
    WHERE trigger_time LIKE CONCAT(p_stat_period, '-%');

    -- 3. 将结果插入 ReportData 表
    INSERT INTO ReportData (template_id, stat_period, generation_time, file_path, data_source_desc)
    VALUES (
        v_template_id,
        p_stat_period,
        NOW(),
        CONCAT('/reports/', v_report_name, '.pdf'),
        CONCAT('AvgTemp:', IFNULL(@avg_temp_month, 0), '; TotalWarnings:', @total_warnings_month)
    );
END;
//
DELIMITER ;
"""

# --- 封装执行逻辑 (保持不变) ---

def setup_all_views():
    """创建所有 15 个视图"""
    print("--- 正在创建 15 个视图... ---")

    try:
        conn = db_manager.get_connection()
        with conn.cursor() as cursor:
            for view_name, ddl in DDL_VIEWS:
                full_ddl = f"CREATE OR REPLACE VIEW {view_name} AS {ddl}"
                cursor.execute(full_ddl)
        conn.commit()
        print("✅ 15 个视图创建成功。")
        return True
    except Exception as e:
        print(f"❌ 视图创建失败: {e}")
        return False


# 在 db_features.py 中新增此函数
def execute_atomic_ddl(sql_script):
    """
    专门用于执行包含 DELIMITER 的原子性 DDL 块。
    """
    conn = db_manager.get_connection()
    if not conn: return False

    # pymysql 客户端默认不支持直接执行 DELIMITER 语句，
    # 但我们可以利用游标执行整个块。
    try:
        with conn.cursor() as cursor:
            # 尝试执行 DROP
            try:
                cursor.execute(sql_script.split(';')[0])  # 执行 DROP IF EXISTS
                conn.commit()
            except:
                pass

                # 执行 CREATE 块
            # 注意：这要求 MySQL 服务器能够识别 DELIMITER 关键字，
            # 如果服务器无法识别，则需要在客户端环境执行。
            cursor.execute(sql_script, multi=True)  # 使用 multi=True 尝试执行多语句块
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ ATOMIC DDL 执行失败: {e}")
        return False


def setup_all_triggers_and_sps():
    """创建所有 5 个存储过程和触发器"""
    print("--- 正在创建 5 个 Trigger/SP... ---")

    # 确保所有 DDL 变量名与外部定义一致
    all_ddl = [
        DDL_TRIGGER_1_ENV,
        DDL_TRIGGER_2_WARNING,
        DDL_TRIGGER_3_RESOURCE,
        DDL_SP_1_EQUIP,
        DDL_SP_2_STATS
    ]

    # 修正：直接调用 execute_delimited_sql 函数
    success = True
    for ddl in all_ddl:
        if not execute_delimited_sql(ddl):
            success = False
            break

    if success:
        print("✅ 5 个触发器/存储过程创建成功。")
    return success


def execute_complex_query(user_id, query_number):
    """根据用户选择执行复杂查询"""
    queries = {
        1: (SQL_COMPLEX_1, '区域编号'),
        2: (SQL_COMPLEX_2, '无参数'),
        3: (SQL_COMPLEX_3, '管理员ID'),
        4: (SQL_COMPLEX_4, '无参数'),
        5: (SQL_COMPLEX_5, '报表模板ID')
    }

    if query_number not in queries:
        return "查询编号无效。", None

    sql, param_name = queries[query_number]
    params = None

    if param_name == '区域编号':
        input_param = input(f"请输入查询参数 ({param_name}): ")
        try:
            params = (int(input_param),)
        except ValueError:
            return "参数输入无效，必须是数字。", None
    elif param_name == '管理员ID':
        params = (user_id,)
    elif param_name == '报表模板ID':
        input_param = input(f"请输入查询参数 ({param_name}): ")
        try:
            params = (int(input_param),)
        except ValueError:
            return "参数输入无效，必须是数字。", None

    try:
        results = db_manager.execute_query(sql, params=params)
        return "查询成功。", results
    except Exception as e:
        return f"查询执行失败: {e}", None
