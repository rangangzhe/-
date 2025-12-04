import sys
import pymysql.cursors
from db_utils import db_manager


def force_fix_stored_procedure():
    print("--- 开始强制修复存储过程 SP_Generate_Monthly_Summary ---")

    conn = db_manager.get_connection()
    if not conn: return

    # 1. 定义最终的、防错的存储过程 DDL
    # 关键点：使用 'ORDER BY template_id LIMIT 1' 强制只返回一行，解决 1172 错误
    sp_ddl = """
    CREATE PROCEDURE SP_Generate_Monthly_Summary(
        IN p_stat_period VARCHAR(7),
        IN p_creator_id INT
    )
    BEGIN
        DECLARE v_report_name VARCHAR(100);
        DECLARE v_template_id INT;

        SET v_report_name = CONCAT('月度综合报告-', p_stat_period);

        -- 1. 确保模板存在
        INSERT IGNORE INTO ReportTemplate (report_name, stat_dimension, stat_indicator, generation_cycle, creator_id)
        VALUES ('月度综合汇总', 'Month', 'All', 'Monthly', p_creator_id);

        -- 2. 获取模板 ID (核心修复：强制限制返回 1 行)
        SELECT template_id INTO v_template_id 
        FROM ReportTemplate 
        WHERE report_name = '月度综合汇总'
        ORDER BY template_id ASC 
        LIMIT 1;

        -- 3. 计算指标
        SELECT AVG(md.value_num) INTO @avg_temp_month
        FROM MonitorData md JOIN Sensor s ON md.sensor_id = s.sensor_id
        WHERE md.collect_time LIKE CONCAT(p_stat_period, '-%') AND s.monitor_type = 'Temperature';

        SELECT COUNT(warning_id) INTO @total_warnings_month
        FROM WarningRecord 
        WHERE trigger_time LIKE CONCAT(p_stat_period, '-%');

        -- 4. 插入结果
        INSERT INTO ReportData (template_id, stat_period, generation_time, file_path, data_source_desc)
        VALUES (
            v_template_id,
            p_stat_period,
            NOW(),
            CONCAT('/reports/', v_report_name, '.pdf'),
            CONCAT('AvgTemp:', IFNULL(@avg_temp_month, 0), '; TotalWarnings:', @total_warnings_month)
        );
    END
    """

    try:
        with conn.cursor() as cursor:
            # 步骤 A: 彻底删除旧的存储过程
            print("1. 删除旧的存储过程...")
            cursor.execute("DROP PROCEDURE IF EXISTS SP_Generate_Monthly_Summary")

            # 步骤 B: 创建新的存储过程
            print("2. 创建新的存储过程 (含 LIMIT 1 修复)...")
            cursor.execute(sp_ddl)

        conn.commit()
        print("✅ 修复成功！存储过程已更新。现在可以处理重复模板数据了。")

    except Exception as e:
        print(f"❌ 修复失败: {e}")
    finally:
        db_manager.close_connection()


if __name__ == "__main__":
    force_fix_stored_procedure()