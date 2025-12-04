import pymysql
from db_utils import db_manager


class ResourceDAO:
    """
    【资源管理业务线】持久层封装
    负责 Resource 表及相关日志的原子操作
    """

    def create_resource(self, region_id, res_type, species_name, amount, growth_stage):
        """新增林草资源 (Create)"""
        sql = """
        INSERT INTO Resource (region_id, res_type, species_name, amount, growth_stage, update_time)
        VALUES (%s, %s, %s, %s, %s, NOW())
        """
        try:
            # execute_update 返回受影响行数，lastrowid 需要 cursor 对象，这里我们假设 db_utils 支持或我们再次查询
            # 为了通用性，我们先执行插入
            rows = db_manager.execute_update(sql, params=(region_id, res_type, species_name, amount, growth_stage))
            if rows > 0:
                # 获取刚刚插入的 ID (在并发不高的情况下使用 MAX ID 是简单的替代方案，严谨生产环境应修改 db_utils 返回 lastrowid)
                res = db_manager.execute_query("SELECT MAX(resource_id) as new_id FROM Resource")
                return res[0]['new_id'] if res else None
            return None
        except Exception as e:
            print(f"DAO Error (Create Resource): {e}")
            return None

    def get_resource_by_id(self, resource_id):
        """根据ID查询资源详情 (Read)"""
        sql = "SELECT * FROM Resource WHERE resource_id = %s"
        results = db_manager.execute_query(sql, params=(resource_id,))
        return results[0] if results else None

    def get_resources_by_region(self, region_id):
        """查询某区域下的所有资源 (Read List)"""
        sql = "SELECT * FROM Resource WHERE region_id = %s"
        return db_manager.execute_query(sql, params=(region_id,))

    def update_resource_status(self, resource_id, new_stage, new_amount=None):
        """
        更新资源状态或数量 (Update)
        注：ResourceChange 日志将由我们之前定义的数据库触发器自动处理，
        持久层只需关注 Resource 表本身的更新。
        """
        params = [new_stage]
        sql = "UPDATE Resource SET growth_stage = %s"

        if new_amount is not None:
            sql += ", amount = %s"
            params.append(new_amount)

        sql += ", update_time = NOW() WHERE resource_id = %s"
        params.append(resource_id)

        rows = db_manager.execute_update(sql, params=tuple(params))
        return rows > 0

    def delete_resource(self, resource_id):
        """删除资源 (Delete)"""
        # 注意：如果有外键约束 (如 ResourceChange)，可能需要先删子表或依赖级联删除
        # 这里假设数据库配置了级联删除或允许直接删除
        sql = "DELETE FROM Resource WHERE resource_id = %s"
        rows = db_manager.execute_update(sql, params=(resource_id,))
        return rows > 0


class EquipmentDAO:
    """
    【设备管理业务线】持久层封装
    """

    def add_equipment(self, name, equip_type, region_id, model, purchase_time):
        """录入新设备档案 (Create)"""
        sql = """
        INSERT INTO Equipment (equip_name, equip_type, region_id, model_spec, purchase_time, status)
        VALUES (%s, %s, %s, %s, %s, 'Normal')
        """
        rows = db_manager.execute_update(sql, params=(name, equip_type, region_id, model, purchase_time))
        if rows > 0:
            res = db_manager.execute_query("SELECT MAX(equip_id) as new_id FROM Equipment")
            return res[0]['new_id'] if res else None
        return None

    def get_equipment_detail(self, equip_id):
        """获取设备档案 (Read)"""
        sql = "SELECT * FROM Equipment WHERE equip_id = %s"
        res = db_manager.execute_query(sql, params=(equip_id,))
        return res[0] if res else None

    def update_status(self, equip_id, status):
        """更新设备运行状态 (Update)"""
        sql = "UPDATE Equipment SET status = %s WHERE equip_id = %s"
        return db_manager.execute_update(sql, params=(status, equip_id)) > 0

    def delete_equipment(self, equip_id):
        """删除设备档案 (Delete)"""
        # 先删除关联的维护记录 (防止外键报错)
        db_manager.execute_update("DELETE FROM Maintenance WHERE equip_id = %s", params=(equip_id,))
        # 再删除设备
        return db_manager.execute_update("DELETE FROM Equipment WHERE equip_id = %s", params=(equip_id,)) > 0
