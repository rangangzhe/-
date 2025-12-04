import pymysql
from pymysql.cursors import DictCursor


class DBManager:
    def __init__(self):
        # 请根据你本地的实际情况修改以下配置
        self.config = {
            'host': 'localhost',  # 数据库地址，本地为 localhost
            'port': 3306,  # 默认端口
            'user': 'root',  # 你的 MySQL 用户名
            'password': 'yua7w83b',  # ⚠️替换为你的 MySQL 密码
            'database': 'IntelligentForestSystem',  # 第一步创建的数据库名
            'charset': 'utf8mb4',
            'cursorclass': DictCursor  # 让查询结果以字典形式返回，方便前端使用
        }
        self.conn = None

    def get_connection(self):
        """获取数据库连接"""
        try:
            # 如果连接不存在或已断开，则重新连接
            if self.conn is None or not self.conn.open:
                self.conn = pymysql.connect(**self.config)
                print("数据库连接成功")
            return self.conn
        except pymysql.MySQLError as e:
            print(f"数据库连接失败: {e}")
            return None

    def close_connection(self):
        """关闭数据库连接"""
        if self.conn and self.conn.open:
            self.conn.close()
            print("数据库连接已关闭")

    def execute_query(self, sql, params=None):
        """执行查询语句 (SELECT)，返回列表"""
        conn = self.get_connection()
        if not conn:
            return None

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                result = cursor.fetchall()
                return result
        except Exception as e:
            print(f"查询出错: {e}")
            return None

    def execute_update(self, sql, params=None):
        """执行更新语句 (INSERT, UPDATE, DELETE)，返回受影响行数"""
        conn = self.get_connection()
        if not conn:
            return None

        try:
            with conn.cursor() as cursor:
                rows = cursor.execute(sql, params)
                conn.commit()  # 提交事务
                return rows
        except Exception as e:
            conn.rollback()  # 发生错误回滚
            print(f"更新出错: {e}")
            return None

# 单例模式：创建一个全局实例供其他模块调用
db_manager = DBManager()
