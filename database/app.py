from flask import Flask, render_template, request, redirect, url_for, session, flash
from functools import wraps
import pymysql.cursors

# 导入现有的模块
from db_utils import db_manager
from auth_service_util import login, register_user, check_permission
from db_features import setup_all_views, setup_all_triggers_and_sps
# 1. 导入持久层 DAO
from forestry_dao import ResourceDAO

app = Flask(__name__)
app.secret_key = 'your_secret_key_123'


# --- 辅助装饰器：检查登录状态 ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('请先登录', 'warning')
            return redirect(url_for('login_route'))
        return f(*args, **kwargs)

    return decorated_function


# --- 1. 认证路由 (保持不变) ---

@app.route('/', methods=['GET', 'POST'])
def login_route():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user_id, msg = login(username, password)
        if user_id:
            session['user_id'] = user_id
            session['username'] = username
            return redirect(url_for('dashboard'))
        else:
            flash(msg, 'danger')
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register_route():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        phone = request.form['phone']
        success, msg = register_user(username, password, phone)
        if success:
            flash('注册成功，请登录', 'success')
            return redirect(url_for('login_route'))
        else:
            flash(msg, 'danger')
    return render_template('register.html')


@app.route('/logout')
def logout():
    session.clear()
    flash('已注销', 'info')
    return redirect(url_for('login_route'))


# --- 2. 基础业务路由 (保持不变) ---

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', username=session['username'])


@app.route('/init_db')
@login_required
def init_db():
    has_perm, msg = check_permission(session['user_id'], '预警：管理规则')
    if not has_perm:
        flash('权限不足：需要系统管理员权限', 'danger')
        return redirect(url_for('dashboard'))
    setup_all_views()
    setup_all_triggers_and_sps()
    flash('视图、触发器和存储过程初始化成功！', 'success')
    return redirect(url_for('dashboard'))


# --- 3. 高级查询路由 (保持不变，省略部分逻辑以节省篇幅，请保留您原有的 handle_query) ---
@app.route('/query/<int:query_id>', methods=['GET', 'POST'])
@login_required
def handle_query(query_id):
    # ... (请保留您之前 app.py 中 handle_query 的完整代码) ...
    # 为确保完整性，这里给出一个简化版，实际请使用您上一轮成功的代码
    user_id = session['user_id']
    results = None;
    columns = None;
    query_config = {
        1: {'perm': None, 'param': '区域编号 (ID)', 'title': '近7天火灾预警'},
        2: {'perm': None, 'param': None, 'title': '设备故障与成本统计'},
        3: {'perm': None, 'param': None, 'title': '成熟资源变动记录'},
        4: {'perm': '资源：创建/更新林草资源', 'param': None, 'title': '24小时内超温区域'},
        5: {'perm': '报表：查看存档', 'param': '报表模板ID', 'title': '报表生成历史'}
    }
    cfg = query_config.get(query_id)
    if not cfg: return redirect(url_for('dashboard'))

    if cfg['param'] and request.method == 'GET':
        return render_template('feature.html', query_id=query_id, config=cfg, show_form=True)

    try:
        from db_features import SQL_COMPLEX_1, SQL_COMPLEX_2, SQL_COMPLEX_3, SQL_COMPLEX_4, SQL_COMPLEX_5
        sql = {1: SQL_COMPLEX_1, 2: SQL_COMPLEX_2, 3: SQL_COMPLEX_3, 4: SQL_COMPLEX_4, 5: SQL_COMPLEX_5}[query_id]
        params = None
        if query_id == 1:
            params = (int(request.form.get('param_val')),)
        elif query_id == 3:
            params = (user_id,)
        elif query_id == 5:
            params = (int(request.form.get('param_val')),)

        raw_results = db_manager.execute_query(sql, params=params)
        if raw_results:
            results = raw_results
            columns = list(raw_results[0].keys())
        else:
            flash("查询成功，但未找到匹配数据。", "info")
    except Exception as e:
        flash(f"查询出错: {str(e)}", "danger")
    return render_template('feature.html', query_id=query_id, config=cfg, results=results, columns=columns)


@app.route('/generate_report', methods=['GET', 'POST'])
@login_required
def generate_report():
    # ... (保留原有的生成报表代码) ...
    has_perm, msg = check_permission(session['user_id'], '报表：生成')
    if not has_perm:
        flash('权限不足: 需要 报表：生成', 'danger')
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        month = request.form['month']
        try:
            sql = f"CALL SP_Generate_Monthly_Summary('{month}', {session['user_id']})"
            db_manager.execute_update(sql)
            flash(f'月度报表 {month} 生成成功。', 'success')
        except Exception as e:
            flash(f'生成失败: {e}', 'danger')
    return render_template('feature.html', is_report=True, title="生成月度报表")


# -----------------------------------------------------------
# 4. 新增：资源管理 CRUD 路由
# -----------------------------------------------------------

@app.route('/resources', methods=['GET', 'POST'])
@login_required
def resources_list():
    """资源列表与管理页面"""
    dao = ResourceDAO()
    results = None

    # 权限检查：查看资源需要权限 (假设 resource:view_public)
    has_perm, _ = check_permission(session['user_id'], '资源：查看公开林草资源')
    if not has_perm:
        flash('权限不足，无法查看资源列表。', 'danger')
        return redirect(url_for('dashboard'))

    # 处理查询 (按区域ID过滤)
    if request.method == 'POST' and 'search_region_id' in request.form:
        region_id = request.form['search_region_id']
        try:
            results = dao.get_resources_by_region(region_id)
            if not results:
                flash(f'区域 {region_id} 暂无数据。', 'info')
        except Exception as e:
            flash(f'查询出错: {e}', 'danger')

    return render_template('resources.html', results=results)


@app.route('/resources/add', methods=['POST'])
@login_required
def resources_add():
    """新增资源"""
    # 权限检查
    has_perm, _ = check_permission(session['user_id'], '资源：创建/更新林草资源')
    if not has_perm:
        flash('操作失败：权限不足 (需要 资源：创建/更新林草资源)', 'danger')
        return redirect(url_for('resources_list'))

    dao = ResourceDAO()
    try:
        new_id = dao.create_resource(
            request.form['region_id'],
            request.form['res_type'],
            request.form['species_name'],
            request.form['amount'],
            request.form['growth_stage']
        )
        if new_id:
            flash(f'资源录入成功！ID: {new_id}', 'success')
        else:
            flash('资源录入失败，请检查输入。', 'danger')
    except Exception as e:
        flash(f'系统错误: {e}', 'danger')

    return redirect(url_for('resources_list'))


@app.route('/resources/update', methods=['POST'])
@login_required
def resources_update():
    """更新资源状态"""
    has_perm, _ = check_permission(session['user_id'], '资源：创建/更新林草资源')
    if not has_perm:
        flash('权限不足', 'danger')
        return redirect(url_for('resources_list'))

    dao = ResourceDAO()
    try:
        r_id = request.form['resource_id']
        stage = request.form['growth_stage']
        amount = request.form['amount']

        # 简单处理：如果数量为空字符串则设为 None (DAO 应处理 None, 或此处不传)
        # 为了兼容 DAO 参数，如果 amount 是空串，我们传 None
        final_amount = amount if amount else None

        if dao.update_resource_status(r_id, stage, final_amount):
            flash(f'资源 {r_id} 更新成功！', 'success')
        else:
            flash('更新失败，ID 可能不存在。', 'warning')
    except Exception as e:
        flash(f'错误: {e}', 'danger')

    return redirect(url_for('resources_list'))


@app.route('/resources/delete', methods=['POST'])
@login_required
def resources_delete():
    """删除资源"""
    has_perm, _ = check_permission(session['user_id'], '资源：创建/更新林草资源')
    if not has_perm:
        flash('权限不足', 'danger')
        return redirect(url_for('resources_list'))

    dao = ResourceDAO()
    try:
        r_id = request.form['resource_id']
        if dao.delete_resource(r_id):
            flash(f'资源 {r_id} 已删除。', 'success')
        else:
            flash('删除失败，ID 可能不存在。', 'warning')
    except Exception as e:
        flash(f'错误: {e}', 'danger')

    return redirect(url_for('resources_list'))


if __name__ == '__main__':
    app.run(debug=True, port=5000)