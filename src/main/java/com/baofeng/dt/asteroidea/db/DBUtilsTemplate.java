package com.baofeng.dt.asteroidea.db;



import com.baofeng.dt.asteroidea.exception.DBExecutionException;
import com.baofeng.dt.asteroidea.util.StringUtil;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.handlers.*;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据库操作模板类
 * 
 * @date 2015-8-17
 * @author zhuhui
 */
public class DBUtilsTemplate {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtilsTemplate.class);
    private static final String CHECK_CONN_SQL = "select 1"; // 校验数据库连接
    private static final int NO_NEED_PAGE = -1;
    private static final String EMPTY_STRING="";

    private  BasicDataSource dataSource;
    
    public DBUtilsTemplate(String driverClassName, String url, String userName, String passWord){
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(userName);
        dataSource.setPassword(passWord);
    }
    public void close() throws SQLException{
        dataSource.close();
    }

    // =================== save by bean ===================================================================

    /**
     * 持久化bean到数据库
     *
     * @param bean
     * @return true成功 false失败
     */
    public boolean save(Object bean) {
        return save(false, EMPTY_STRING, bean);
    }

    public boolean save(Object bean, boolean autoCreateTable) {
        return save(autoCreateTable, EMPTY_STRING, bean);
    }

    public boolean save(Object bean, boolean autoCreateTable, String tableNamePrefix) {
        return save(autoCreateTable, tableNamePrefix, bean);
    }

    public boolean save(List<?> list) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("the bean list cann't be empty");
        }
        return save(false, EMPTY_STRING, list.toArray());
    }

    public boolean save(List<?> list, boolean autoCreateTable) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("the bean list cann't be empty");
        }
        return save(autoCreateTable,EMPTY_STRING, list.toArray());
    }

    public boolean save(List<?> list, boolean autoCreateTable, String tableNamePrefix) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("the bean list cann't be empty");
        }
        return save(autoCreateTable, tableNamePrefix, list.toArray());
    }


    /**
     * 持久化bean到数据库(save核心方法)
     *
     * @param autoCreateTable 是否自动建表 表名为: 前缀 + 类名转下划线风格 eg: AlarmMetric --> ht_alarm_metric
     * @param tableNamePrefix 表名前缀 eg: ht_
     * @param beans           要保存的bean
     * @return true成功 false失败
     */
    public boolean save(boolean autoCreateTable, String tableNamePrefix, Object... beans) {
        if (beans == null || beans.length == 0) {
            throw new DBExecutionException("bean array cann't be empty");
        }

        // 取第一个非null的bean, 以便生成insert语句, 同时校验不能所有的bean都是null
        Object firstNotNullBean = null;
        for (Object bean : beans) {
            if (bean != null) {
                firstNotNullBean = bean;
                break;
            }
        }
        if (firstNotNullBean == null) {
            throw new DBExecutionException("bean cann't be empty");
        }

        QueryRunner q = new QueryRunner(dataSource);
        if (autoCreateTable) {
            createTableByBean(firstNotNullBean, tableNamePrefix);
        }

        // 生成插入SQL语句
        List<String> columnNames = new ArrayList<String>();
        String sql = SQL.insertSQLByClass(firstNotNullBean.getClass(), tableNamePrefix, columnNames);
        String[] columnNameArray = columnNames.toArray(new String[columnNames.size()]);
        // 批量执行
        PreparedStatement pstmt = null;
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            pstmt = conn.prepareStatement(sql);
            for (Object bean : beans) {
                if (bean != null) {
                    q.fillStatementWithBean(pstmt, bean, columnNameArray);
                    pstmt.addBatch();
                }
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            throw new DBExecutionException(e);
        } finally {
            DbUtils.closeQuietly(pstmt);
            DbUtils.closeQuietly(conn);
        }
        return true;
    }

    // =================== query by bean ===================================================================
    public <T> List<T> findAll(Class<T> entityClass, String tableNamePrefix) {
        return findByPage(entityClass, tableNamePrefix, EMPTY_STRING, EMPTY_STRING, NO_NEED_PAGE, NO_NEED_PAGE);
    }
    public <T> List<T> findAll(Class<T> entityClass, String where, String orderBy, Object... params) {
        return findByPage(entityClass, EMPTY_STRING, where, orderBy, NO_NEED_PAGE, NO_NEED_PAGE, params);
    }
    public <T> List<T> findAll(Class<T> entityClass, String tableNamePrefix, String where, String orderBy, Object... params) {
        return findByPage(entityClass, tableNamePrefix, where, orderBy, NO_NEED_PAGE, NO_NEED_PAGE, params);
    }

    /**
     * 根据实体查询(查询核心方法)
     *
     * @param entityClass     实体class
     * @param tableNamePrefix 表前缀
     * @param where           where条件体(不用含where关键字)
     * @param orderBy         排序字段(不用含order by)
     * @param pageSize        分页大小
     * @param pageNumber      页码,从1开始
     * @param params          条件参数
     * @param <T>             对应实体类型
     * @return
     */
    public <T> List<T> findByPage(Class<T> entityClass,
                                  String tableNamePrefix,
                                  String where,
                                  String orderBy,
                                  int pageSize,
                                  int pageNumber,
                                  Object... params) {
        String sql = SQL.selectSQLByClass(entityClass, tableNamePrefix, where, orderBy, pageSize, pageNumber);
        LOG.debug("select sql: {}", sql);
        QueryRunner q = new QueryRunner(dataSource);
        List<T> list;
        try {
            if (params == null || params.length == 0) {
                list = q.query(sql, new BeanListHandler<T>(entityClass,new BasicRowProcessor( new EnumBeanProcessor())));
            } else {
                list = q.query(sql, new BeanListHandler<T>(entityClass,new BasicRowProcessor( new EnumBeanProcessor())), params);
            }
        } catch (SQLException e) {
            throw new DBExecutionException(e);
        }
        return list;
    }

    /**
     * 根据bean 创建表
     *
     * @param bean
     */
    public void createTableByBean(Object bean, String tableNamePrefix) {
        if (!checkTableExists(bean, tableNamePrefix)) {
            String sql = SQL.createTableSQLByClass(bean.getClass(), tableNamePrefix);
            try {
                QueryRunner q = new QueryRunner(dataSource);
                q.update(sql);
            } catch (SQLException e) {
                LOG.error("fail to create table:" + sql, e);
            }
        }
    }
    // =================== by bean end ===================================================================

    /**
     * @param sql 插入sql语句
     * @param params 插入参数
     * @return 返回影响行数
     */
    public int insert(String sql, Object... params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        int affectedRows = 0;
        try {
            if (params == null) {
                affectedRows = queryRunner.update(sql);
            } else {
                affectedRows = queryRunner.update(sql, params);
            }
        } catch (SQLException e) {
            LOG.error("insert.插入记录错误：" + sql, e);
        }
        return affectedRows;
    }

    /**
     * 插入数据库，返回自动增长的主键
     * 
     * @param sql - 执行的sql语句
     * @return 主键 注意；
     */
    public int insertForKeys(String sql, Object[] params) {
        int key = 0;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ParameterMetaData pmd = stmt.getParameterMetaData();
            if (params.length < pmd.getParameterCount()) {
                throw new SQLException("error parameter:" + pmd.getParameterCount());
            }
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            stmt.executeUpdate();
            rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                key = rs.getInt(1);
            }
        } catch (SQLException e) {
            LOG.error("insertForKey.插入返回主键错误：" + sql, e);
        } finally {
            if (rs != null) { // 关闭记录集
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (stmt != null) { // 关闭声明
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
            if (conn != null) { // 关闭连接对象
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
        return key;
    }

    private ScalarHandler scalarHandler = new ScalarHandler() {

        @Override
        public Object handle(ResultSet rs) throws SQLException {
            Object obj = super.handle(rs);
            if (obj instanceof BigInteger) return ((BigInteger) obj).longValue();
            return obj;
        }
    };

    public long count(String sql, Object... params) {
        Number num = 0;
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            if (params == null) {
                num = (Number) queryRunner.query(sql, scalarHandler);
            } else {
                num = (Number) queryRunner.query(sql, scalarHandler, params);
            }
        } catch (SQLException e) {
            LOG.error("count.统计数量错误" + sql, e);
        }
        return (num != null) ? num.longValue() : -1;
    }

    /**
     * 执行sql语句
     * 
     * @param sql sql语句
     * @return 受影响的行数
     */
    public int update(String sql) {
        return update(sql, null);
    }

    /**
     * 单条修改记录
     * 
     * @param sql sql语句
     * @param param 参数
     * @return 受影响的行数
     */
    public int update(String sql, Object param) {
        return update(sql, new Object[] { param });
    }

    /**
     * 单条修改记录
     * 
     * @param sql sql语句
     * @param params 参数数组
     * @return 受影响的行数
     */
    public int update(String sql, Object... params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        int affectedRows = 0;
        try {
            if (params == null) {
                affectedRows = queryRunner.update(sql);
            } else {
                affectedRows = queryRunner.update(sql, params);
            }
        } catch (SQLException e) {
            LOG.error("update.单条修改记录错误：" + sql, e);
        }
        return affectedRows;
    }

    /**
     * 批量修改记录
     * 
     * @param sql sql语句
     * @param params 二维参数数组
     * @return 受影响的行数的数组
     */
    public int[] batchUpdate(String sql, Object[][] params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        int[] affectedRows = new int[0];
        try {
            affectedRows = queryRunner.batch(sql, params);
        } catch (SQLException e) {
            LOG.error("update.批量修改记录错误：" + sql, e);
        }
        return affectedRows;
    }

    /**
     * 执行查询，将每行的结果保存到一个Map对象中，然后将所有Map对象保存到List中
     * 
     * @param sql sql语句
     * @return 查询结果
     */
    public List<Map<String, Object>> find(String sql) {
        return find(sql, null);
    }

    /**
     * 执行查询，将每行的结果保存到一个Map对象中，然后将所有Map对象保存到List中
     * 
     * @param sql sql语句
     * @param param 参数
     * @return 查询结果
     */
    public List<Map<String, Object>> find(String sql, Object param) {
        return find(sql, new Object[] { param });
    }

    /**
     * 执行查询，将每行的结果保存到一个Map对象中，然后将所有Map对象保存到List中
     * 
     * @param sql sql语句
     * @param params 参数数组
     * @return 查询结果
     */
    public List<Map<String, Object>> findPage(String sql, int page, int count, Object... params) {
        sql = sql + " LIMIT ?,?";
        QueryRunner queryRunner = new QueryRunner(dataSource);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            if (params == null) {
                list = (List<Map<String, Object>>) queryRunner.query(sql, new MapListHandler(), page, count);
            } else {
                list =
                        (List<Map<String, Object>>) queryRunner.query(sql, new MapListHandler(),
                            ArrayUtils.addAll(params, new Integer[] { page, count }));
            }
        } catch (SQLException e) {
            LOG.error("map 数据分页查询错误", e);
        }
        return list;
    }

    /**
     * 执行查询，将每行的结果保存到一个Map对象中，然后将所有Map对象保存到List中
     * 
     * @param sql sql语句
     * @param params 参数数组
     * @return 查询结果
     */
    public List<Map<String, Object>> find(String sql, Object[] params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            if (params == null) {
                list = (List<Map<String, Object>>) queryRunner.query(sql, new MapListHandler());
            } else {
                list = (List<Map<String, Object>>) queryRunner.query(sql, new MapListHandler(), params);
            }
        } catch (SQLException e) {
            LOG.error("map 数据查询错误", e);
        }
        return list;
    }

    /**
     * 执行查询，将每行的结果保存到Bean中，然后将所有Bean保存到List中
     * 
     * @param entityClass 类名
     * @param sql sql语句
     * @param params 参数数组
     * @return 查询结果
     */
    public <T> List<T> find(Class<T> entityClass, String sql, Object... params) throws SQLException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        List<T> list = new ArrayList<T>();

        if (params == null || params.length == 0) {
            list = (List<T>) queryRunner.query(sql, new BeanListHandler<T>(entityClass,new BasicRowProcessor( new EnumBeanProcessor())));
        } else {
            list = (List<T>) queryRunner.query(sql, new BeanListHandler<T>(entityClass,new BasicRowProcessor( new EnumBeanProcessor())), params);
        }

        return list;
    }

    /**
     * 查询出结果集中的第一条记录，并封装成对象
     * 
     * @param entityClass 类名
     * @param sql sql语句
     * @return 对象
     */
    public <T> T findFirst(Class<T> entityClass, String sql) {
        return findFirst(entityClass, sql, null);
    }

    /**
     * 查询出结果集中的第一条记录，并封装成对象
     * 
     * @param entityClass 类名
     * @param sql sql语句
     * @param param 参数
     * @return 对象
     */
    public <T> T findFirst(Class<T> entityClass, String sql, Object param) {
        return findFirst(entityClass, sql, new Object[] { param });
    }

    /**
     * 查询出结果集中的第一条记录，并封装成对象
     * 
     * @param entityClass 类名
     * @param sql sql语句
     * @param params 参数数组
     * @return 对象
     */
    @SuppressWarnings("unchecked")
    public <T> T findFirst(Class<T> entityClass, String sql, Object[] params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        Object object = null;
        try {
            if (params == null) {
                object = queryRunner.query(sql, new BeanHandler<T>(entityClass));
            } else {
                object = queryRunner.query(sql, new BeanHandler<T>(entityClass), params);
            }
        } catch (SQLException e) {
            LOG.error("返回一条记录错误：findFirst" + e.getMessage());
        }
        return (T) object;
    }

    /**
     * 查询出结果集中的第一条记录，并封装成Map对象
     * 
     * @param sql sql语句
     * @return 封装为Map的对象
     */
    public Map<String, Object> findFirst(String sql) {
        return findFirst(sql, null);
    }

    /**
     * 查询出结果集中的第一条记录，并封装成Map对象
     * 
     * @param sql sql语句
     * @param param 参数
     * @return 封装为Map的对象
     */
    public Map<String, Object> findFirst(String sql, Object param) {
        return findFirst(sql, new Object[] { param });
    }

    /**
     * 查询出结果集中的第一条记录，并封装成Map对象
     * 
     * @param sql sql语句
     * @param params 参数数组
     * @return 封装为Map的对象
     */
    public Map<String, Object> findFirst(String sql, Object[] params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        Map<String, Object> map = null;
        try {
            if (params == null) {
                map = (Map<String, Object>) queryRunner.query(sql, new MapHandler());
            } else {
                map = (Map<String, Object>) queryRunner.query(sql, new MapHandler(), params);
            }
        } catch (SQLException e) {
            LOG.error("findFirst.查询一条记录错误" + sql, e);
        }
        return map;
    }

    /**
     * 查询某一条记录，并将指定列的数据转换为Object
     * 
     * @param sql sql语句
     * @return 结果对象
     */
    public Object findBy(String sql, String params) {
        return findBy(sql, params, null);
    }

    /**
     * 查询某一条记录，并将指定列的数据转换为Object
     * 
     * @param sql sql语句
     * @param columnName 列名
     * @param param 参数
     * @return 结果对象
     */
    public Object findBy(String sql, String columnName, Object param) {
        return findBy(sql, columnName, new Object[] { param });
    }

    /**
     * 查询某一条记录，并将指定列的数据转换为Object
     * 
     * @param sql sql语句
     * @param columnName 列名
     * @param params 参数数组
     * @return 结果对象
     */
    public Object findBy(String sql, String columnName, Object[] params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        Object object = null;
        try {
            if (params == null) {
                object = queryRunner.query(sql, new ScalarHandler(columnName));
            } else {
                object = queryRunner.query(sql, new ScalarHandler(columnName), params);
            }
        } catch (SQLException e) {
            LOG.error("findBy 错误" + sql, e);
        }
        return object;
    }

    /**
     * 查询某一条记录，并将指定列的数据转换为Object
     * 
     * @param sql sql语句
     * @param columnIndex 列索引
     * @return 结果对象
     */
    public Object findBy(String sql, int columnIndex) {
        return findBy(sql, columnIndex, null);
    }

    /**
     * 查询某一条记录，并将指定列的数据转换为Object
     * 
     * @param sql sql语句
     * @param columnIndex 列索引
     * @param param 参数
     * @return 结果对象
     */
    public Object findBy(String sql, int columnIndex, Object param) {
        return findBy(sql, columnIndex, new Object[] { param });
    }

    /**
     * 查询某一条记录，并将指定列的数据转换为Object
     * 
     * @param sql sql语句
     * @param columnIndex 列索引
     * @param params 参数数组
     * @return 结果对象
     */
    public Object findBy(String sql, int columnIndex, Object[] params) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        Object object = null;
        try {
            if (params == null) {
                object = queryRunner.query(sql, new ScalarHandler(columnIndex));
            } else {
                object = queryRunner.query(sql, new ScalarHandler(columnIndex), params);
            }
        } catch (SQLException e) {
            LOG.error("findBy.错误" + sql, e);
        }
        return object;
    }

    /**
     * @param <T>分页查询
     * @param beanClass
     * @param sql
     * @param page
     * @param params
     * @return
     */
    public <T> List<T> findPage(Class<T> beanClass, String sql, int page, int pageSize, Object... params) {
        if (page <= 1) {
            page = 0;
        }
        return query(beanClass, sql + " LIMIT ?,?", ArrayUtils.addAll(params, new Integer[] { page, pageSize }));
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> query(Class<T> beanClass, String sql, Object... params) {
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            return (List<T>) queryRunner.query(sql,
                isPrimitive(beanClass) ? columnListHandler : new BeanListHandler<T>(beanClass,new BasicRowProcessor( new EnumBeanProcessor())), params);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean checkConnection() {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        try {
            queryRunner.query(CHECK_CONN_SQL, columnListHandler);
            return true;
        } catch (SQLException e) {
            LOG.error("checkConnection.校验数据库连接错误:" + CHECK_CONN_SQL, e);
        }
        return false;
    }

    public boolean checkTableExists(Object bean, String tableNamePrefix) {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        try {
            queryRunner.query(SQL.checkTableExistsSQLByClass(bean.getClass(), tableNamePrefix), columnListHandler);
        } catch (SQLException e) {
            if (e.getMessage().contains("doesn't exist")) {
                return false;
            }
        }
        return true;
    }

    private List<Class<?>> PrimitiveClasses = new ArrayList<Class<?>>() {

        private static final long serialVersionUID = 1L;
        {
            add(Long.class);
            add(Integer.class);
            add(String.class);
            add(java.util.Date.class);
            add(Date.class);
            add(Timestamp.class);
        }
    };
    // 返回单一列时用到的handler
    private final static ColumnListHandler columnListHandler = new ColumnListHandler() {

        @Override
        protected Object handleRow(ResultSet rs) throws SQLException {
            Object obj = super.handleRow(rs);
            if (obj instanceof BigInteger) return ((BigInteger) obj).longValue();
            return obj;
        }

    };

    // 判断是否为原始类型
    private boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || PrimitiveClasses.contains(cls);
    }
    
    /***删除数据************************/
    
    public boolean delete(String table,String key,int id){
    	 QueryRunner qr = new QueryRunner(dataSource);
    	 String sql="delete from {0} where {1}=? and 1=1 ";
    	 try {
    		 sql = StringUtil.replaceSequenced(sql,table,key);
			return 1==qr.update(sql, id);
		} catch (SQLException e) {
			LOG.error("删除数据出错，sql:"+sql,e);
		}
    	 return false;
    }
    /**
     * 根据实体查询(查询核心方法)
     *
     * @param entityClass     实体class
     * @param tableNamePrefix 表前缀
     * @param where           where条件体(不用含where关键字)
     * @param orderBy         排序字段(不用含order by)
     * @param pageSize        分页大小
     * @param pageNumber      页码,从1开始
     * @param params          条件参数
     * @param <T>             对应实体类型
     * @return
     */
    public <T> List<T> findEnumByPage(Class<T> entityClass,
                                  String tableNamePrefix,
                                  String where,
                                  String orderBy,
                                  int pageSize,
                                  int pageNumber,
                                  Object... params) {
        String sql = SQL.selectSQLByClass(entityClass, tableNamePrefix, where, orderBy, pageSize, pageNumber);
        LOG.debug("select sql: {}", sql);
        QueryRunner q = new QueryRunner(dataSource);
        List<T> list;
        try {
            if (params == null || params.length == 0) {
                list = q.query(sql, new BeanListHandler<T>(entityClass));
            } else {
                list = q.query(sql, new BeanListHandler<T>(entityClass), params);
            }
        } catch (SQLException e) {
            throw new DBExecutionException(e);
        }
        return list;
    }

}
