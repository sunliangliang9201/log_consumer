package com.baofeng.dt.asteroidea.db;


import com.baofeng.dt.asteroidea.db.annotation.PrimaryKey;
import com.baofeng.dt.asteroidea.util.NameConvertor;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.*;

/**
 * SQL工具
 * TODO 按方言组织
 *
 * @author mingjia
 * @date 2015-11-12
 */
public class SQL {

    /**
     * 生成insert SQL语句
     *
     * @param cls            sql对应的实体类
     * @param outColumnNames 输出字段名的容器
     * @return insert sql
     */
    public static String insertSQLByClass(Class cls, String tableNamePrefix, List<String> outColumnNames) {
        if (cls == null) {
            return null;
        }

        List<DBColumnDescriptor> cols = getColumns(cls);
        List<String> columnNames = new ArrayList<String>();

        for (DBColumnDescriptor descriptor : cols) {
            if (!descriptor.isPrimarykey || !descriptor.isAutoincrement) {
                columnNames.add(descriptor.colName);
            }
        }

        if (outColumnNames != null) {
            outColumnNames.addAll(columnNames);
        }

        String table = tableName(cls, tableNamePrefix);
        String colLabels = "(" + Joiner.on(",").join(columnNames) + ")";
        String valueLabels = "(" + Joiner.on(",").join(Collections.nCopies(columnNames.size(), "?")) + ")";

        return "insert into " + table + colLabels + " values" + valueLabels;
    }

    /**
     * 生成建表语句
     *
     * @param cls             sql对应的实体类
     * @param tableNamePrefix 表名前缀
     * @return create table
     */
    public static String createTableSQLByClass(Class cls, String tableNamePrefix) {
        List<DBColumnDescriptor> cols = getColumns(cls);
        String table = tableName(cls, tableNamePrefix);

        StringBuilder sql = new StringBuilder("create table ");
        sql.append(table).append("(");
        // 主键字段
        for (DBColumnDescriptor col : cols) {
            if (col.isPrimarykey) {
                sql.append(col.colName).append(" ").append(col.colType).append(" NOT NULL PRIMARY KEY");
                if (col.isAutoincrement) {
                    sql.append(" AUTO_INCREMENT");
                }
                sql.append(",");
                break;
            }
        }
        if (sql.indexOf("PRIMARY KEY") < 0) {
            sql.append("rawdataId int NOT NULL PRIMARY KEY AUTO_INCREMENT,");
        }
        // 其他字段
        for (DBColumnDescriptor col : cols) {
            if (!col.isPrimarykey) {
                sql.append(col.colName).append(" ").append(col.colType).append(",");
            }
        }
        sql.append("updateTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
        sql.append(")");

        return sql.toString();
    }

    /**
     * 生成查询语句
     *
     * @param cls             实体class
     * @param tableNamePrefix 表前缀
     * @param where           查询条件 eg: name=? and age>?
     * @param orderBy         排序字段 eg: id desc
     * @param pageSize        分页大小
     * @param pageNumber      页码, 从1开始
     * @return select * 语句
     */
    public static String selectSQLByClass(Class cls,
                                          String tableNamePrefix,
                                          String where,
                                          String orderBy,
                                          int pageSize,
                                          int pageNumber) {
        StringBuilder sql = new StringBuilder("select * from ");
        sql.append(tableName(cls, tableNamePrefix));
        if (StringUtils.isNotBlank(where)) {
            sql.append(" where ").append(where);
        }
        if (StringUtils.isNotBlank(orderBy)) {
            sql.append(" order by ").append(orderBy);
        }
        if (pageSize > 0 && pageNumber > 0) {
            sql.append(" limit ").append((pageNumber - 1) * pageSize).append(",").append(pageSize);
        }
        return sql.toString();
    }

    /**
     * 生成检测表是否存在的SQL
     *
     * @param cls
     * @return
     */
    public static String checkTableExistsSQLByClass(Class cls, String tableNamePrefix) {
        return "select 1 from " + tableName(cls, tableNamePrefix) + " limit 1";
    }

    /**
     * 生成表名
     *
     * @param cls
     * @param tableNamePrefix
     * @return
     */
    private static String tableName(Class cls, String tableNamePrefix) {
        return (StringUtils.isNotBlank(tableNamePrefix) ? tableNamePrefix.trim() : "") +
                NameConvertor.forDB(cls.getSimpleName());
    }

    private static List<DBColumnDescriptor> getColumns(Class cls) {
        List<DBColumnDescriptor> cols = new ArrayList<DBColumnDescriptor>();
        Field[] fields = cls.getDeclaredFields();
        for (Field f : fields) {
            DBColumnDescriptor descriptor = new DBColumnDescriptor();
            // 基础信息
            descriptor.colName = f.getName();
            descriptor.colType = javaType2DBType(f.getType());
            // 主键信息
            PrimaryKey pk = f.getAnnotation(PrimaryKey.class);
            if (pk != null) {
                descriptor.isPrimarykey = true;
                descriptor.isAutoincrement = pk.isAutoincrement();
            }
            cols.add(descriptor);
        }
        return cols;
    }

    private static final Map<Class, String> TYPE_MAPPING = new HashMap<Class, String>();
    private static final String DEFAULT_DB_TYPE = "varchar(200)";

    static {
        TYPE_MAPPING.put(String.class, "varchar(200)");
        TYPE_MAPPING.put(Integer.class, "int(10)");
        TYPE_MAPPING.put(int.class, "int(10)");
        TYPE_MAPPING.put(Long.class, "bigint(20)");
        TYPE_MAPPING.put(long.class, "bigint(20)");
        TYPE_MAPPING.put(Float.class, "decimal(10,4)");
        TYPE_MAPPING.put(float.class, "decimal(10,4)");
        TYPE_MAPPING.put(Double.class, "decimal(13,4)");
        TYPE_MAPPING.put(double.class, "decimal(13,4)");
        TYPE_MAPPING.put(Boolean.class, "tinyint(1)");
        TYPE_MAPPING.put(boolean.class, "tinyint(1)");
        TYPE_MAPPING.put(Date.class, "datetime");
    }

    private static String javaType2DBType(Class javaType) {
        String type = TYPE_MAPPING.get(javaType);
        return type != null ? type : DEFAULT_DB_TYPE;
    }

    /**
     * 数据库字段描述
     */
    private static class DBColumnDescriptor {
        private boolean isPrimarykey = false;
        private boolean isAutoincrement = false;
        private String colName;
        private String colType = DEFAULT_DB_TYPE;
    }

}
