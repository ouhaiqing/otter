package com.alibaba.otter.node.etl.common.db.utils;

import com.alibaba.otter.node.common.config.ConfigClientService;
import com.alibaba.otter.node.etl.common.db.dialect.DbDialect;
import com.alibaba.otter.node.etl.common.db.dialect.DbDialectFactory;
import com.alibaba.otter.node.etl.extract.extractor.DatabaseExtractor;
import com.alibaba.otter.node.etl.load.loader.db.DbLoadAction;
import com.alibaba.otter.shared.common.model.config.data.ColumnPair;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SyncUtils {

    protected final static Logger logger = LoggerFactory.getLogger(SyncUtils.class);
    private final static int DEFAUTL_PAGE_SIZE = 1000;

    //该脚本执行必须要有转义符号
    private final static String ESCAPE = "\\`";
    private final static String SOURCE_JDBC_URL = "SOURCE_JDBC_URL";
    private final static String SOURCE_DB_NAME = "SOURCE_DB_NAME";
    private final static String HIVE_COLUMN_TO_TYPE = "HIVE_COLUMN_TO_TYPE";
    private final static String SOURCE_TABLE_NAME = "SOURCE_TABLE_NAME";
    private final static String TARGET_DB_NAME = "TARGET_DB_NAME";
    private final static String TARGET_TABLE_NAME = "TARGET_TABLE_NAME";
    private final static String TARGET_COLUMNS = "TARGET_COLUMNS";
    private final static String INIT_THREAD_NUM = "INIT_THREAD_NUM";
    private final static String USERNAME = "USERNAME";
    private final static String PASSWORD = "PASSWORD";


    public static void fullSyncData(DataMediaPair pair, ConfigClientService configClientService, DbDialectFactory dbDialectFactory) {

        List<String> srcColumns = new ArrayList<String>();
        List<String> desColumns = new ArrayList<String>();
        initFullSync(dbDialectFactory, pair.getPipelineId(), pair, srcColumns, desColumns);

        long count = selectCount(dbDialectFactory, pair.getPipelineId(), pair.getSource(), srcColumns.get(0));

        String minId = selectMinId(dbDialectFactory, pair.getPipelineId(), pair.getSource(), srcColumns.get(0));
        boolean numeric = StringUtils.isNumeric(minId);
        if (numeric) {
            Long mid = Long.parseLong(minId);
            if (mid <= 0) {
                //设置  sql_mode可以插入主键为0的数据， 即原主键值不会自增为最大的主键值
                final String sql = "SET sql_mode='NO_AUTO_VALUE_ON_ZERO'";
                excuteSql(dbDialectFactory, pair.getPipelineId(), pair.getTarget(), sql);
            }
        } else {
            DataMedia dataMedia = pair.getSource();
            String schemaName = dataMedia.getNamespace();
            String tableName = dataMedia.getName();
            logger.error("数据库表（" + schemaName + "." + tableName + "）没有使用numericType主键！");
        }
        long start = 0;

        if (!StringUtils.isEmpty(pair.getPosInitialize())) {
            start = Long.valueOf(pair.getPosInitialize());
        }

        long end = start;
        List<Map<String, Object>> batchData;
        while (count > end) {
            end = end + DEFAUTL_PAGE_SIZE;
            batchData = selectByPage(dbDialectFactory, pair.getPipelineId(), pair.getSource(), srcColumns, start, DEFAUTL_PAGE_SIZE);
            if (!batchData.isEmpty()) {
                load(dbDialectFactory, pair.getPipelineId(), pair.getTarget(), batchData, srcColumns, desColumns);
            }
            if (count > end) {
                configClientService.updateDataMediaPair(pair.getId(), false, String.valueOf(end));
            }
            //pageNo++;
            start = end;
        }
        configClientService.updateDataMediaPair(pair.getId(), true, String.valueOf(count));
        logger.info("--------------------------全量初始化完成----------------------------");
    }


    private static void initFullSync(DbDialectFactory dbDialectFactory, Long pipelineId, DataMediaPair pair, List<String> srcColumns, List<String> desColumns) {
        DataMedia dataMedia = pair.getSource();
        DbMediaSource dataSource = (DbMediaSource)dataMedia.getSource();

        DbDialect dbDialect = dbDialectFactory.getDbDialect(pipelineId, dataSource);
        String schemaName = dataMedia.getNamespace();
        String tableName = dataMedia.getName();
        Table table = dbDialect.findTable(schemaName, tableName);
        Column[] pkColumns = table.getPrimaryKeyColumns();
        /*
         * if (pkColumns.length != 1 || !pkColumns[0].isOfNumericType()) { throw new
         * RuntimeException("数据库表（" + schemaName + "." + tableName +
         * "）不是使用自增ID主键，不支持初始化！"); }
         */
        if (pkColumns.length < 1) {
            throw new RuntimeException("数据库表（" + schemaName + "." + tableName + "）没有使用主键， 不支持初始化！");
        }
        /*
         * srcColumns.add(pkColumns[0].getName());
         * desColumns.add(pkColumns[0].getName());
         */
        List<ColumnPair> columnPairs = pair.getColumnPairs();
        String srcColumn,desColumn;
        int index;
        if (columnPairs == null || columnPairs.isEmpty() || pair.getColumnPairMode().isExclude()) {
            // 先添加所有字段映射
            Column[] columns = table.getColumns();
            for (int i = 0, n = columns.length; i < n; i++) {
                srcColumns.add(columns[i].getName());
                desColumns.add(columns[i].getName());
            }
            // 排除字段映射， 当前这个逻辑是有问题的
            if (pair.getColumnPairMode().isExclude() && columnPairs != null && !columnPairs.isEmpty()) {
                for (ColumnPair columnPair: columnPairs) {
                    srcColumn = columnPair.getSourceColumn().getName();
                    if (!StringUtils.equalsIgnoreCase(pkColumns[0].getName(), srcColumn)) {
                        index = srcColumns.indexOf(srcColumn);
                        if (index >= 0) {
                            srcColumns.remove(index);
                            desColumns.remove(index);
                        }
                    }
                }
            }
        }
        else {
            for (ColumnPair columnPair: columnPairs) {
                srcColumn = columnPair.getSourceColumn().getName();
                desColumn = columnPair.getTargetColumn().getName();
                index = srcColumns.indexOf(srcColumn);
                if (index == 0) {
                    desColumns.set(0, desColumn);
                }
                else if (index < 0) {
                    srcColumns.add(srcColumn);
                    desColumns.add(desColumn);
                }
            }
        }
    }


    private static long selectCount(DbDialectFactory dbDialectFactory, Long pipelineId, DataMedia dataMedia, String pkName) {
        DbMediaSource dataSource = (DbMediaSource)dataMedia.getSource();
        String schemaName = dataMedia.getNamespace();
        String tableName = dataMedia.getName();
        DbDialect dbDialect = dbDialectFactory.getDbDialect(pipelineId, dataSource);

        String selectSql = "SELECT COUNT(*) AS count FROM " + schemaName + "." + tableName;
        List<Map<String, Object>> list = dbDialect.getJdbcTemplate().queryForList(selectSql);
        if (list.isEmpty()) {
            return 0L;
        }
        else {
            Object result = list.get(0).get("count");
            if (result == null) {
                return 0L;
            }
            else {
                return ((Number)result).longValue();
            }
        }
    }


    private static String selectMinId(DbDialectFactory dbDialectFactory, Long pipelineId, DataMedia dataMedia, String pkName) {
        DbMediaSource dataSource = (DbMediaSource)dataMedia.getSource();
        String schemaName = dataMedia.getNamespace();
        String tableName = dataMedia.getName();
        DbDialect dbDialect = dbDialectFactory.getDbDialect(pipelineId, dataSource);

        //取别名， 然后存在map的key中
        String selectSql = "SELECT MIN(" + pkName + ") AS id FROM " + schemaName + "." + tableName;
        List<Map<String, Object>> list = dbDialect.getJdbcTemplate().queryForList(selectSql);
        if (list.isEmpty()) {
            return null;
        }
        else {
            Object result = list.get(0).get("id");
            if (result == null) {
                return null;
            }else {
                return result.toString();
            }
        }
    }

    private static List<Map<String, Object>> selectByPage(DbDialectFactory dbDialectFactory, Long pipelineId, DataMedia dataMedia, List<String> srcColumns, long start, long limit) {
        DbMediaSource dataSource = (DbMediaSource)dataMedia.getSource();
        String schemaName = dataMedia.getNamespace();
        String tableName = dataMedia.getName();
        DbDialect dbDialect = dbDialectFactory.getDbDialect(pipelineId, dataSource);
        String pkName = srcColumns.get(0);
        String pageSql = dbDialect.getSqlTemplate().getSelectPageSql(schemaName, tableName, pkName, srcColumns.toArray(new String[0]), start, limit);
        List<Map<String, Object>> list = dbDialect.getJdbcTemplate().queryForList(pageSql);
        return list;
    }

    private static void excuteSql(DbDialectFactory dbDialectFactory, Long pipelineId, final DataMedia dataMedia, String sql) {
        final DbDialect dbDialect = dbDialectFactory.getDbDialect(pipelineId, (DbMediaSource) dataMedia.getSource());
        JdbcTemplate template = dbDialect.getJdbcTemplate();
        template.execute(sql);
    }


    private static void load(DbDialectFactory dbDialectFactory, Long pipelineId, final DataMedia dataMedia, final List<Map<String, Object>> batchData,
                     List<String> srcColumns, List<String> desColumns) {
        final DbDialect dbDialect = dbDialectFactory.getDbDialect(pipelineId, (DbMediaSource) dataMedia.getSource());
        JdbcTemplate template = dbDialect.getJdbcTemplate();
        String schemaName = dataMedia.getNamespace();
        String tableName = dataMedia.getName();

        final Table table = dbDialect.findTable(schemaName, tableName);

        final String[] srcPkColumns = new String[]{srcColumns.get(0)};
        final String[] srcOtherColumns = srcColumns.subList(1, desColumns.size()).toArray(new String[0]);
        String[] desPkColumns = new String[]{desColumns.get(0)};
        String[] desOtherColumns = desColumns.subList(1, desColumns.size()).toArray(new String[0]);

        String sql = dbDialect.getSqlTemplate().getMergeSql(schemaName, tableName, desPkColumns, desOtherColumns, new String[]{}, !dbDialect.isDRDS(),null);

        int[] affects = template.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int idx) throws SQLException {
                doPreparedStatement(ps, srcOtherColumns, srcPkColumns, batchData.get(idx));
            }

            @Override
            public int getBatchSize() {
                return batchData.size();
            }

            private void doPreparedStatement(PreparedStatement ps, String[] columnNames, String[] pkNames, Map<String, Object> data) throws SQLException {
                List<String> columns = new ArrayList<String>();
                columns.addAll(Arrays.asList(columnNames));
                columns.addAll(Arrays.asList(pkNames));
                String columnName;
                for (int i = 0; i < columns.size(); i++) {
                    columnName = columns.get(i);

                    Column[] tColumns = table.getColumns();
                    for (Column c : tColumns) {
                        if (c.getName().equalsIgnoreCase(columnName)) {

                            Object object = data.get(columnName);
                            String type = c.getType();
                            if (type.equalsIgnoreCase("BIGINT") || type.equalsIgnoreCase("INT")) {
                                Long value = null;
                                if (object != null) {
                                    value = Long.parseLong(object.toString());
                                }
                                ps.setObject(i + 1, value);
                            } else if (type.equalsIgnoreCase("CHAR")) {
                                String value = null;
                                if (object != null) {
                                    value = object.toString();
                                }
                                ps.setObject(i + 1, value);
                            } else {
                                ps.setObject(i + 1, object);
                            }
                            break;
                        }
                    }

                }
            }
        });
    }

}
