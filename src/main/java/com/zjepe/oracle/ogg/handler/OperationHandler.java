package com.zjepe.oracle.ogg.handler;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created by WangChao on 2016/8/10.
 */
public class OperationHandler {
    private final static Logger logger = LoggerFactory.getLogger(OperationHandler.class);
    private HandlerProperties handlerProperties;
    //key为columnName，value为clob/blob
    private Map<String,String> bigdataColumnMap = Maps.newHashMap();
    //key为sql，value为大数据字段的实际值
    private List<Map<String,String>> clobSqlList = new ArrayList<Map<String, String>>();
    private List<Map<String,String>> blobSqlList = new ArrayList<Map<String, String>>();

    public void process(Op op, HandlerProperties handlerProperties){
        this.handlerProperties = handlerProperties;

        List<Map<String, String>> rowList = new ArrayList<Map<String, String>>();
        String opType = transformOpType(op, handlerProperties);
        //处理主键更新外的所有逻辑，即新增、非主键更新、删除
        if(!opType.equals("DI")) {
            Map<String, String> rowMap = getRowMap(op, opType);
            rowList.add(rowMap);
        }
        //处理主键更新的逻辑，需要插入两条数据，一条为D，一条为I
        else{
            Map<String, String> rowMap4D = getRowMap4D(op);
            Map<String, String> rowMap4I = getRowMap4I(op);
            rowList.add(rowMap4D);
            rowList.add(rowMap4I);
        }
        execute(rowList, op);
    }

    private void execute(List<Map<String, String>> rowList, Op op) {
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        String oracleTableName = handlerProperties.getOggOracleTableMap().get(fullTableName);
        if(rowList.size() == 2) {
            //处理目标端保留历史情况的逻辑，直接插入
            if(handlerProperties.getTargetDealSituationMap().get(fullTableName).equals("history"))
                doInsert(oracleTableName, rowList, fullTableName);
            //处理目标端保留最终版本的逻辑，先删除原主键在数据库中的记录后再插入
            else if (handlerProperties.getTargetDealSituationMap().get(fullTableName).equals("final")){
                doDelAndInsert(oracleTableName, rowList,op);
            }
        } else if(rowList.size() == 1){
            //处理目标端保留历史情况的逻辑，直接插入
            if(handlerProperties.getTargetDealSituationMap().get(fullTableName).equals("history"))
                doInsert(oracleTableName,rowList,fullTableName);
            //处理目标端保留最终版本的逻辑
            else if(handlerProperties.getTargetDealSituationMap().get(fullTableName).equals("final"))
                doUpdate(oracleTableName,rowList,op);
        }
    }

    private void doDelAndInsert(String oracleTableName, List<Map<String, String>> rowList, Op op) {
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        List<String> sqlList  = new ArrayList<String>();
        String delSql = getDelSql(oracleTableName, rowList.get(0), op);
        sqlList.add(delSql);
        for (Map<String, String> rowMap : rowList){
            String insSql = getInsertSql(oracleTableName, rowMap, fullTableName);
            sqlList.add(insSql);
        }
        executeDb(sqlList);
    }

    private void executeDb(List<String> sqlList) {
        Connection connection = handlerProperties.getConnection();
        Statement st = null;
        int retrys = 0;
        while (true) {
            try {
                connection.setAutoCommit(false);
                for (String sql : sqlList) {
                    st = connection.createStatement();
                    st.execute(sql);
                }
                if(!clobSqlList.isEmpty()){
                    for (Map<String,String> clobSqlMap : clobSqlList){
                        for (Map.Entry<String,String> entry : clobSqlMap.entrySet())
                            extraExecuteDb(entry.getKey(),entry.getValue(), "clob");
                    }
                }
                if(!blobSqlList.isEmpty()){
                    for (Map<String,String> blobSqlMap : blobSqlList){
                        for (Map.Entry<String,String> entry : blobSqlMap.entrySet())
                            extraExecuteDb(entry.getKey(),entry.getValue(),"blob");
                    }
                }
                clobSqlList.clear();
                blobSqlList.clear();
                connection.commit();
                break;
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                if (retrys >= handlerProperties.getRetryCount()) {
                    throw new RuntimeException("Failed writing data. Retry count reaches limit.", e);
                }
                int sleepTime = 3000;
                logger.warn("Write pack failed. Will retry after " + sleepTime + " ms.", e);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e1) {
                }
                retrys++;
            } finally {
                if (st != null) {
                    try {
                        st.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                st = null;
            }
        }
    }

    private void extraExecuteDb(String sql, String columnValue, String columnType) {
        Connection connection = handlerProperties.getConnection();
        Statement st = null;
        BufferedWriter out = null;
        OutputStream outStream = null;
        try {
            st = connection.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                if (columnType.equals("clob")) {
                    if(!StringUtils.isEmpty(columnValue)) {
                        oracle.sql.CLOB clob = (oracle.sql.CLOB) rs.getClob(1);
                        out = new BufferedWriter(clob.getCharacterOutputStream());
                        out.write(columnValue);
                    }
                } else if (columnType.equals("blob")) {
                    if(!StringUtils.isEmpty(columnValue)) {
                        oracle.sql.BLOB blob = (oracle.sql.BLOB) rs.getBlob(1);
                        outStream = blob.getBinaryOutputStream();
                        outStream.write(columnValue.getBytes());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            st = null;
            if(out != null)
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if(outStream != null)
                try {
                    outStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    private String getDelSql(String oracleTableName, Map<String, String> rowMap, Op op) {
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        List<String> keyFieldList = getKeyFieldList(op);
        Map<String,String> keyFieldValueMap =  Maps.newHashMap();
        String delSql;
        //处理目标表结构为保留历史情况的逻辑
        if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")) {
            for (int i = 0; i < keyFieldList.size(); i++) {
                String mapValue = rowMap.get(getColNameAfter(keyFieldList.get(i)));
                if (StringUtils.isEmpty(mapValue))
                    mapValue = rowMap.get(getColNameBefore(keyFieldList.get(i)));
                keyFieldValueMap.put(keyFieldList.get(i), mapValue);
            }
            delSql = initDel(oracleTableName, keyFieldValueMap, "history");
        }
        //处理目标表结构为保留最终版本的逻辑
        else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")) {
            for (int i = 0; i < keyFieldList.size(); i++) {
                String mapValue = rowMap.get(keyFieldList.get(i));
                keyFieldValueMap.put(keyFieldList.get(i), mapValue);
            }
            delSql = initDel(oracleTableName, keyFieldValueMap, "final");
        }
        else
            throw new RuntimeException("不识别的targetDealSituation！" + handlerProperties.getTargetTableTypeMap().get(fullTableName));
        return delSql;
    }

    private void doUpdate(String oracleTableName, List<Map<String, String>> rowList, Op op) {
        for (Map<String, String> rowMap : rowList){
            List<String> sqlList = getUpdateSql(oracleTableName, rowMap, op);
            executeDb(sqlList);
        }
    }

    private void doInsert(String oracleTableName, List<Map<String, String>> rowList, String fullTableName) {
        List<String> sqlList = new ArrayList<String>();
        for (Map<String, String> rowMap : rowList){
            String sql = getInsertSql(oracleTableName, rowMap, fullTableName);
            sqlList.add(sql);
        }
        executeDb(sqlList);
    }

    private List<String> getUpdateSql(String oracleTableName, Map<String, String> rowMap, Op op) {
        List<String> sqlList = new ArrayList<String>();
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        List<String> keyFieldList = getKeyFieldList(op);
        Map<String,String> keyFieldValueMap =  Maps.newHashMap();
        //处理目标表结构为保留历史情况的逻辑
        if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")){
            for (int i = 0; i <keyFieldList.size(); i++) {
                String mapValue = rowMap.get(getColNameAfter(keyFieldList.get(i)));
                if(StringUtils.isEmpty(mapValue))
                    mapValue = rowMap.get(getColNameBefore(keyFieldList.get(i)));
                keyFieldValueMap.put(keyFieldList.get(i), mapValue);
            }
            //新增和非主键更新的逻辑为先删后插，所以有两条sql
            if(!rowMap.get(handlerProperties.getOpTypeFieldName()).equals("D")){
                String delSql = initDel(oracleTableName,keyFieldValueMap,"history");
                String addSql = getInsertSql(oracleTableName, rowMap, fullTableName);
                sqlList.add(delSql);
                sqlList.add(addSql);
            }
            //删除的逻辑为先判断目标端是否存在该主键的记录：有记录则更新；无记录则直接插入。之所以不采用上面的先删后插的逻辑是因为ogg的删除数据只保留了主键的值，其他字段的值都为null，需要把这些值都记录下来
            else{
                String selSql = initSel(oracleTableName, keyFieldValueMap, "history");
                if(checkIfExists(selSql) != 0){
                    String updSql = initUpd(oracleTableName, keyFieldValueMap, rowMap, "history");
                    String updSql2 = initUpd2(oracleTableName, keyFieldValueMap, rowMap, "history");
                    sqlList.add(updSql);
                    sqlList.add(updSql2);
                }else{
                    String addSql = getInsertSql(oracleTableName, rowMap, fullTableName);
                    sqlList.add(addSql);
                }
            }
        }
        //处理目标表结构为保留最终版本的逻辑
        else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")){
            for (int i = 0; i <keyFieldList.size(); i++) {
                String mapValue = rowMap.get(keyFieldList.get(i));
                keyFieldValueMap.put(keyFieldList.get(i), mapValue);
            }
            //新增和更新的逻辑为先删后插，所以有两条sql
            if(!rowMap.get(handlerProperties.getOpTypeFieldName()).equals("D")){
                String delSql = initDel(oracleTableName,keyFieldValueMap,"final");
                String addSql = getInsertSql(oracleTableName, rowMap, fullTableName);
                sqlList.add(delSql);
                sqlList.add(addSql);
            }
            //删除的逻辑为有记录则更新，无记录则直接插入
            else{
                String selSql = initSel(oracleTableName, keyFieldValueMap, "final");
                if(checkIfExists(selSql) != 0){
                    String updSql = initUpd(oracleTableName, keyFieldValueMap, rowMap, "final");
                    sqlList.add(updSql);
                }else{
                    String addSql = getInsertSql(oracleTableName, rowMap, fullTableName);
                    sqlList.add(addSql);
                }
            }
        }
        return  sqlList;
    }

    private String initUpd(String oracleTableName, Map<String, String> keyFieldValueMap, Map<String, String> rowMap, String type) {
        String whereCondition = " where";
        for(Map.Entry<String,String> entry : keyFieldValueMap.entrySet()){
            if(type.equals("history")) {
                whereCondition += " ( " + getColNameBefore(entry.getKey()) + "='" + entry.getValue() + "'";
                whereCondition += " or " + getColNameAfter(entry.getKey()) + "='" + entry.getValue() + "') and";
            }else if (type.equals("final"))
                whereCondition += " "+ entry.getKey() + "='" + entry.getValue() + "' and";
        }
        whereCondition = whereCondition.substring(0,whereCondition.length()-3);

        String updateCondition = "";
        if(type.equals("history")) {
            List<String> fieldList = transformRowMap2FieldList(rowMap);
            for (String field : fieldList) {
                updateCondition += getColNameBefore(field) + "=" + getColNameAfter(field) + ",";
            }
        }
        updateCondition += handlerProperties.getOpTypeFieldName() + "='" + rowMap.get(handlerProperties.getOpTypeFieldName()) + "',";
        updateCondition += handlerProperties.getReadTimeFieldName() + "='" + rowMap.get(handlerProperties.getReadTimeFieldName()) + "'";
        return "update " + oracleTableName + " set " + updateCondition + whereCondition;
    }

    private String initUpd2(String oracleTableName, Map<String, String> keyFieldValueMap, Map<String, String> rowMap, String type) {
        String whereCondition = " where";
        for(Map.Entry<String,String> entry : keyFieldValueMap.entrySet()){
            if(type.equals("history")) {
                whereCondition += " ( " + getColNameBefore(entry.getKey()) + "='" + entry.getValue() + "'";
                whereCondition += " or " + getColNameAfter(entry.getKey()) + "='" + entry.getValue() + "') and";
            }else if (type.equals("final"))
                whereCondition += " "+ entry.getKey() + "='" + entry.getValue() + "' and";
        }
        whereCondition = whereCondition.substring(0,whereCondition.length()-3);

        String updateCondition = "";
        if(type.equals("history")) {
            List<String> fieldList = transformRowMap2FieldList(rowMap);
            for (String field : fieldList) {
                updateCondition += getColNameAfter(field) + "='',";
            }
        }
        updateCondition += handlerProperties.getOpTypeFieldName() + "='" + rowMap.get(handlerProperties.getOpTypeFieldName()) + "',";
        updateCondition += handlerProperties.getReadTimeFieldName() + "='" + rowMap.get(handlerProperties.getReadTimeFieldName()) + "'";
        return "update " + oracleTableName + " set " + updateCondition + whereCondition;
    }

    private List<String> transformRowMap2FieldList(Map<String, String> rowMap) {
        List<String> fieldList = new ArrayList<String>();
        for(Map.Entry<String,String> entry : rowMap.entrySet()){
            if(entry.getKey().endsWith("_before"))
                fieldList.add(entry.getKey().substring(0,entry.getKey().indexOf("_before")));
        }
        return fieldList;
    }

    private int checkIfExists(String selSql) {
        int count = 0;
        Connection connection = handlerProperties.getConnection();
        ResultSet rs = null;
        Statement st = null;
        try {
            st = connection.createStatement();
            rs = st.executeQuery(selSql);
            while (rs.next()){
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            rs = null;
            st = null;
        }
        return count;
    }

    private String initSel(String oracleTableName, Map<String, String> keyFieldValueMap, String type) {
        String whereCondition = " where";
        for(Map.Entry<String,String> entry : keyFieldValueMap.entrySet()){
            if(type.equals("history")) {
                whereCondition += " ( " + getColNameBefore(entry.getKey()) + "='" + entry.getValue() + "'";
                whereCondition += " or " + getColNameAfter(entry.getKey()) + "='" + entry.getValue() + "') and";
            }else if (type.equals("final"))
                whereCondition += " "+ entry.getKey() + "='" + entry.getValue() + "' and";
        }
        whereCondition = whereCondition.substring(0,whereCondition.length()-3);
        return "select count(*) from " + oracleTableName + whereCondition;
    }

    private String initDel(String oracleTableName, Map<String, String> keyFieldValueMap, String type) {
        String whereCondition = " where";
        for(Map.Entry<String,String> entry : keyFieldValueMap.entrySet()){
            if(type.equals("history")) {
                whereCondition += " ( " + getColNameBefore(entry.getKey()) + "='" + entry.getValue() + "'";
                whereCondition += " or " + getColNameAfter(entry.getKey()) + "='" + entry.getValue() + "') and";
            }else if (type.equals("final"))
                whereCondition += " "+ entry.getKey() + "='" + entry.getValue() + "' and";
        }
        whereCondition = whereCondition.substring(0,whereCondition.length()-3);
        return "delete from " + oracleTableName + whereCondition;
    }

    //得到ogg表中的主键列表
    private List<String> getKeyFieldList(Op op){
        List<String> list = new ArrayList<String>();
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        Map<String,String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if (keyFields != null && keyFields.containsKey(colName)){
                list.add(colName);
            }
        }
        return list;
    }

    private String getInsertSql(String oracleTableName, Map<String, String> rowMap, String fullTableName) {
        // Get data
        Map<String,String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Map<String,String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);

        Map<String,String> wholefields = Maps.newHashMap();
        wholefields.putAll(focusFields);
        wholefields.putAll(keyFields);

        String sql = "insert into " + oracleTableName + getInsertSqlPart(rowMap,wholefields).get(0) + "values" + getInsertSqlPart(rowMap,wholefields).get(1);
        //判断是否有大数据字段（clob、blob）
        if(!bigdataColumnMap.isEmpty()){
            for (Map.Entry<String,String> columnMap : bigdataColumnMap.entrySet()) {
                List<Map<String,String>> bigDataSqlMapList = getBigDataSqlMapList(columnMap.getKey(), rowMap, oracleTableName, fullTableName,bigdataColumnMap.keySet());
                if(columnMap.getValue().toLowerCase().equals("clob"))
                    clobSqlList.addAll(bigDataSqlMapList);
                else if(columnMap.getValue().toLowerCase().equals("blob"))
                    blobSqlList.addAll(bigDataSqlMapList);
            }
        }
        bigdataColumnMap.clear();
        return sql;
    }

    private List<Map<String, String>> getBigDataSqlMapList(String columnName, Map<String, String> rowMap, String oracleTableName, String fullTableName, Set<String> columnNameSet) {
        List<Map<String,String>> bigDataSqlMapList = new ArrayList<Map<String, String>>();
        Map<String,String> remainRowMap = getRemainRowMap(columnNameSet, rowMap, fullTableName);
        //处理目标表结构为保留历史情况的逻辑
        if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")){
            if(!StringUtils.isEmpty(rowMap.get(getColNameBefore(columnName)))){
                Map<String,String> bigDataSqlMap = new HashMap<String, String>();
                String bigDataSql = getBigDataSql(getColNameBefore(columnName),remainRowMap,oracleTableName,fullTableName);
                bigDataSqlMap.put(bigDataSql,rowMap.get(getColNameBefore(columnName)));
                bigDataSqlMapList.add(bigDataSqlMap);
            }
            if(!StringUtils.isEmpty(rowMap.get(getColNameAfter(columnName)))){
                Map<String,String> bigDataSqlMap = new HashMap<String, String>();
                String bigDataSql = getBigDataSql(getColNameAfter(columnName),remainRowMap,oracleTableName,fullTableName);
                bigDataSqlMap.put(bigDataSql,rowMap.get(getColNameAfter(columnName)));
                bigDataSqlMapList.add(bigDataSqlMap);
            }
        }
        //处理目标表结构为保留最终情况的逻辑
        else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")){
            if(!StringUtils.isEmpty(rowMap.get(columnName))){
                Map<String,String> bigDataSqlMap = new HashMap<String, String>();
                String bigDataSql = getBigDataSql(columnName,remainRowMap,oracleTableName,fullTableName);
                bigDataSqlMap.put(bigDataSql,rowMap.get(columnName));
                bigDataSqlMapList.add(bigDataSqlMap);
            }
        }
        return bigDataSqlMapList;
    }

    private String getBigDataSql(String columnName, Map<String, String> remainRowMap, String oracleTableName, String fullTableName) {
        // Get data
        Map<String,String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Map<String,String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);

        Map<String,String> wholefields = Maps.newHashMap();
        wholefields.putAll(focusFields);
        wholefields.putAll(keyFields);

        String whereCondition = " where";
        for(Map.Entry<String,String> entry : remainRowMap.entrySet()){
            String filedName = entry.getKey();
            if(entry.getKey().endsWith("_before") || entry.getKey().endsWith("_after"))
                filedName = entry.getKey().substring(0, entry.getKey().lastIndexOf("_"));
            if(wholefields.containsKey(filedName)) {
                String filedType = wholefields.get(filedName);
                whereCondition += " "+ entry.getKey() + "=" + getColumnValue(filedName,entry.getValue(), filedType) + " and";
            }else
                whereCondition += " "+ entry.getKey() + "='" + entry.getValue() + "' and";
        }
        whereCondition = whereCondition.substring(0,whereCondition.length()-3);
        return "select " + columnName + " from " + oracleTableName + whereCondition + " for update";
    }

    //返回所有字段中除了大数据字段外的其他字段，用于sql的where条件
    private Map<String,String> getRemainRowMap(Set<String> columnNameSet, Map<String, String> rowMap, String fullTableName){
        Map<String,String> remainRowMap = Maps.newHashMap();
        remainRowMap.putAll(rowMap);
        if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")){
            for (String columnName : columnNameSet){
                remainRowMap.remove(getColNameBefore(columnName));
                remainRowMap.remove(getColNameAfter(columnName));
            }
        }
        else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")){
            for (String columnName : columnNameSet)
                remainRowMap.remove(columnName);
        }
        return  remainRowMap;
    }

    private List<String> getInsertSqlPart(Map<String, String> rowMap, Map<String,String> wholefields) {
        List<String> insertSqlPart = new ArrayList<String>();
        String colunmName = " ( ";
        String columnValue = " ( ";
        for(Map.Entry<String,String> entry : rowMap.entrySet()){
            colunmName += entry.getKey() + ",";

            String filedName = entry.getKey();
            if(entry.getKey().endsWith("_before") || entry.getKey().endsWith("_after"))
                filedName = entry.getKey().substring(0, entry.getKey().lastIndexOf("_"));
            if(wholefields.containsKey(filedName)) {
                String filedType = wholefields.get(filedName);
                columnValue += getColumnValue(filedName,entry.getValue(), filedType) + ",";
            }else
                columnValue += "'" + entry.getValue() + "',";
        }
        colunmName = colunmName.substring(0,colunmName.length()-1) + " ) ";
        columnValue = columnValue.substring(0,columnValue.length()-1) + " ) ";
        insertSqlPart.add(colunmName);
        insertSqlPart.add(columnValue);
        return  insertSqlPart;
    }

    //根据字段类型返回值
    private String getColumnValue(String filedName, String filedValue, String filedType){
            if (filedType.toLowerCase().equals("string")) {
                if(StringUtils.isEmpty(filedValue))
                    return "''";
                else
                    return "'" + filedValue + "'";
            }else if (filedType.toLowerCase().equals("number"))
                if(StringUtils.isEmpty(filedValue))
                    return "''";
                else
                    return filedValue;
            else if (filedType.toLowerCase().equals("clob")) {
                bigdataColumnMap.put(filedName,"clob");
                return "EMPTY_CLOB()";
            }
            else if (filedType.toLowerCase().equals("blob")) {
                bigdataColumnMap.put(filedName,"blob");
                return "EMPTY_BLOB()";
            } else
                throw new RuntimeException("不识别的字段类型：" + filedType);
    }

    /**
     * @param op
     * @param opType
     * @return rowMap即对应表中一行的数据
     */
    private Map<String, String> getRowMap(Op op, String opType) {
        // Get data
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        Map<String,String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Map<String,String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();

        Map<String, String> rowMap = Maps.newHashMap();
        rowMap.put(handlerProperties.getOpTypeFieldName(), opType);
        rowMap.put(handlerProperties.getReadTimeFieldName(), op.getOperation().getReadTimeAsString());

        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if ((focusFields != null && focusFields.containsKey(colName)) || (keyFields != null && keyFields.containsKey(colName))) {
                //处理目标表结构为保留历史情况的逻辑
                if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")) {
                    rowMap.put(getColNameBefore(colName), cols.get(i).getBeforeValue());
                    rowMap.put(getColNameAfter(colName), cols.get(i).getAfterValue());
                }
                //处理目标表结构为保留最终版本的逻辑
                else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")){
                    String colValue = cols.get(i).getAfterValue();
                    rowMap.put(colName, colValue);
                }
            }
        }
        return rowMap;
    }

    private Map<String, String> getRowMap4I(Op op) {
        // Get data
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        Map<String,String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Map<String,String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();

        Map<String, String> rowMap = Maps.newHashMap();
        rowMap.put(handlerProperties.getOpTypeFieldName(), "I");
        rowMap.put(handlerProperties.getReadTimeFieldName(), op.getOperation().getReadTimeAsString());
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if ((focusFields != null && focusFields.containsKey(colName)) || (keyFields != null && keyFields.containsKey(colName))) {
                //处理目标表结构为保留历史情况的逻辑
                if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")) {
                    rowMap.put(getColNameBefore(colName), cols.get(i).getBeforeValue());
                    rowMap.put(getColNameAfter(colName), cols.get(i).getAfterValue());
                }
                //处理目标表结构为保留最终版本的逻辑
                else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")){
                    String colValue = cols.get(i).getAfterValue();
                    rowMap.put(colName, colValue);
                }
            }
        }
        return rowMap;
    }

    private Map<String, String> getRowMap4D(Op op) {
        // Get data
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        Map<String,String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Map<String,String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();

        Map<String, String> rowMap = Maps.newHashMap();
        rowMap.put(handlerProperties.getOpTypeFieldName(), "D");
        rowMap.put(handlerProperties.getReadTimeFieldName(), op.getOperation().getReadTimeAsString());
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if ((focusFields != null && focusFields.containsKey(colName)) || (keyFields != null && keyFields.containsKey(colName))) {
                //处理目标表结构为保留历史情况的逻辑
                if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")) {
                    rowMap.put(getColNameBefore(colName), cols.get(i).getBeforeValue());
                    rowMap.put(getColNameAfter(colName), null);
                }
                //处理目标表结构为保留最终版本的逻辑
                else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")){
                    String colValue = cols.get(i).getBeforeValue();
                    rowMap.put(colName, colValue);
                }
            }
        }
        return rowMap;
    }

    /**
     * @param op
     * @param handlerProperties
     * @return 处理类型: I（新增） D(删除) U(非主键更新) DI(主键更新)
     */
    private String transformOpType(Op op, HandlerProperties handlerProperties){
        String opType;
        String operationType = op.getOperationType().fullString().toUpperCase();

        if(operationType.toUpperCase().equals("INSERT(5)")) {
            opType = "I";
            handlerProperties.totalInserts++;
            handlerProperties.totalOperations++;
        }
        else if(operationType.toUpperCase().equals("UPDATE(15)") || operationType.toUpperCase().equals("UPDATE_AUDITCOMP(15)")
                || operationType.equals("UPDATE_FIELDCOMP(15)") || operationType.equals("UNIFIED_UPDATE_VAL(15)") || operationType.equals("UNIFIED_PK_UPDATE_VAL(15)")) {
            opType = "U";
            handlerProperties.totalUpdates++;
            handlerProperties.totalOperations++;
        }
        else if(operationType.toUpperCase().equals("DELETE(3)")) {
            opType = "D";
            handlerProperties.totalDeletes++;
            handlerProperties.totalOperations++;
        }
        else if(operationType.toUpperCase().equals("UPDATE_FIELDCOMP_PK(115)")){
            String fullTableName = op.getTableName().getFullName().toLowerCase();
            Map<String,String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
            // Key Cols and Focus Cols
            List<DsColumn> cols = op.getColumns();
            boolean flag = false;
            for (int i = 0; i < cols.size(); i++) {
                String colName = op.getTableMeta().getColumnName(i).toLowerCase();
                if (keyFields != null && keyFields.containsKey(colName)) {
                    if(!cols.get(i).getBeforeValue().equals(cols.get(i).getAfterValue())) {
                        flag = true;
                        break;
                    }
                }
            }
            if(flag) {
                opType = "DI";
                handlerProperties.totalDeletes++;
                handlerProperties.totalInserts++;
                handlerProperties.totalOperations += 2;
            }else {
                opType = "U";
                handlerProperties.totalUpdates++;
                handlerProperties.totalOperations++;
            }
        }else
            throw new RuntimeException("不识别的类型: " + operationType);
        return opType;
    }

    private String getColNameBefore(String s) {
        return s + "_before";
    }

    private String getColNameAfter(String s) {
        return s + "_after";
    }

    public static  void main(String args[]){
        OperationHandler oh = new OperationHandler();
        Map<String,String> rowMap = Maps.newHashMap();

        HandlerProperties hp = new HandlerProperties();
        hp.setOpTypeFieldName("optype");
        hp.setReadTimeFieldName("readtime");

        Map<String,Map<String,String>> tableKeyMap = new HashMap<String, Map<String, String>>();
        Map<String,String> map = new HashMap<String, String>();
        map.put("columna", "NUMBER");
        tableKeyMap.put("tableA_ORGIN", map);
        hp.setTableKeysMap(tableKeyMap);

        Map<String,Map<String,String>> tableFocusMap = new HashMap<String, Map<String, String>>();
        Map<String,String> map2 = new HashMap<String, String>();
        map2.put("columnb", "CLOB");
        map2.put("columnc", "STRING");
        tableFocusMap.put("tableA_ORGIN", map2);
        hp.setTableFocusMap(tableFocusMap);

        oh.handlerProperties = hp;


        rowMap.put("columna_before",null);
        rowMap.put("columna_after", "1");
        rowMap.put("columnb_before", "B");
        rowMap.put("columnb_after", "b");
        rowMap.put("columnc_before", "C");
        rowMap.put("columnc_after", "c");
        rowMap.put("optype", "D");
        rowMap.put("readtime", "12345");
      // System.out.println(oh.getInsertSql("tableA", rowMap, "tableA_ORGIN"));

        Map<String,String> keyMap = Maps.newHashMap();
        keyMap.put("columna", "a");
       // keyMap.put("columnb", "b");
        System.out.println(oh.initDel("tableA", keyMap, "history"));
        System.out.println(oh.initDel("tableA", keyMap, "final"));
        System.out.println(oh.initSel("tableA", keyMap, "history"));
        System.out.println(oh.initSel("tableA", keyMap, "final"));

        System.out.println(oh.initUpd("tableA", keyMap, rowMap, "history"));
        System.out.println(oh.initUpd2("tableA", keyMap, rowMap, "history"));
        System.out.println(oh.initUpd("tableA", keyMap, rowMap, "final"));

        Map<String,String> rowMap2 = new HashMap<String, String>();
        rowMap2.put("columna_before",null);
        rowMap2.put("columna_after", "1");
        rowMap2.put("columnc_before", "C");
        rowMap2.put("columnc_after", "c");
        rowMap2.put("optype", "D");
        rowMap2.put("readtime", "12345");
       System.out.println(oh.getBigDataSql("columnb_after", rowMap2, "tableA","tableA_ORGIN"));
    }
}
