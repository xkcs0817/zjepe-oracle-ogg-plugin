package com.zjepe.oracle.ogg.handler;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void process(Op op, HandlerProperties handlerProperties){
        this.handlerProperties = handlerProperties;

        List<Map<String, String>> rowList = new ArrayList<Map<String, String>>();
        String opType = transformOpType(op, handlerProperties);
        //处理非主键更新外的所有逻辑，即新增、更新、删除
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
                doInsert(oracleTableName, rowList);
            //处理目标端保留最终版本的逻辑，先删除后插入
            else if (handlerProperties.getTargetDealSituationMap().get(fullTableName).equals("final")){
                doDelAndInsert(oracleTableName, rowList,op);
            }
        } else if(rowList.size() == 1){
            //处理目标端保留历史情况的逻辑
            if(handlerProperties.getTargetDealSituationMap().get(fullTableName).equals("history"))
                doInsert(oracleTableName,rowList);
            //处理目标端保留最终版本的逻辑
            else if(handlerProperties.getTargetDealSituationMap().get(fullTableName).equals("final"))
                doUpdate(oracleTableName,rowList,op);
        }
    }

    private void doDelAndInsert(String oracleTableName, List<Map<String, String>> rowList, Op op) {
        List<String> sqlList  = new ArrayList<String>();
        String delSql = getDelSql(oracleTableName, rowList.get(0), op);
        sqlList.add(delSql);
        for (Map<String, String> rowMap : rowList){
            String insSql = getInsertSql(oracleTableName, rowMap);
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

    private void doInsert(String oracleTableName, List<Map<String, String>> rowList) {
        List<String> sqlList = new ArrayList<String>();
        for (Map<String, String> rowMap : rowList){
            String sql = getInsertSql(oracleTableName, rowMap);
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
            //新增和更新的逻辑为先删后插，所以有两条sql
            if(!rowMap.get(handlerProperties.getOpTypeFieldName()).equals("D")){
                String delSql = initDel(oracleTableName,keyFieldValueMap,"history");
                String addSql = getInsertSql(oracleTableName, rowMap);
                sqlList.add(delSql);
                sqlList.add(addSql);
            }
            //删除的逻辑为先判断目标端是否存在该主键的记录：有记录则更新；无记录则直接插入。之所以不先删后插是因为ogg的删除数据只保留了主键的值，其他值都为null
            else{
                String selSql = initSel(oracleTableName, keyFieldValueMap, "history");
                if(checkIfExists(selSql) != 0){
                    String updSql = initUpd(oracleTableName, keyFieldValueMap, rowMap, "history");
                    String updSql2 = initUpd2(oracleTableName, keyFieldValueMap, rowMap, "history");
                    sqlList.add(updSql);
                    sqlList.add(updSql2);
                }else{
                    String addSql = getInsertSql(oracleTableName, rowMap);
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
                String addSql = getInsertSql(oracleTableName, rowMap);
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
                    String addSql = getInsertSql(oracleTableName, rowMap);
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
        Set<String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if (keyFields != null && keyFields.contains(colName)){
                list.add(colName);
            }
        }
        return list;
    }

    private String getInsertSql(String oracleTableName, Map<String, String> rowMap) {
        String colunmName = " ( ";
        String columnValue = " ( ";
        for(Map.Entry<String,String> entry : rowMap.entrySet()){
            colunmName += entry.getKey() + ",";
            columnValue += entry.getValue() == null ? "'',": "'"+ entry.getValue() + "',";
        }
        colunmName = colunmName.substring(0,colunmName.length()-1) + " ) ";
        columnValue = columnValue.substring(0,columnValue.length()-1) + " ) ";
        return "insert into " + oracleTableName + colunmName + "values" + columnValue;
    }

    private Map<String, String> getRowMap(Op op, String opType) {
        // Get data
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        Set<String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Set<String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();

        Map<String, String> rowMap = Maps.newHashMap();
        rowMap.put(handlerProperties.getOpTypeFieldName(), opType);
        rowMap.put(handlerProperties.getReadTimeFieldName(), op.getOperation().getReadTimeAsString());

        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if ((focusFields != null && focusFields.contains(colName)) || (keyFields != null && keyFields.contains(colName))) {
                //处理目标表结构为保留历史情况的逻辑
                if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("history")) {
                    rowMap.put(getColNameBefore(colName), cols.get(i).getBeforeValue());
                    rowMap.put(getColNameAfter(colName), cols.get(i).getAfterValue());
                }
                //处理目标表结构为保留最终版本的逻辑
                else if(handlerProperties.getTargetTableTypeMap().get(fullTableName).equals("final")){
                    String colValue = cols.get(i).getAfterValue();
                    if (StringUtils.isEmpty(colValue)) {
                        colValue = cols.get(i).getBeforeValue();
                    }
                    rowMap.put(colName, colValue);
                }
            }
        }
        return rowMap;
    }

    private Map<String, String> getRowMap4I(Op op) {
        // Get data
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        Set<String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Set<String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();

        Map<String, String> rowMap = Maps.newHashMap();
        rowMap.put(handlerProperties.getOpTypeFieldName(), "I");
        rowMap.put(handlerProperties.getReadTimeFieldName(), op.getOperation().getReadTimeAsString());
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if ((focusFields != null && focusFields.contains(colName)) || (keyFields != null && keyFields.contains(colName))) {
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
        Set<String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Set<String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();

        Map<String, String> rowMap = Maps.newHashMap();
        rowMap.put(handlerProperties.getOpTypeFieldName(), "D");
        rowMap.put(handlerProperties.getReadTimeFieldName(), op.getOperation().getReadTimeAsString());
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if ((focusFields != null && focusFields.contains(colName)) || (keyFields != null && keyFields.contains(colName))) {
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
            Set<String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
            // Key Cols and Focus Cols
            List<DsColumn> cols = op.getColumns();
            boolean flag = false;
            for (int i = 0; i < cols.size(); i++) {
                String colName = op.getTableMeta().getColumnName(i).toLowerCase();
                if (keyFields != null && keyFields.contains(colName)) {
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
        rowMap.put("columna_before",null);
        rowMap.put("columna_after", "a");
        rowMap.put("columnb_before", "B");
        rowMap.put("columnb_after", "b");
        rowMap.put("columnc_before", "C");
        rowMap.put("columnc_after", "c");
        rowMap.put("optype", "D");
        rowMap.put("readtime", "12345");
        System.out.println(oh.getInsertSql("tableA", rowMap));

        Map<String,String> keyMap = Maps.newHashMap();
        keyMap.put("columna", "a");
       // keyMap.put("columnb", "b");
        System.out.println(oh.initDel("tableA", keyMap, "history"));
        System.out.println(oh.initDel("tableA", keyMap, "final"));
        System.out.println(oh.initSel("tableA", keyMap, "history"));
        System.out.println(oh.initSel("tableA", keyMap, "final"));

        HandlerProperties hp = new HandlerProperties();
        hp.setOpTypeFieldName("optype");
        hp.setReadTimeFieldName("readtime");
        oh.handlerProperties = hp;
        System.out.println(oh.initUpd("tableA", keyMap, rowMap, "history"));
        System.out.println(oh.initUpd2("tableA", keyMap, rowMap, "history"));
        System.out.println(oh.initUpd("tableA", keyMap, rowMap, "final"));
    }
}
