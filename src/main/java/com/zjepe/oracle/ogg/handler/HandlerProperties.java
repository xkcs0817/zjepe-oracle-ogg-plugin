package com.zjepe.oracle.ogg.handler;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

/**
 * Created by WangChao on 2016/8/10.
 */
public class HandlerProperties {
    private Connection connection;
    private Map<String,String> oggOracleTableMap;
    private Map<String, Set<String>> tableKeysMap;
    private Map<String, Set<String>> tableFocusMap;
    private Map<String,String> targetTableTypeMap;
    private Map<String,String> targetDealSituationMap;
    private int retryCount;
    private String opTypeFieldName;
    private String readTimeFieldName;
    public Long totalInserts = 0L;
    public Long totalUpdates = 0L;
    public Long totalDeletes =  0L;
    public Long totalTxns = 0L;
    public Long totalOperations = 0L;

    public String getReadTimeFieldName() {
        return readTimeFieldName;
    }

    public void setReadTimeFieldName(String readTimeFieldName) {
        this.readTimeFieldName = readTimeFieldName;
    }

    public String getOpTypeFieldName() {
        return opTypeFieldName;
    }

    public void setOpTypeFieldName(String opTypeFieldName) {
        this.opTypeFieldName = opTypeFieldName;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public Map<String, String> getTargetTableTypeMap() {
        return targetTableTypeMap;
    }

    public Map<String, String> getTargetDealSituationMap() {
        return targetDealSituationMap;
    }

    public void setTargetDealSituationMap(Map<String, String> targetDealSituationMap) {
        this.targetDealSituationMap = targetDealSituationMap;
    }

    public void setTargetTableTypeMap(Map<String, String> targetTableTypeMap) {

        this.targetTableTypeMap = targetTableTypeMap;
    }

    public Map<String, Set<String>> getTableKeysMap() {
        return tableKeysMap;
    }

    public void setTableKeysMap(Map<String, Set<String>> tableKeysMap) {
        this.tableKeysMap = tableKeysMap;
    }

    public Map<String, Set<String>> getTableFocusMap() {
        return tableFocusMap;
    }

    public void setTableFocusMap(Map<String, Set<String>> tableFocusMap) {
        this.tableFocusMap = tableFocusMap;
    }

    public Connection getConnection() {
        return connection;
    }
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Map<String, String> getOggOracleTableMap() {
        return oggOracleTableMap;
    }

    public void setOggOracleTableMap(Map<String, String> oggOracleTableMap) {
        this.oggOracleTableMap = oggOracleTableMap;
    }
}
