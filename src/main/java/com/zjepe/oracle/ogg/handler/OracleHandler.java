package com.zjepe.oracle.ogg.handler;

import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.google.common.collect.Maps;
import com.zjepe.oracle.ogg.handler.alarm.LogAlarm;
import com.zjepe.oracle.ogg.handler.alarm.OggAlarm;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by WangChao on 2016/8/10.
 */
public class OracleHandler extends AbstractHandler{
    private final static Logger logger = LoggerFactory.getLogger(OracleHandler.class);
    private OggAlarm oggAlarm;
    private HandlerProperties handlerProperties;
    private String alarmImplement = LogAlarm.class.getName();

    private String driverName;
    private String url;
    private String userName;
    private String password;
    private String tableMap;
    private String keyFields;
    private String focusFields;
    private String targetTableType;
    private String targetDealSituation;
    private int retryCount;


    @Override
    public void init(DsConfiguration dsConf, DsMetaData dsMeta){
        super.init(dsConf, dsMeta);

        handlerProperties = new HandlerProperties();
        getJdbcConnector(driverName,url,userName,password);
        initProperty();
    }

    @Override
    public Status metaDataChanged(DsEvent e, DsMetaData meta) {
        return super.metaDataChanged(e, meta);
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction tx) {
        return super.transactionBegin(e, tx);
    }

    @Override
    public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation dsOperation) {
        Status status = Status.OK;
        super.operationAdded(e, tx, dsOperation);

        Op op = new Op(dsOperation, getMetaData().getTableMetaData(dsOperation.getTableName()), getConfig());
        OperationHandler operationHandler = new OperationHandler();
        try {
            operationHandler.process(op, handlerProperties);
        }catch (Exception ex){
            ex.printStackTrace();
            status = Status.ABEND;
            oggAlarm.error("Failed after retry", ex);
            logger.error("Failed after retry", ex);
        }
        return status;
    }

    @Override
    public Status transactionCommit(DsEvent e, DsTransaction tx) {
        Status status = super.transactionCommit(e, tx);
        handlerProperties.totalTxns++;
        return status;
    }

    @Override
    public String reportStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(":- Status report: mode=").append(getMode());
        sb.append(", transactions=").append(handlerProperties.totalTxns);
        sb.append(", operations=").append(handlerProperties.totalOperations);
        sb.append(", inserts=").append(handlerProperties.totalInserts);
        sb.append(", updates=").append(handlerProperties.totalUpdates);
        sb.append(", deletes=").append(handlerProperties.totalDeletes);

        return sb.toString();
    }

    @Override
    public void destroy() {
        oggAlarm.warn("Handler destroying...");
        super.destroy();
    }

    private void getJdbcConnector(String driverName,String url,String userName,String password){
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("驱动类找不到  ",e);
        }
        try {
            Connection connection = DriverManager.getConnection(url, userName, password);
            handlerProperties.setConnection(connection);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("数据库连接错误  ",e);
        }
    }

    private void  initProperty(){
        handlerProperties.setOggOracleTableMap((buildMap(tableMap)));
        handlerProperties.setTableKeysMap(buildStringSetMap(keyFields));
        handlerProperties.setTableFocusMap(buildStringSetMap(focusFields));
        handlerProperties.setTargetTableTypeMap(buildMap(targetTableType));
        handlerProperties.setTargetDealSituationMap(buildMap(targetDealSituation));

        // Set custom alarm...
        try {
            oggAlarm = (OggAlarm) Class.forName(alarmImplement).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Cannot initialize custom OGG alarm. ", e);
        }

        // Other properties
        handlerProperties.setRetryCount(retryCount);
        handlerProperties.setOpTypeFieldName("optype");
        handlerProperties.setReadTimeFieldName("readtime");
    }

    private Map<String, String> buildMap(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        // "key1/value1,key2/value2,..."
        // all to lower case
        Map<String, String> map = Maps.newHashMap();
        String[] array = str.split(",");
        for(String s: array) {
            String[] pair = s.split("/");
            map.put(pair[0].trim().toLowerCase(), pair[1].trim().toLowerCase());
        }
        return map;
    }

    private Map<String, Set<String>> buildStringSetMap(String str) {
        Map<String, Set<String>> map = Maps.newHashMap();
        if (StringUtils.isEmpty(str)) {
            return map;
        }
        // "table1:name1,name2|table2:name3|...
        // all to lower case
        String[] tableInfos = str.split("\\|");
        for (String tableInfo: tableInfos) {
            String[] nameList = tableInfo.split(":");
            String name = nameList[0].trim().toLowerCase();
            Set<String> valSet = new HashSet<String>();
            for (String s: nameList[1].split(",")) {
                valSet.add(s.trim().toLowerCase());
            }
            map.put(name, valSet);
        }
        return map;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableMap() {
        return tableMap;
    }

    public void setTableMap(String tableMap) {
        this.tableMap = tableMap;
    }

    public String getKeyFields() {
        return keyFields;
    }

    public void setKeyFields(String keyFields) {
        this.keyFields = keyFields;
    }

    public String getFocusFields() {
        return focusFields;
    }

    public void setFocusFields(String focusFields) {
        this.focusFields = focusFields;
    }

    public String getTargetTableType() {
        return targetTableType;
    }

    public void setTargetTableType(String targetTableType) {
        this.targetTableType = targetTableType;
    }

    public String getTargetDealSituation() {
        return targetDealSituation;
    }

    public void setTargetDealSituation(String targetDealSituation) {
        this.targetDealSituation = targetDealSituation;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public static void main(String args[]){
        OracleHandler oh = new OracleHandler();
        System.out.println(oh.buildStringSetMap("test_1:Columna,columnb|test_2:columna"));
    }
}
