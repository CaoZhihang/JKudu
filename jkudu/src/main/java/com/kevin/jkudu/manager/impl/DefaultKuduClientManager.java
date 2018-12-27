package com.kevin.jkudu.manager.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.ListTablesResponse;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Status;

import com.alibaba.fastjson.JSONObject;
import com.kevin.jkudu.global.KuduConfigGlobal;
import com.kevin.jkudu.manager.KuduClientManager;
import com.kevin.jkudu.manager.KuduExcuteCall;
import com.kevin.jkudu.utils.StringUtils;

/**
 * 默认kudu客户端管理类
 * @author zhcao1
 *
 */
public class DefaultKuduClientManager implements KuduClientManager {

	private static int FLUSH_COUNT = 5000;
	private KuduClient client = null;
	private String url;
	private String kuduDb;
	private String preFix;
	private int maxReTryCount = -1;

	private DefaultKuduClientManager() {
		init();
	}

	private void init() {
		url = KuduConfigGlobal.getUrl();
		FLUSH_COUNT = KuduConfigGlobal.getFlushCount();
		client = new KuduClient.KuduClientBuilder(url).defaultAdminOperationTimeoutMs(180000).defaultOperationTimeoutMs(180000).defaultSocketReadTimeoutMs(18000).build();
		preFix = KuduConfigGlobal.getPreFix();
		kuduDb = KuduConfigGlobal.getDefaultDb();//ARE.getProperty("KuduDb","impala::fin_operation");
		maxReTryCount = KuduConfigGlobal.getRetryCount();
		if (!kuduDb.startsWith(preFix)){
			kuduDb = preFix+kuduDb;
		}
		if (kuduDb.endsWith(".")){
			kuduDb = kuduDb.substring(0, kuduDb.length()-1);
		}
	}

	public void setRow(KuduTable table, PartialRow row, JSONObject data){
		if (null != table && null != row){
			
			Schema schema = table.getSchema();
			List<ColumnSchema> cols = schema.getColumns();
			for (ColumnSchema col : cols) {
				Type type = col.getType();
				String colName = col.getName();
				
				if (data.containsKey(colName)){
					
					if (null == data.get(colName)){
						row.setNull(colName);
					} else{
						if (Type.INT16.getName().equals(type.getName())){
							row.addShort(colName, data.getShort(colName));
						} else if(Type.INT32.getName().equals(type.getName())){
							row.addInt(colName, data.getInteger(colName));
						} else if(Type.INT64.getName().equals(type.getName())){
							row.addLong(colName, data.getLong(colName));
						} else if(Type.DOUBLE.getName().equals(type.getName())){
							row.addDouble(colName, data.getDouble(colName));
						} else if(Type.STRING.getName().equals(type.getName())){
							row.addString(colName, data.getString(colName));
						} else if(Type.BOOL.getName().equals(type.getName())){
							row.addBoolean(colName, data.getBoolean(colName));
						}
					}
					
				}
			}
			
		}
	}
	
	public void setRow(KuduTable table, PartialRow row, RowResult data){
		if (null != table && null != row){
			
			Schema schema = table.getSchema();
			List<ColumnSchema> cols = schema.getColumns();
			if (null != cols){
				for (ColumnSchema col : cols) {
					Type type = col.getType();
					String colName = col.getName();
					
					if (data.isNull(colName)){
						row.setNull(colName);
					} else {
						if (Type.INT16.getName().equals(type.getName())){
							row.addShort(colName, data.getShort(colName));
						} else if(Type.INT32.getName().equals(type.getName())){
							row.addInt(colName, data.getInt(colName));
						} else if(Type.INT64.getName().equals(type.getName())){
							row.addLong(colName, data.getLong(colName));
						} else if(Type.DOUBLE.getName().equals(type.getName())){
							row.addDouble(colName, data.getDouble(colName));
						} else if(Type.STRING.getName().equals(type.getName())){
							row.addString(colName, data.getString(colName));
						} else if(Type.BOOL.getName().equals(type.getName())){
							row.addBoolean(colName, data.getBoolean(colName));
						}
					}
				}
			}
			
		}
	}
	
	public void dropTable(String tableName) {
		try {
			if (null != tableName && client.tableExists(tableName)){
				client.deleteTable(tableName);
			}
			
		} catch(Exception e) {
			System.out.println(e);
		}
		
	}
	
	public void testSelect() {

		List<String> projectColumns = new ArrayList<String>();
		projectColumns.add("id");
		projectColumns.add("name");
		try {
			KuduTable table = client.openTable("impala::default.test4");
			Schema sche = table.getSchema();
			PartialRow lower = sche.newPartialRow();
			lower.addInt("id", 2);
			lower.addInt("id", 1);
			PartialRow upper = sche.newPartialRow();
			upper.addInt("id", 3);
			
			KuduScanner scanner = client.newScannerBuilder(table).lowerBound(lower).exclusiveUpperBound(upper).build();
			while (scanner.hasMoreRows()) {
				RowResultIterator results = scanner.nextRows();
				while (results.hasNext()) {
					RowResult result = results.next();
					long id = result.getInt("id");
					String name = result.getString("name");
					System.out.println("id="+id+",name="+name);
				}
			}
			System.out.println("finish");
			
		} catch (KuduException e) {
			e.printStackTrace();
		}

	}

	public void showTableCols(String tableName){
		if (null != client && null != tableName){
			try {
				KuduTable table = client.openTable(tableName);
				Schema schema = table.getSchema();
				List<ColumnSchema> cols = schema.getColumns();
				for (ColumnSchema col : cols){
					System.out.println(col.getName()+","+col.getType().getName());
				}
				
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public void showTables() {
		if (null != client) {

			try {
				ListTablesResponse tableList = client.getTablesList();
				List<String> tables = tableList.getTablesList();
				for (String table : tables) {
					System.out.println(table);
				}
			} catch (KuduException e) {
				e.printStackTrace();
			}

		}

	}

	public Map<String,Object> excute(String tableName, List<JSONObject> datas, KuduExcuteCall executeCall) {
		KuduSession s = null;
		try {
			if (null == tableName || !client.tableExists(tableName) || null == datas) {
				System.out.println("kudu执行参数不符合，tableName==null?"+(null==tableName)+"，exist table?"+client.tableExists(tableName)+"，datas==null?"+(null==datas));
				return null;
			}

			tableName = this.useDefaultKuduDb(tableName);
			s = client.newSession();
			// 使用批量提交模式
			SessionConfiguration.FlushMode mode = SessionConfiguration.FlushMode.MANUAL_FLUSH;
			s.setFlushMode(mode);
			s.setMutationBufferSpace(FLUSH_COUNT);
			KuduTable table = client.openTable(tableName);
			
			boolean maxRetry = true;
			if (maxReTryCount>-1){
				maxRetry = false;
			}
			boolean isSuccess = false;
			int tryCount = 0;

			while (!isSuccess){	
				try {
						if (tryCount>0){
							System.out.println("第"+tryCount+"次重拾执行kudu提交");
						}
						isSuccess = true;
						return executeCall.excute(client, s, table, datas);
				} catch (Exception e){
					System.out.println(e);
					tryCount++;
					if (maxRetry || tryCount<=tryCount){						
						isSuccess = false;
						Thread.sleep(3000);
					} else{
						isSuccess = true;
					}
				}
			}

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			if (null != s) {
				try {
					s.close();
				} catch (KuduException e) {
					System.out.println(e);
				}
			}
		}
		
		return null;
	}
	
	public boolean checkResult(List<OperationResponse> responseList){
		if (null == responseList)
			return false;
		boolean existFallFlag = false;
		for (OperationResponse operationResponse : responseList) {
			if (operationResponse.hasRowError()) {
				RowError rowError = operationResponse.getRowError();
				Status status = rowError.getErrorStatus();
				if (status.isAlreadyPresent() || status.isNotFound()) {
					continue;
				}
				existFallFlag = true;
				System.out.println("errorstatus:"+rowError.getErrorStatus()+" , message:"+rowError.getMessage());
				break;
			}
		}
		return existFallFlag;
	}

	private static class inner {
		private static DefaultKuduClientManager instance = new DefaultKuduClientManager();
	}

	public static DefaultKuduClientManager getInstance() {
		return inner.instance;
	}

	public static void main(String[] args) {
		//ARE.init("/home/ytwang/workspace/SVNAsooSearchHadoop_V3.0/etc/are.xml");
		DefaultKuduClientManager manager = getInstance();
		//manager.dropTable("impala::default.test3");
//		manager.showTables();
//		manager.testSelect();
		System.out.println(manager.useDefaultKuduDb("impala::fin_operation.t_pira_dt_task_undis"));
		//manager.showTableCols("impala::fin_operation.t_pira_dt_task_undis");
	}

	public int getFlushCount() {
		return FLUSH_COUNT;
	}

	public String useDefaultKuduDb(String tableName) {
		if (StringUtils.isEmpty(tableName))
			return null;
		
		if (!StringUtils.isEmpty(preFix)){			
			if (tableName.startsWith(preFix)){
				tableName = tableName.substring(preFix.length());
			}
		}
		
		if (tableName.contains(".")){
			tableName = tableName.split("\\.")[1];
		}
		
		tableName = this.kuduDb+"."+tableName;
		return tableName;
	}

}
