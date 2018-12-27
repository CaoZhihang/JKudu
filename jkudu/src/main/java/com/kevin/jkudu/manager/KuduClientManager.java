package com.kevin.jkudu.manager;

import java.util.List;
import java.util.Map;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

import com.alibaba.fastjson.JSONObject;

/**
 * kudu客户端管理类
 * @author zhcao1
 *
 */
public interface KuduClientManager {

	/**
	 * kudu客户端执行具体逻辑
	 * @param tableName 目标表名
	 * @param datas 数据
	 * @param executeCall 具体执行逻辑调用类
	 */
	public Map<String,Object> excute(String tableName, List<JSONObject> datas, KuduExcuteCall executeCall);
	
	/**
	 * 根据查询的单条数据，生成对应row对象
	 * @param table　表对象
	 * @param row　要生成的PartialRow对象
	 * @param data　查询到的数据信息
	 */
	public void setRow(KuduTable table, PartialRow row, RowResult data);
	
	/**
	 * 根据查询的单条数据，生成对应row对象
	 * @param table 表对象
	 * @param row 要生成的PartialRow对象
	 * @param data 查询到的数据信息
	 */
	public void setRow(KuduTable table, PartialRow row, JSONObject data);
	
	/**
	 * 获取flush提交的最大数量
	 * @return
	 */
	public int getFlushCount();
	
	/**
	 * 对执行结果集判断
	 * @param responseList
	 * @return　true存在异常，false不存在异常
	 */
	public boolean checkResult(List<OperationResponse> responseList);
	
	/**
	 * 使用配置的kudu库名，结合成表名
	 * @param tableName
	 * @return
	 */
	public String useDefaultKuduDb(String tableName);
}
