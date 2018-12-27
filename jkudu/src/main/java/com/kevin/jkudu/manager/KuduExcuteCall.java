package com.kevin.jkudu.manager;

import java.util.List;
import java.util.Map;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import com.alibaba.fastjson.JSONObject;

/**
 * kudu具体执行逻辑接口
 * @author zhicao1
 *
 */
public interface KuduExcuteCall {

	/**
	 * 执行具体kudu逻辑
	 * @param client　kudu客户端
	 * @param s　session对象
	 * @param table table对象
	 * @param datas　数据
	 * @throws Exception
	 */
	public Map<String,Object> excute(KuduClient client, KuduSession s, KuduTable table, List<JSONObject> datas) throws Exception;
	
}
