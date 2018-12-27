package com.kevin.jkudu;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.kevin.jkudu.loader.LoaderFactory;
import com.kevin.jkudu.manager.KuduClientManagerFactory;
import com.kevin.jkudu.manager.KuduExcuteCall;

public class Jkudu {

	public static void loadConfig(Object source){
		
		if (null == source)
			return;
		
		try {
			
			if (source instanceof String){
				if (!new File((String)source).isFile()){					
					LoaderFactory.getStringLoader().load(source);
				}
			}
			
		} catch(Exception e){
			System.out.println(e);
		}
		
	}
	
	public static Map<String,Object> excute(String tableName, List<JSONObject> datas, KuduExcuteCall executeCall){
		return KuduClientManagerFactory.getDefaultManager().excute(tableName, datas, executeCall);
	}
	
}
