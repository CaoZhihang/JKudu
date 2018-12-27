package com.kevin.jkudu.global;

import com.kevin.jkudu.model.KuduConfig;
import com.kevin.jkudu.utils.StringUtils;

public class KuduConfigGlobal {
	
	/**
	 * kudu所在地址
	 */
	private static String url;
	
	/**
	 * 默认数据库
	 */
	private static String defaultDb = "";
	
	/**
	 * 默认数据库前缀
	 */
	private static String preFix = "";
	
	/**
	 * 默认批量提交数
	 */
	private static int flushCount = 5000;
	
	/**
	 * 默认admin操作超时时间
	 */
	private static int adminTimeout = 180000;
	
	/**
	 * 默认session,scanner超时时间
	 */
	private static int operationTimeout = 180000;
	
	/**
	 * 默认增删改失败后重复提交次数
	 */
	private static int retryCount = -1;
	
	/**
	 * 默认socket等待时间
	 */
	private static int socketTimeout = 18000;

	public static synchronized String getUrl() {
		return url;
	}

	public static synchronized String getDefaultDb() {
		return defaultDb;
	}

	public static synchronized String getPreFix() {
		return preFix;
	}

	public static synchronized int getFlushCount() {
		return flushCount;
	}

	public static synchronized int getAdminTimeout() {
		return adminTimeout;
	}

	public static synchronized int getOperationTimeout() {
		return operationTimeout;
	}

	public static synchronized int getSocketTimeout() {
		return socketTimeout;
	}
	
	public static synchronized int getRetryCount() {
		return retryCount;
	}
	
	public static synchronized void setProperties(KuduConfig config) {
		if (null == config)
			return;
		
		if (!StringUtils.isEmpty(config.getUrl()))
			url=config.getUrl();
		if (!StringUtils.isEmpty(config.getDefaultDb()))
			defaultDb=config.getDefaultDb();
		if (!StringUtils.isEmpty(config.getPreFix()))
			preFix=config.getPreFix();
		if (config.getFlushCount()>-1)
			flushCount = config.getFlushCount();
		if (config.getAdminTimeout()>-1)
			adminTimeout = config.getAdminTimeout();
		if (config.getOperationTimeout()>-1)
			operationTimeout = config.getOperationTimeout();
		if (config.getSocketTimeout()>-1)
			socketTimeout = config.getSocketTimeout();
		if (config.getRetryCount()>-2)
			retryCount = config.getRetryCount();
		
	}
	
}
