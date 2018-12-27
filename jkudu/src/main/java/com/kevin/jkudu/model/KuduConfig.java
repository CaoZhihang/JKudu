package com.kevin.jkudu.model;

/**
 * kudu配置信息类
 * @author zhcao
 *
 */
public class KuduConfig {

	/**
	 * kudu所在地址
	 */
	private String url;
	
	/**
	 * 默认数据库
	 */
	private String defaultDb;
	
	/**
	 * 默认数据库前缀
	 */
	private String preFix;
	
	/**
	 * 默认批量提交数
	 */
	private int flushCount = -1;
	
	/**
	 * 默认admin操作超时时间
	 */
	private int adminTimeout = -1;
	
	/**
	 * 默认session,scanner超时时间
	 */
	private int operationTimeout = -1;
	
	/**
	 * 默认socket等待时间
	 */
	private int socketTimeout = -1;
	
	/**
	 * 默认增删改失败后重复提交次数
	 */
	private int retryCount = -2;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDefaultDb() {
		return defaultDb;
	}

	public void setDefaultDb(String defaultDb) {
		this.defaultDb = defaultDb;
	}

	public String getPreFix() {
		return preFix;
	}

	public void setPreFix(String preFix) {
		this.preFix = preFix;
	}

	public int getFlushCount() {
		return flushCount;
	}

	public void setFlushCount(int flushCount) {
		this.flushCount = flushCount;
	}

	public int getAdminTimeout() {
		return adminTimeout;
	}

	public void setAdminTimeout(int adminTimeout) {
		this.adminTimeout = adminTimeout;
	}

	public int getOperationTimeout() {
		return operationTimeout;
	}

	public void setOperationTimeout(int operationTimeout) {
		this.operationTimeout = operationTimeout;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

}
