package com.kevin.jkudu.manager;

import com.kevin.jkudu.manager.impl.DefaultKuduClientManager;

public class KuduClientManagerFactory {

	public static KuduClientManager getDefaultManager(){
		return DefaultKuduClientManager.getInstance();
	}
	
}
