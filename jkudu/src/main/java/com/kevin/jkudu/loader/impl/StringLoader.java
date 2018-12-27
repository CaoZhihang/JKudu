package com.kevin.jkudu.loader.impl;

import com.kevin.jkudu.global.KuduConfigGlobal;
import com.kevin.jkudu.loader.Loader;
import com.kevin.jkudu.model.KuduConfig;

public class StringLoader implements Loader {

	public void load(Object source) throws Exception {
		String url = (String) source;
		KuduConfig config = new KuduConfig();
		config.setUrl(url);
		KuduConfigGlobal.setProperties(config);
	}

}
