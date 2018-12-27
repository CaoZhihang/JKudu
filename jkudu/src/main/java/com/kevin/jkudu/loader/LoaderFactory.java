package com.kevin.jkudu.loader;

import com.kevin.jkudu.loader.impl.StringLoader;

public class LoaderFactory {

	public static Loader getStringLoader(){
		return new StringLoader();
	}
	
}
