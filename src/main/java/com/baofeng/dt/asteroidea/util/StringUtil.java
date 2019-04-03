package com.baofeng.dt.asteroidea.util;

import org.apache.commons.lang.StringUtils;

public class StringUtil {
	public static final String EMPTY_STRING="";
	public static String replaceSequenced( String originalStr, Object... replacementParams ) {

		if ( StringUtils.isBlank( originalStr ) )
			return EMPTY_STRING;
		if ( null == replacementParams || 0 == replacementParams.length )
			return originalStr;

		for ( int i = 0; i < replacementParams.length; i++ ) {
			String elementOfParams = replacementParams[i] + EMPTY_STRING;
			if ( StringUtil.trimToEmpty( elementOfParams ).equalsIgnoreCase( "null" ) )
				elementOfParams = EMPTY_STRING;
			originalStr = originalStr.replace( "{" + i + "}", StringUtil.trimToEmpty( elementOfParams ) );
		}

		return originalStr;
	}
	public static String trimToEmpty( String originalStr ) {
		if ( null == originalStr || originalStr.isEmpty() )
			return EMPTY_STRING;

		return originalStr.trim();
	}
}
