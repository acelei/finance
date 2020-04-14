package com.cheche365.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * author:WangZhaoliang
 * Date:2020/4/13 14:42
 */
public class CommonUtils {

    public static String getSqlFormat(Object obj){
        if(obj == null || StringUtils.isEmpty(obj.toString())) {
            return null;
        }
        return "'" + obj.toString() + "'";
    }

    public static String getMapStr(Map<String, Object> map, String key) {
        if (map.get(key) == null || StringUtils.isEmpty(map.get(key).toString())) {
            return null;
        } else {
            return "'" + map.get(key).toString() + "'";
        }
    }

    /**
     * 判断Map里面是否包含某个key
     * @param map
     * @param key
     * @return
     */
    public static boolean isExistFromMap(Map<String, Object> map, String key) {
        assert map != null && map.size() > 0;
        boolean exist = false;
        if (map.get(key) != null && StringUtils.isNotEmpty(map.get(key).toString())) {
            exist = true;
        }
        return exist;
    }

}
