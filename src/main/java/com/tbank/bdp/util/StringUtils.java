package com.tbank.bdp.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description:
 * @Author: leiline
 * @CreateTime: 2021-11-21
 */
public class StringUtils {

    public static String today() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date());
    }
}
