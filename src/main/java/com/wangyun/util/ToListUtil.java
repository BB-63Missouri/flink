package com.wangyun.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/20 18:50
 */
public class ToListUtil {
    public static <T> List<T> toList(Iterable<T> it){
        ArrayList<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
        }
        return list;
    }
}
