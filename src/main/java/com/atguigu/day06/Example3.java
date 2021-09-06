package com.atguigu.day06;

import java.util.HashMap;

// 每次可以爬一个台阶或者两个台阶
// n个台阶共几种方法爬
// 斐波那契数列 f(n) = f(n-1) + f(n-2)
public class Example3 {
    public static void main(String[] args) {
//        System.out.println(climbStairs(50));
//        System.out.println(climbStairsImproved(50));
        System.out.println(climbStairsWithCache(50));
    }

    // f(50) = f(49) + f(48) = 2 * f(48) + f(47) = 3 * f(47) + 2 * f(46) = 5 * f(46) + 3 * f(45) = 8 * f(45) + 5 * f(44)
    // 指数级别的时间复杂度
    public static Long climbStairs(int n) {
        if (n == 1) return 1L;
        else if (n == 2) return 2L;
        else return climbStairs(n - 1) + climbStairs(n - 2);
    }

    // 线性时间复杂度
    public static Long climbStairsImproved(int n) {
        // 开辟一个长度为n+1的数组，索引i对应的数组元素是爬i个台阶所需的方法数
        if (n == 1) return 1L;
        else if (n == 2) return 2L;
        else {
            Long[] array = new Long[n + 1];
            array[1] = 1L;
            array[2] = 2L;
            for (int i = 3; i < n + 1; i++) {
                array[i] = array[i - 1] + array[i - 2];
            }
            return array[n];
        }
    }

    // cache = {}
    //def fib(n):
    //    if n not in cache.keys():
    //        cache[n] = _fib(n)
    //    return cache[n]
    //
    //def _fib(n):
    //    if n == 1 or n == 2:
    //        return n
    //    else:
    //        return fib(n-1) + fib(n-2)
    // key: 台阶数量
    // value： 方法数量
    private static HashMap<Integer, Long> cache = new HashMap<>();

    public static Long climbStairsWithCache(int n) {
        if (!cache.containsKey(n)) {
            cache.put(n, help(n));
        }
        return cache.get(n);
    }

    public static Long help(int n) {
        if (n == 1) return 1L;
        else if (n == 2) return 2L;
        else return climbStairsWithCache(n - 1) + climbStairsWithCache(n - 2);
    }

    // 如果缓存中有需要的数据，则直接返回
    // 如果缓存中没有需要的数据，则将硬盘上的所需数据放入缓存中，并返回
}
