package com.xkcoding.batch.service;

import java.util.Map;

/**
 * @Author: liuqiang
 * @Version: 1.0.0
 * @Date: 2026/03/28 15:27
 * @Description: 百万级数据高效批量插入服务接口
 *               对比三种方案：LOAD DATA LOCAL INFILE / PreparedStatement / Statement.executeBatch
 *
 * 性能参考（100 万条，i7-8700K / MySQL 8.0）：
 *   Statement.executeBatch : ~112 s
 *   PreparedStatement      : ~68  s
 *   LOAD DATA LOCAL INFILE : ~9   s（推荐百万级使用）
 */
public interface LoadDataBatchService {

    /**
     * 使用 LOAD DATA LOCAL INFILE 高效批量插入（推荐百万级数据）
     *
     * @param count     总插入条数
     * @param chunkSize 分块大小（建议 10 万条/块，根据内存调整）
     * @return 包含方法名、总条数、影响行数、耗时(ms/s) 的结果 Map
     */
    Map<String, Object> batchInsertWithLoadData(int count, int chunkSize);

    /**
     * 使用 PreparedStatement 批量插入（适合 10~50 万级数据）
     *
     * @param count     总插入条数
     * @param batchSize 每批提交大小（建议 1000 条/批）
     * @return 包含方法名、总条数、影响行数、耗时(ms/s) 的结果 Map
     */
    Map<String, Object> batchInsertWithPreparedStatement(int count, int batchSize);

    /**
     * 使用 Statement.executeBatch 批量插入（仅适合 10 万以下数据，有 SQL 注入风险）
     *
     * @param count     总插入条数
     * @param batchSize 每批提交大小（建议 1000 条/批）
     * @return 包含方法名、总条数、影响行数、耗时(ms/s) 的结果 Map
     */
    Map<String, Object> batchInsertWithStatement(int count, int batchSize);
}
