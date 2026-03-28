package com.xkcoding.batch.controller;

import com.xkcoding.batch.service.LoadDataBatchService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * @Author: liuqiang
 * @Version: 1.0.0
 * @Date: 2026/03/28 15:27
 * @Description: 批量插入性能对比接口
 *
 * 前置 DDL（执行 src/main/resources/db/init.sql）：
 *   CREATE TABLE test_batch_data (...) ENGINE=InnoDB;
 *
 * 前置 MySQL 配置：
 *   SET GLOBAL local_infile = 1;
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/batch")
public class BatchDataController {

    private final LoadDataBatchService loadDataBatchService;

    /**
     * LOAD DATA LOCAL INFILE 高效批量插入（百万级首选，约 9s/百万条）
     *
     * 前置要求：
     *   - MySQL URL 添加 allowLoadLocalInfile=true
     *   - 执行 SET GLOBAL local_infile=1
     *
     * 示例：POST /batch/loadData?count=1000000&chunkSize=100000
     */
    @PostMapping("/loadData")
    public ResponseEntity<Map<String, Object>> batchInsertLoadData(
            @RequestParam(defaultValue = "1000000") int count,
            @RequestParam(defaultValue = "100000")  int chunkSize) {
        return ResponseEntity.ok(loadDataBatchService.batchInsertWithLoadData(count, chunkSize));
    }

    /**
     * PreparedStatement 批量插入（10~50 万级，约 68s/百万条）
     *
     * 示例：POST /batch/preparedStatement?count=100000&batchSize=1000
     */
    @PostMapping("/preparedStatement")
    public ResponseEntity<Map<String, Object>> batchInsertPreparedStatement(
            @RequestParam(defaultValue = "100000") int count,
            @RequestParam(defaultValue = "1000")   int batchSize) {
        return ResponseEntity.ok(loadDataBatchService.batchInsertWithPreparedStatement(count, batchSize));
    }

    /**
     * Statement.executeBatch 批量插入（10 万以下，约 112s/百万条，不推荐生产使用）
     *
     * 注意：存在 SQL 注入风险，仅供对比测试
     *
     * 示例：POST /batch/statement?count=10000&batchSize=1000
     */
    @PostMapping("/statement")
    public ResponseEntity<Map<String, Object>> batchInsertStatement(
            @RequestParam(defaultValue = "10000") int count,
            @RequestParam(defaultValue = "1000")  int batchSize) {
        return ResponseEntity.ok(loadDataBatchService.batchInsertWithStatement(count, batchSize));
    }
}
