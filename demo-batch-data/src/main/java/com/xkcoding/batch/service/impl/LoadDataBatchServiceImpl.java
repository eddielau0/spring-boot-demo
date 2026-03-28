package com.xkcoding.batch.service.impl;

import com.xkcoding.batch.service.LoadDataBatchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author: liuqiang
 * @Version: 1.0.0
 * @Date: 2026/03/28 15:27
 * @Description: 百万级数据高效批量插入服务实现类（目标表：test_batch_data）
 *
 * ========================= 前置配置 =========================
 * 1. MySQL 直连 URL 需含：allowLoadLocalInfile=true（见 application.yml spring.datasource.url）
 * 2. MySQL 服务端需执行一次：SET GLOBAL local_infile = 1;
 * ============================================================
 *
 * 性能参考（100 万条，i7-8700K / MySQL 8.0）：
 *   Statement.executeBatch : ~112 s，内存峰值高，存在 SQL 注入风险
 *   PreparedStatement      : ~68  s，内存适中，安全性高
 *   LOAD DATA LOCAL INFILE : ~9   s，内存最低，性能最优（推荐百万级使用）
 */
@Slf4j
@Service
public class LoadDataBatchServiceImpl implements LoadDataBatchService {

    /** LOAD DATA 专用直连 DataSource */
    private final DataSource dataSource;

    public LoadDataBatchServiceImpl(@Qualifier("loadDataSource") DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private static final String TABLE_NAME = "test_batch_data";
    private static final String TEMP_DIR   = System.getProperty("java.io.tmpdir");

    // ── 参考数据基准值 ──────────────────────────────────────────────
    private static final long   BASE_ID             = 2037066600192077825L;
    private static final long   BASE_PROJECT_ID     = 2037066579712249857L;
    private static final long   PROJECT_SNAPSHOT_ID = 2037066579829690369L;
    private static final String BUSINESS_NO         = "ZX2026032644005";
    private static final int    VERSION             = 0;
    private static final long   TYPE_ID             = 1025L;
    private static final String TYPE_CODE           = "zxsqs_big_data_image";
    private static final long   ATTACHMENT_ID       = 2029120989150392321L;
    private static final String FIXED_DATETIME      = "2026-03-26 15:18:19";
    private static final int    CREATE_USER_ID      = 0;
    private static final int    UPDATE_USER_ID      = 0;
    private static final int    DELETED             = 0;

    // CSV 列顺序与 LOAD DATA 字段映射保持一致
    private static final String COLUMNS =
            "(id,project_id,project_snapshot_id,business_no,version,type_id,type_code," +
            "attachment_id,create_time,create_user_id,update_time,update_user_id,deleted)";

    // =========================================================
    // 方案三：LOAD DATA LOCAL INFILE（百万级首选，~9s/百万）
    // =========================================================

    @Override
    public Map<String, Object> batchInsertWithLoadData(int count, int chunkSize) {
        log.info("【LOAD DATA】开始批量插入，总条数：{}，分块大小：{}", count, chunkSize);
        long totalStart    = System.currentTimeMillis();
        int  totalAffected = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            int processed = 0;
            while (processed < count) {
                int    currentChunkSize = Math.min(chunkSize, count - processed);
                String filePath         = null;
                try {
                    filePath  = this.generateCsvFile(processed, currentChunkSize);
                    int affected = this.loadDataWithFile(conn, filePath);
                    totalAffected += affected;
                    conn.commit();
                    log.info("【LOAD DATA】分块 [{}-{}] 完成，影响行数：{}",
                            processed + 1, processed + currentChunkSize, affected);
                } catch (Exception e) {
                    log.error("【LOAD DATA】分块 [{}-{}] 处理失败，执行回滚",
                            processed + 1, processed + currentChunkSize, e);
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        log.error("【LOAD DATA】事务回滚失败", ex);
                    }
                    throw new RuntimeException("LOAD DATA 批量插入失败: " + e.getMessage(), e);
                } finally {
                    this.cleanupTempFile(filePath);
                }

                processed += currentChunkSize;
                System.gc();
            }
        } catch (SQLException e) {
            log.error("【LOAD DATA】获取数据库连接失败", e);
            throw new RuntimeException("获取数据库连接失败: " + e.getMessage(), e);
        }

        long elapsed = System.currentTimeMillis() - totalStart;
        log.info("【LOAD DATA】批量插入完成，总条数：{}，耗时：{}ms（{} s）",
                count, elapsed, String.format("%.2f", elapsed / 1000.0));
        return this.buildResult("LOAD DATA LOCAL INFILE", count, totalAffected, elapsed);
    }

    /**
     * 将一个分块的数据写入 CSV 临时文件
     * id / project_id 以 BASE 值为起点，按全局偏移量逐行 +1
     *
     * @param processedCount 已处理总条数（全局偏移量起点）
     * @param chunkSize      本块条数
     * @return CSV 临时文件绝对路径
     */
    private String generateCsvFile(int processedCount, int chunkSize) throws IOException {
        String fileName = "batch_" + processedCount + "_" + System.currentTimeMillis() + ".csv";
        String filePath = TEMP_DIR + File.separator + fileName;

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.UTF_8))) {
            for (int i = 0; i < chunkSize; i++) {
                long offset    = (long) processedCount + i;
                long id        = BASE_ID + offset;
                long projectId = BASE_PROJECT_ID + offset;

                // 列顺序与 COLUMNS 常量保持一致
                writer.write(
                        id + "," +
                        projectId + "," +
                        PROJECT_SNAPSHOT_ID + "," +
                        BUSINESS_NO + "," +
                        VERSION + "," +
                        TYPE_ID + "," +
                        TYPE_CODE + "," +
                        ATTACHMENT_ID + "," +
                        FIXED_DATETIME + "," +
                        CREATE_USER_ID + "," +
                        FIXED_DATETIME + "," +
                        UPDATE_USER_ID + "," +
                        DELETED
                );
                writer.newLine();
            }
        }
        log.debug("【LOAD DATA】CSV 已生成：{}，条数：{}", filePath, chunkSize);
        return filePath;
    }

    /**
     * 执行 LOAD DATA LOCAL INFILE
     * 文件路径由内部生成（非用户输入），直接嵌入 SQL 安全；Windows 反斜杠转正斜杠
     */
    private int loadDataWithFile(Connection conn, String filePath) throws SQLException {
        String mysqlPath = filePath.replace("\\", "/");
        String loadSql = "LOAD DATA LOCAL INFILE '" + mysqlPath + "'"
                + " INTO TABLE " + TABLE_NAME
                + " FIELDS TERMINATED BY ','"
                + " LINES TERMINATED BY '\\n'"
                + " " + COLUMNS;

        log.debug("【LOAD DATA】执行 SQL：{}", loadSql);
        try (Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(loadSql);
        }
    }

    /** 安全删除临时文件（finally 块调用，异常也保证执行） */
    private void cleanupTempFile(String filePath) {
        if (filePath == null) {
            return;
        }
        File file = new File(filePath);
        if (file.exists() && !file.delete()) {
            log.warn("【LOAD DATA】临时文件删除失败，请手动清理：{}", filePath);
        }
    }

    // =========================================================
    // 方案二：PreparedStatement（10~50 万级，~68s/百万）
    // =========================================================

    @Override
    public Map<String, Object> batchInsertWithPreparedStatement(int count, int batchSize) {
        log.info("【PreparedStatement】开始批量插入，总条数：{}，批次大小：{}", count, batchSize);
        long totalStart    = System.currentTimeMillis();
        int  totalAffected = 0;

        String sql = "INSERT INTO " + TABLE_NAME +
                " (id,project_id,project_snapshot_id,business_no,version,type_id,type_code," +
                "attachment_id,create_time,create_user_id,update_time,update_user_id,deleted)" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (int i = 0; i < count; i++) {
                long offset    = (long) i;
                long id        = BASE_ID + offset;
                long projectId = BASE_PROJECT_ID + offset;

                pstmt.setLong  (1,  id);
                pstmt.setLong  (2,  projectId);
                pstmt.setLong  (3,  PROJECT_SNAPSHOT_ID);
                pstmt.setString(4,  BUSINESS_NO);
                pstmt.setInt   (5,  VERSION);
                pstmt.setLong  (6,  TYPE_ID);
                pstmt.setString(7,  TYPE_CODE);
                pstmt.setLong  (8,  ATTACHMENT_ID);
                pstmt.setString(9,  FIXED_DATETIME);
                pstmt.setInt   (10, CREATE_USER_ID);
                pstmt.setString(11, FIXED_DATETIME);
                pstmt.setInt   (12, UPDATE_USER_ID);
                pstmt.setInt   (13, DELETED);
                pstmt.addBatch();

                if ((i + 1) % batchSize == 0 || i == count - 1) {
                    int[] results = pstmt.executeBatch();
                    for (int r : results) {
                        totalAffected += r;
                    }
                    conn.commit();
                    pstmt.clearBatch();
                    log.info("【PreparedStatement】已提交 {}/{}", Math.min(i + 1, count), count);
                }
            }
        } catch (SQLException e) {
            log.error("【PreparedStatement】批量插入失败", e);
            throw new RuntimeException("PreparedStatement 批量插入失败: " + e.getMessage(), e);
        }

        long elapsed = System.currentTimeMillis() - totalStart;
        log.info("【PreparedStatement】完成，总条数：{}，耗时：{}ms（{} s）",
                count, elapsed, String.format("%.2f", elapsed / 1000.0));
        return this.buildResult("PreparedStatement", count, totalAffected, elapsed);
    }

    // =========================================================
    // 方案一：Statement.executeBatch（10 万以下，~112s/百万，不推荐）
    // =========================================================

    @Override
    public Map<String, Object> batchInsertWithStatement(int count, int batchSize) {
        log.info("【Statement】开始批量插入，总条数：{}，批次大小：{}", count, batchSize);
        long totalStart = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(false);

            for (int i = 0; i < count; i++) {
                long offset    = (long) i;
                long id        = BASE_ID + offset;
                long projectId = BASE_PROJECT_ID + offset;

                // 直接拼接 SQL，存在 SQL 注入风险，仅用于性能对比演示
                String insertSql = String.format(
                        "INSERT INTO %s (id,project_id,project_snapshot_id,business_no,version,type_id," +
                        "type_code,attachment_id,create_time,create_user_id,update_time,update_user_id,deleted)" +
                        " VALUES (%d,%d,%d,'%s',%d,%d,'%s',%d,'%s',%d,'%s',%d,%d)",
                        TABLE_NAME,
                        id, projectId, PROJECT_SNAPSHOT_ID, BUSINESS_NO, VERSION,
                        TYPE_ID, TYPE_CODE, ATTACHMENT_ID, FIXED_DATETIME,
                        CREATE_USER_ID, FIXED_DATETIME, UPDATE_USER_ID, DELETED);
                stmt.addBatch(insertSql);

                if ((i + 1) % batchSize == 0 || i == count - 1) {
                    stmt.executeBatch();
                    conn.commit();
                    stmt.clearBatch();
                    log.info("【Statement】已提交 {}/{}", Math.min(i + 1, count), count);
                }
            }
        } catch (SQLException e) {
            log.error("【Statement】批量插入失败", e);
            throw new RuntimeException("Statement.executeBatch 批量插入失败: " + e.getMessage(), e);
        }

        long elapsed = System.currentTimeMillis() - totalStart;
        log.info("【Statement】完成，总条数：{}，耗时：{}ms（{} s）",
                count, elapsed, String.format("%.2f", elapsed / 1000.0));
        return this.buildResult("Statement.executeBatch", count, count, elapsed);
    }

    // =========================================================
    // 公共工具方法
    // =========================================================

    private Map<String, Object> buildResult(String method, int totalCount, int affectedRows, long elapsedMs) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method",       method);
        result.put("totalCount",   totalCount);
        result.put("affectedRows", affectedRows);
        result.put("elapsedMs",    elapsedMs);
        result.put("elapsedSec",   String.format("%.3f", elapsedMs / 1000.0));
        return result;
    }
}
