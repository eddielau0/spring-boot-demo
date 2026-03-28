-- ============================================================
-- 批量插入性能对比 Demo 建表脚本
-- 执行前请先创建数据库：CREATE DATABASE IF NOT EXISTS demo DEFAULT CHARSET utf8mb4;
-- ============================================================

-- 允许服务端接受 LOAD DATA LOCAL 上传（需要 root 或 SUPER 权限）
SET GLOBAL local_infile = 1;

-- 批量插入目标表
CREATE TABLE IF NOT EXISTS `test_batch_data`
(
    `id`                  BIGINT       NOT NULL COMMENT '主键 ID（雪花 ID）',
    `project_id`          BIGINT       NOT NULL COMMENT '项目 ID',
    `project_snapshot_id` BIGINT       NOT NULL COMMENT '项目快照 ID',
    `business_no`         VARCHAR(64)  NOT NULL COMMENT '业务单号',
    `version`             INT          NOT NULL DEFAULT 0 COMMENT '版本号',
    `type_id`             BIGINT       NOT NULL COMMENT '附件类型 ID',
    `type_code`           VARCHAR(128) NOT NULL COMMENT '附件类型编码',
    `attachment_id`       BIGINT       NOT NULL COMMENT '附件 ID',
    `create_time`         DATETIME     NOT NULL COMMENT '创建时间',
    `create_user_id`      BIGINT       NOT NULL DEFAULT 0 COMMENT '创建人 ID',
    `update_time`         DATETIME     NOT NULL COMMENT '更新时间',
    `update_user_id`      BIGINT       NOT NULL DEFAULT 0 COMMENT '更新人 ID',
    `deleted`             TINYINT      NOT NULL DEFAULT 0 COMMENT '逻辑删除（0=未删除，1=已删除）',
    PRIMARY KEY (`id`),
    KEY `idx_project_id` (`project_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '批量插入性能对比测试表';
