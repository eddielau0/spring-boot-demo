# demo-batch-data

> 百万级数据库批量插入性能对比 Demo

## 简介

本模块对比三种常见的 MySQL 批量插入方案，量化各方案在百万级数据下的性能差异：

| 方案 | 耗时（百万条参考） | 安全性 | 适用场景 |
|------|:-----------------:|:------:|---------|
| `LOAD DATA LOCAL INFILE` | **~9 s** | ✅ | 百万级以上，**强烈推荐** |
| `PreparedStatement` | ~68 s | ✅ | 10~50 万级 |
| `Statement.executeBatch` | ~112 s | ❌ SQL 注入风险 | 仅对比演示，不推荐生产 |

> 测试环境：i7-8700K / MySQL 8.0

---

## 快速开始

### 1. 数据库准备

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS demo DEFAULT CHARSET utf8mb4;

-- 执行建表脚本
source src/main/resources/db/init.sql;
```

### 2. 开启 MySQL 服务端 local_infile（仅 LOAD DATA 方案需要）

```sql
SET GLOBAL local_infile = 1;
```

### 3. 修改配置

编辑 `src/main/resources/application.yml`，填入你的 MySQL 连接信息：

```yaml
spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/demo?...&allowLoadLocalInfile=true
    username: root
    password: root
```

> ⚠️ URL 必须包含 `allowLoadLocalInfile=true`，否则 LOAD DATA 方案会报错。

### 4. 启动

```bash
mvn spring-boot:run -pl demo-batch-data
```

---

## 接口说明

所有接口均为 `POST` 方法，返回包含耗时统计的 JSON。

### LOAD DATA LOCAL INFILE（推荐）

```
POST http://localhost:8080/batch/loadData?count=1000000&chunkSize=100000
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `count` | 1000000 | 总插入条数 |
| `chunkSize` | 100000 | 每块生成一个 CSV 文件，防止内存溢出 |

### PreparedStatement

```
POST http://localhost:8080/batch/preparedStatement?count=100000&batchSize=1000
```

### Statement.executeBatch（不推荐）

```
POST http://localhost:8080/batch/statement?count=10000&batchSize=1000
```

### 响应示例

```json
{
  "method": "LOAD DATA LOCAL INFILE",
  "totalCount": 1000000,
  "affectedRows": 1000000,
  "elapsedMs": 9123,
  "elapsedSec": "9.123"
}
```

---

## 核心原理

### LOAD DATA LOCAL INFILE 为何快？

1. **批量文件导入**：将数据写入 CSV 临时文件，由 MySQL 引擎直接解析，绕过逐行 SQL 解析开销。
2. **分块写入**：每 `chunkSize` 条生成一个 CSV 文件，避免单次内存溢出。
3. **事务控制**：每块独立提交，失败自动回滚当前块。
4. **临时文件清理**：每块处理完成后立即删除 CSV 文件（`finally` 块保证执行）。

### LOAD DATA 前置要求

```
MySQL URL  : 必须含 allowLoadLocalInfile=true
MySQL 服务端: SET GLOBAL local_infile = 1;
```

---

## 项目结构

```
demo-batch-data
└── src/main
    ├── java/com/xkcoding/batch
    │   ├── BatchDataApplication.java          # 启动类
    │   ├── config
    │   │   └── LoadDataSourceConfig.java      # 直连 DataSource（含 allowLoadLocalInfile）
    │   ├── controller
    │   │   └── BatchDataController.java       # HTTP 接口
    │   └── service
    │       ├── LoadDataBatchService.java      # 服务接口
    │       └── impl
    │           └── LoadDataBatchServiceImpl.java  # 三种方案实现
    └── resources
        ├── application.yml
        └── db
            └── init.sql                       # 建表 DDL
```
