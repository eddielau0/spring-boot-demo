package com.xkcoding.batch.config;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * @Author: liuqiang
 * @Version: 1.0.0
 * @Date: 2026/03/28 15:27
 * @Description: LOAD DATA LOCAL INFILE 专用直连 DataSource 配置
 *
 * 背景：
 *   LOAD DATA LOCAL INFILE 要求 JDBC URL 含 allowLoadLocalInfile=true，
 *   且 MySQL 服务端需执行一次：SET GLOBAL local_infile = 1;
 *   此 Bean 直连 MySQL 实例，仅供批量导入场景使用。
 *
 * 配置项（application.yml）：
 *   spring.datasource.url      # 直连 MySQL JDBC URL（必须含 allowLoadLocalInfile=true）
 *   spring.datasource.username # 数据库用户名
 *   spring.datasource.password # 数据库密码
 */
@Slf4j
@Configuration
public class LoadDataSourceConfig {

    @Value("${spring.datasource.url}")
    private String url;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    /**
     * LOAD DATA 专用 DataSource，包含 allowLoadLocalInfile=true
     * 作为 Primary Bean 供全局注入；如需多数据源可通过 @Qualifier 区分
     */
    @Primary
    @Bean("loadDataSource")
    public DataSource loadDataSource() {
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        // 连接池轻量配置：LOAD DATA 批量插入不需要大量并发连接
        ds.setInitialSize(1);
        ds.setMinIdle(1);
        ds.setMaxActive(5);
        ds.setMaxWait(30_000);
        ds.setTestWhileIdle(true);
        ds.setValidationQuery("SELECT 1");
        log.info("【LoadDataSource】直连 MySQL DataSource 初始化完成，url={}", url);
        return ds;
    }
}
