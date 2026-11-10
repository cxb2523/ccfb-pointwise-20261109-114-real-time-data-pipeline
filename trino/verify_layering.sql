-- Trino 端核对脚本：catalog 名 iceberg 与 trino/iceberg.properties 的文件名一致。
-- 进入 CLI：docker-compose exec trino trino
-- 执行：     trino> -- 逐条粘贴，或 trino --file 挂载后执行

-- 1) reject_reason 分布：四类脏数据各应有 2 条，且没有静默丢失
SELECT reject_reason, count(*) AS cnt
FROM iceberg.ods.clickstream_dirty
GROUP BY reject_reason
ORDER BY reject_reason;

-- 2) ODS 原样层行数 = 脏表行数 + DWD 去重后行数（全量不丢、脏表可对账）
SELECT
    (SELECT count(*) FROM iceberg.ods.clickstream)        AS ods_total,
    (SELECT count(*) FROM iceberg.ods.clickstream_dirty)  AS dirty_total,
    (SELECT count(*) FROM iceberg.dwd.clickstream_dedup)  AS dwd_total;

-- 3) event_id 无重复：下面两个计数必须相等（主键去重生效，UPSERT 未退化成追加）
SELECT
    count(*)            AS rows_cnt,
    count(DISTINCT event_id) AS distinct_event_id_cnt
FROM iceberg.dwd.clickstream_dedup;

-- 4) 重复样例 event_id 在 dwd 只剩一行
SELECT event_id, count(*) AS cnt
FROM iceberg.dwd.clickstream_dedup
WHERE event_id IN ('evt-dup-A', 'evt-dup-B')
GROUP BY event_id
ORDER BY event_id;

-- 5) dim_user 取最新 profile：专用用户的 url 必须是时间最晚的 /checkout
SELECT user_id, event_type, url, device, event_time
FROM iceberg.dim.dim_user
WHERE user_id = 'user-latest-01';

-- 6) dim_user 主键唯一性：user_id 不允许重复（UPSERT 语义核对）
SELECT count(*) AS rows_cnt, count(DISTINCT user_id) AS distinct_user_cnt
FROM iceberg.dim.dim_user;

-- 7) 脏表原始消息留存核对：raw_payload 不得为空
SELECT reject_reason,
       count(*)                                                  AS cnt,
       count_if(raw_payload IS NULL OR raw_payload = '')         AS empty_payload_cnt
FROM iceberg.ods.clickstream_dirty
GROUP BY reject_reason
ORDER BY reject_reason;
