package com.lakehouse;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

import org.apache.hadoop.conf.Configuration;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink DataStream job: consumes Debezium CDC JSON from Kafka and writes to Iceberg bronze tables.
 *
 * Topics (flat JSON via ExtractNewRecordState SMT):
 *   cdc.public.customers → iceberg bronze.customers
 *   cdc.public.products  → iceberg bronze.products
 *   cdc.public.orders    → iceberg bronze.orders
 *
 * Upsert mode on equality field 'id'.  Checkpointing every 60 s.
 */
public class KafkaToIcebergJob {

    // ── Connection constants ────────────────────────────────────────────────
    private static final String KAFKA_BROKERS      = "kafka:9092";
    private static final String ICEBERG_REST_URI   = "http://iceberg-rest:8181";
    private static final String WAREHOUSE          = "s3://lakehouse/";
    private static final String MINIO_ENDPOINT     = "http://minio:9000";
    private static final String MINIO_ACCESS_KEY   = "minioadmin";
    private static final String MINIO_SECRET_KEY   = "minioadmin123";
    private static final String CONSUMER_GROUP     = "flink-lakehouse";

    public static void main(String[] args) throws Exception {

        // ── 1. Execution environment ────────────────────────────────────────
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);

        // ── 2. Catalog and table setup ───────────────────────────────────────
        Map<String, String> catalogProps = buildCatalogProperties();
        RESTCatalog catalog = openCatalog(catalogProps);
        ensureNamespace(catalog, "bronze");
        ensureTable(catalog, "bronze", "customers", customersSchema());
        ensureTable(catalog, "bronze", "products",  productsSchema());
        ensureTable(catalog, "bronze", "orders",    ordersSchema());
        catalog.close();

        // ── 3. CatalogLoader (serialisable reference passed to sinks) ───────
        Configuration hadoopConf = buildHadoopConf();
        CatalogLoader catalogLoader = CatalogLoader.custom(
                "lakehouse",
                catalogProps,
                hadoopConf,
                "org.apache.iceberg.rest.RESTCatalog");

        // ── 4. Wire customers ────────────────────────────────────────────────
        DataStream<RowData> customersStream =
                kafkaStream(env, "cdc.public.customers", "customers-src")
                        .map(new DebeziumToRowData(customersSchema(), "customers"))
                        .name("parse-customers");

        FlinkSink.forRowData(customersStream)
                .tableLoader(TableLoader.fromCatalog(
                        catalogLoader, TableIdentifier.of(Namespace.of("bronze"), "customers")))
                .upsert(true)
                .equalityFieldColumns(Collections.singletonList("id"))
                .append();

        // ── 5. Wire products ─────────────────────────────────────────────────
        DataStream<RowData> productsStream =
                kafkaStream(env, "cdc.public.products", "products-src")
                        .map(new DebeziumToRowData(productsSchema(), "products"))
                        .name("parse-products");

        FlinkSink.forRowData(productsStream)
                .tableLoader(TableLoader.fromCatalog(
                        catalogLoader, TableIdentifier.of(Namespace.of("bronze"), "products")))
                .upsert(true)
                .equalityFieldColumns(Collections.singletonList("id"))
                .append();

        // ── 6. Wire orders ───────────────────────────────────────────────────
        DataStream<RowData> ordersStream =
                kafkaStream(env, "cdc.public.orders", "orders-src")
                        .map(new DebeziumToRowData(ordersSchema(), "orders"))
                        .name("parse-orders");

        FlinkSink.forRowData(ordersStream)
                .tableLoader(TableLoader.fromCatalog(
                        catalogLoader, TableIdentifier.of(Namespace.of("bronze"), "orders")))
                .upsert(true)
                .equalityFieldColumns(Collections.singletonList("id"))
                .append();

        env.execute("KafkaToIcebergJob");
    }

    // ── Kafka source factory ────────────────────────────────────────────────

    private static SingleOutputStreamOperator<String> kafkaStream(
            StreamExecutionEnvironment env, String topic, String uid) {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .append();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-" + uid);
    }

    // ── Catalog helpers ─────────────────────────────────────────────────────

    private static RESTCatalog openCatalog(Map<String, String> props) {
        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("lakehouse", props);
        return catalog;
    }

    private static void ensureNamespace(RESTCatalog catalog, String ns) {
        Namespace namespace = Namespace.of(ns);
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }
    }

    private static void ensureTable(RESTCatalog catalog, String ns, String tableName, Schema schema) {
        TableIdentifier id = TableIdentifier.of(Namespace.of(ns), tableName);
        if (!catalog.tableExists(id)) {
            Map<String, String> props = new HashMap<>();
            props.put(TableProperties.FORMAT_VERSION, "2");
            props.put("write.upsert.enabled", "true");
            catalog.createTable(id, schema, PartitionSpec.unpartitioned(), props);
        }
    }

    // ── Catalog / Hadoop configuration ──────────────────────────────────────

    private static Map<String, String> buildCatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("uri",                  ICEBERG_REST_URI);
        props.put("warehouse",            WAREHOUSE);
        props.put("io.impl",              "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint",          MINIO_ENDPOINT);
        props.put("s3.access-key-id",     MINIO_ACCESS_KEY);
        props.put("s3.secret-access-key", MINIO_SECRET_KEY);
        props.put("s3.path-style-access", "true");
        props.put("s3.region",            "us-east-1");
        return props;
    }

    private static Configuration buildHadoopConf() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint",          MINIO_ENDPOINT);
        conf.set("fs.s3a.access.key",        MINIO_ACCESS_KEY);
        conf.set("fs.s3a.secret.key",        MINIO_SECRET_KEY);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return conf;
    }

    // ── Iceberg schemas ──────────────────────────────────────────────────────

    static Schema customersSchema() {
        return new Schema(
            Types.NestedField.required(1,  "id",         Types.LongType.get()),
            Types.NestedField.optional(2,  "name",       Types.StringType.get()),
            Types.NestedField.optional(3,  "email",      Types.StringType.get()),
            Types.NestedField.optional(4,  "city",       Types.StringType.get()),
            Types.NestedField.optional(5,  "created_at", Types.TimestampType.withZone()),
            Types.NestedField.optional(6,  "updated_at", Types.TimestampType.withZone()),
            Types.NestedField.optional(7,  "__op",       Types.StringType.get())
        );
    }

    static Schema productsSchema() {
        return new Schema(
            Types.NestedField.required(1,  "id",         Types.LongType.get()),
            Types.NestedField.optional(2,  "name",       Types.StringType.get()),
            Types.NestedField.optional(3,  "category",   Types.StringType.get()),
            Types.NestedField.optional(4,  "price",      Types.DecimalType.of(12, 2)),
            Types.NestedField.optional(5,  "stock",      Types.IntegerType.get()),
            Types.NestedField.optional(6,  "created_at", Types.TimestampType.withZone()),
            Types.NestedField.optional(7,  "updated_at", Types.TimestampType.withZone()),
            Types.NestedField.optional(8,  "__op",       Types.StringType.get())
        );
    }

    static Schema ordersSchema() {
        return new Schema(
            Types.NestedField.required(1,  "id",           Types.LongType.get()),
            Types.NestedField.optional(2,  "customer_id",  Types.LongType.get()),
            Types.NestedField.optional(3,  "product_id",   Types.LongType.get()),
            Types.NestedField.optional(4,  "quantity",     Types.IntegerType.get()),
            Types.NestedField.optional(5,  "total_amount", Types.DecimalType.of(14, 2)),
            Types.NestedField.optional(6,  "status",       Types.StringType.get()),
            Types.NestedField.optional(7,  "created_at",   Types.TimestampType.withZone()),
            Types.NestedField.optional(8,  "updated_at",   Types.TimestampType.withZone()),
            Types.NestedField.optional(9,  "__op",         Types.StringType.get())
        );
    }

    // ── Debezium JSON → RowData mapper ───────────────────────────────────────

    /**
     * Deserialises a flat Debezium JSON string (post-SMT unwrap) into a Flink RowData
     * matching the Iceberg table schema.
     */
    static class DebeziumToRowData implements MapFunction<String, RowData> {

        private final String tableName;
        private transient ObjectMapper mapper;
        private transient Schema schema;

        DebeziumToRowData(Schema schema, String tableName) {
            this.tableName = tableName;
        }

        @Override
        public RowData map(String json) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> m = mapper.readValue(json, Map.class);

            switch (tableName) {
                case "customers": return mapCustomer(m);
                case "products":  return mapProduct(m);
                case "orders":    return mapOrder(m);
                default: throw new IllegalArgumentException("Unknown table: " + tableName);
            }
        }

        private RowData mapCustomer(Map<String, Object> m) {
            GenericRowData row = new GenericRowData(7);
            row.setField(0, toLong(m.get("id")));
            row.setField(1, toStr(m.get("name")));
            row.setField(2, toStr(m.get("email")));
            row.setField(3, toStr(m.get("city")));
            row.setField(4, toTimestamp(m.get("created_at")));
            row.setField(5, toTimestamp(m.get("updated_at")));
            row.setField(6, toStr(m.get("__op")));
            return row;
        }

        private RowData mapProduct(Map<String, Object> m) {
            GenericRowData row = new GenericRowData(8);
            row.setField(0, toLong(m.get("id")));
            row.setField(1, toStr(m.get("name")));
            row.setField(2, toStr(m.get("category")));
            row.setField(3, toDecimal(m.get("price"), 12, 2));
            row.setField(4, toInt(m.get("stock")));
            row.setField(5, toTimestamp(m.get("created_at")));
            row.setField(6, toTimestamp(m.get("updated_at")));
            row.setField(7, toStr(m.get("__op")));
            return row;
        }

        private RowData mapOrder(Map<String, Object> m) {
            GenericRowData row = new GenericRowData(9);
            row.setField(0, toLong(m.get("id")));
            row.setField(1, toLong(m.get("customer_id")));
            row.setField(2, toLong(m.get("product_id")));
            row.setField(3, toInt(m.get("quantity")));
            row.setField(4, toDecimal(m.get("total_amount"), 14, 2));
            row.setField(5, toStr(m.get("status")));
            row.setField(6, toTimestamp(m.get("created_at")));
            row.setField(7, toTimestamp(m.get("updated_at")));
            row.setField(8, toStr(m.get("__op")));
            return row;
        }

        // ── Type conversion helpers ─────────────────────────────────────────

        private static Long toLong(Object v) {
            if (v == null) return null;
            return ((Number) v).longValue();
        }

        private static Integer toInt(Object v) {
            if (v == null) return null;
            return ((Number) v).intValue();
        }

        private static StringData toStr(Object v) {
            if (v == null) return null;
            return StringData.fromString(v.toString());
        }

        /**
         * Debezium with time.precision.mode=connect emits timestamps as epoch millis (long).
         * Convert millis → TimestampData (millis + nano adjustment).
         */
        private static TimestampData toTimestamp(Object v) {
            if (v == null) return null;
            long millis = ((Number) v).longValue();
            return TimestampData.fromEpochMillis(millis);
        }

        /**
         * Debezium with decimal.handling.mode=string emits NUMERIC as a string like "1500.00".
         */
        private static DecimalData toDecimal(Object v, int precision, int scale) {
            if (v == null) return null;
            BigDecimal bd = new BigDecimal(v.toString()).setScale(scale, java.math.RoundingMode.HALF_UP);
            return DecimalData.fromBigDecimal(bd, precision, scale);
        }
    }
}
