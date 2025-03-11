import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExporterInfoNoMysql extends AbstractJavaSamplerClient {
    private static final Logger log = LogManager.getLogger(ExporterInfoNoMysql.class);
    private static ScheduledExecutorService scheduler;
    private String[] ips;
    private static Map<String, BufferedWriter> writers = new HashMap<>();
    private long currentDate;
    private int n = 1;
    private static Map<String, Map<String, Map<Integer, Double>>> total = new HashMap<>();
    // 存储每个 IP 对应的上次采样指标
    private static Map<String, LastSampleMetrics> lastSampleMetricsMap = new HashMap<>();

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument("ip", "39.107.95.220");
        return arguments;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.sampleStart();

        try {
            String threadName = Thread.currentThread().getName();
            if (threadName.contains("setUp")) {
                this.ips = context.getParameter("ip").contains(",") ? context.getParameter("ip").split(",") : new String[]{context.getParameter("ip")};
                currentDate = System.currentTimeMillis();

                for (String ip : ips) {
                    String[] cleanIp = ip.contains(":") ? ip.split(":") : new String[]{ip, "9100"};
                    File file = new File(currentDate + ".csv");
                    BufferedWriter writer = writers.get(cleanIp[0]);

                    if (writer == null) {
                        writer = new BufferedWriter(new FileWriter(file, true));
                        writers.put(cleanIp[0], writer);
                        writer.write("ip,cpu_usage(%),memory_usage(%),io_per_second,network_mb/s");
                        writer.newLine();
                    }
                    // 初始化每个 IP 的上次采样指标
                    lastSampleMetricsMap.put(cleanIp[0], new LastSampleMetrics());
                }

                scheduler = Executors.newSingleThreadScheduledExecutor();
                scheduler.scheduleAtFixedRate(() -> {
                    try {
                        for (String ip : ips) {
                            String[] cleanIp = ip.contains(":") ? ip.split(":") : new String[]{ip, "9100"};
                            BufferedWriter writer = writers.get(cleanIp[0]);
                            NodeMetrics metrics = fetchMetricsFromNodeExporter(cleanIp[0], cleanIp[1]);

                            String line = String.format("%s,%.2f,%.2f,%.2f,%.2f",
                                    ip, metrics.cpuUsage, metrics.memoryUsage, metrics.ioPerSecond, metrics.networkBytesPerSecond);

                            Map<String, Map<Integer, Double>> detail = null;
                            Map<Integer, Double> detail_cpuUsage = null;
                            Map<Integer, Double> detail_memoryUsage = null;
                            Map<Integer, Double> detail_ioPerSecond = null;
                            Map<Integer, Double> detail_networkBytesPerSecond = null;

                            if (total.get(cleanIp[0]) == null) {
                                detail = new HashMap<>();
                                total.put(cleanIp[0], detail);
                            }
                            if (total.get(cleanIp[0]).get("cpu") == null) {
                                detail_cpuUsage = new HashMap<>();
                                detail_memoryUsage = new HashMap<>();
                                detail_ioPerSecond = new HashMap<>();
                                detail_networkBytesPerSecond = new HashMap<>();
                                total.get(cleanIp[0]).put("cpu", detail_cpuUsage);
                                total.get(cleanIp[0]).put("men", detail_memoryUsage);
                                total.get(cleanIp[0]).put("io", detail_ioPerSecond);
                                total.get(cleanIp[0]).put("net", detail_networkBytesPerSecond);
                            }

                            total.get(cleanIp[0]).get("cpu").put(n, metrics.cpuUsage);
                            total.get(cleanIp[0]).get("men").put(n, metrics.memoryUsage);
                            total.get(cleanIp[0]).get("io").put(n, metrics.ioPerSecond);
                            total.get(cleanIp[0]).get("net").put(n, metrics.networkBytesPerSecond);

                            if (ips[0].equals(cleanIp[0]) || ips[0].equals(cleanIp[0] + ":" + cleanIp[1]))
                                n += 1;

                            synchronized (writer) {
                                writer.write(line);
                                writer.newLine();
                                writer.flush();
                            }
                        }
                        log.info("指标数据已写入文件");
                    } catch (Exception e) {
                        log.error("写入文件失败", e);
                    }
                }, 0, 1, TimeUnit.SECONDS);

                result.setResponseMessage("文件写入器已初始化");
                result.setResponseCodeOK();
                result.setSuccessful(true);

            } else if (threadName.contains("tearDown")) {
                if (scheduler != null && !scheduler.isShutdown()) {
                    scheduler.shutdownNow();
                }
                Thread.sleep(1000);

                for (String ip : ips) {
                    String cleanIp = ip.contains(":") ? ip.split(":")[0] : ip;
                    Map<String, Map<Integer, Double>> detail = total.get(cleanIp);
                    Double c = detail.get("cpu").values().stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                    Double m = detail.get("men").values().stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                    Double i = detail.get("io").values().stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                    Double n = detail.get("net").values().stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                    BufferedWriter writer = writers.get(cleanIp);
                    writer.newLine();
                    String line = String.format("%s,%.2f,%.2f,%.2f,%.2f,%s", ip, c, m, i, n, "结果");
                    writer.write(line);
                    writer.flush();
                }

                for (BufferedWriter writer : writers.values()) {
                    try {
                        if (writer != null) {
                            writer.close();
                        }
                    } catch (Exception e) {
                        log.error("关闭文件写入器失败", e);
                    }
                }
                writers.clear();

                result.setResponseMessage("文件写入器已关闭");
                result.setResponseCodeOK();
                result.setSuccessful(true);

            } else {
                log.warn("不是 Setup 或 Teardown 线程组中的取样器");
                result.setResponseMessage("操作未执行");
                result.setResponseCodeOK();
                result.setSuccessful(true);
            }

            result.sampleEnd();
        } catch (Exception e) {
            log.error("操作失败", e);
            result.setResponseMessage("操作失败: " + e.getMessage());
            result.setResponseCode("500");
            result.setSuccessful(false);
            result.sampleEnd();
        }

        return result;
    }

    private NodeMetrics fetchMetricsFromNodeExporter(String ip, String port) {
        NodeMetrics metrics = new NodeMetrics();
        try {
            String url = "http://" + ip + ":" + port + "/metrics";
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            if (connection == null) {
                log.error("无法建立到 {} 的连接", url);
                return metrics;
            }
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                log.error("从 {} 获取数据失败，响应状态码: {}", url, responseCode);
                return metrics;
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            double idleTime = 0;
            double totalTime = 0;

            while ((line = in.readLine()) != null) {
                if (line.startsWith("node_cpu_seconds_total")) {
                    String[] parts = line.split("\\s+");
                    double value = Double.parseDouble(parts[1]);
                    totalTime += value;

                    if (line.contains("mode=\"idle\"")) {
                        idleTime += value;
                    }
                } else if (line.startsWith("node_memory_MemAvailable_bytes")) {
                    metrics.memoryAvailable = Double.parseDouble(line.split(" ")[1]);
                } else if (line.startsWith("node_memory_MemTotal_bytes")) {
                    metrics.memoryTotal = Double.parseDouble(line.split(" ")[1]);
                } else if (line.startsWith("node_disk_reads_completed_total")) {
                    metrics.readsCompleted += Double.parseDouble(line.split(" ")[1]);
                } else if (line.startsWith("node_disk_writes_completed_total")) {
                    metrics.writesCompleted += Double.parseDouble(line.split(" ")[1]);
                } else if (line.startsWith("node_network_receive_bytes_total")) {
                    metrics.networkReceive += Double.parseDouble(line.split(" ")[1]);
                } else if (line.startsWith("node_network_transmit_bytes_total")) {
                    metrics.networkTransmit += Double.parseDouble(line.split(" ")[1]);
                }
            }
            in.close();

            // 获取该 IP 对应的上次采样指标
            LastSampleMetrics lastMetrics = lastSampleMetricsMap.get(ip);
            double currentTotalIO = metrics.readsCompleted + metrics.writesCompleted;
            double currentTotalNet = metrics.networkReceive + metrics.networkTransmit;

            // 计算 CPU 利用率
            if (lastMetrics.totalTime != 0) {
                metrics.cpuUsage = (1 - ((idleTime - lastMetrics.idleTime) / (totalTime - lastMetrics.totalTime))) * 100;
                metrics.cpuUsage = Math.round(metrics.cpuUsage * 100.0) / 100.0;
            } else {
                metrics.cpuUsage = 0;
            }
//            double usage = 1 - (idleTime / totalTime);
//            metrics.cpuUsage = Math.round(usage * 10000.0) / 100.0;

            // 计算内存利用率
            metrics.memoryUsage = (1 - (metrics.memoryAvailable / metrics.memoryTotal)) * 100;
            metrics.memoryUsage = Math.round(metrics.memoryUsage * 100.0) / 100.0;

            // 计算平均每秒 I/O 次数
            if (lastMetrics.totalio != 0) {
                metrics.ioPerSecond = currentTotalIO - lastMetrics.totalio;
                metrics.ioPerSecond = Math.round(metrics.ioPerSecond * 100.0) / 100.0;
            } else {
                metrics.ioPerSecond = 0;
            }

            // 计算每秒流量上传下载的 Mb/s
            if (lastMetrics.totalnet != 0) {
                metrics.networkBytesPerSecond = (currentTotalNet - lastMetrics.totalnet) / (1024 * 1024);
                metrics.networkBytesPerSecond = Math.round(metrics.networkBytesPerSecond * 100.0) / 100.0;
            } else {
                metrics.networkBytesPerSecond = 0;
            }
            // 更新该 IP 的上次采样指标
            lastMetrics.idleTime = idleTime;
            lastMetrics.totalTime = totalTime;
            lastMetrics.readsCompleted = metrics.readsCompleted;
            lastMetrics.writesCompleted = metrics.writesCompleted;
            lastMetrics.totalio = currentTotalIO;
            lastMetrics.networkReceive = metrics.networkReceive;
            lastMetrics.networkTransmit = metrics.networkTransmit;
            lastMetrics.totalnet = currentTotalNet;

        } catch (Exception e) {
            log.error("从 {}node_exporter 获取数据失败", ip, e);
        }
        return metrics;
    }

    private static class NodeMetrics {
        double cpuUsage = 0;
        double memoryAvailable = 0;
        double memoryTotal = 0;
        double memoryUsage = 0;
        double readsCompleted = 0;
        double writesCompleted = 0;
        double ioPerSecond = 0;
        double networkReceive = 0;
        double networkTransmit = 0;
        double networkBytesPerSecond = 0;
    }

    private static class LastSampleMetrics {
        double idleTime = 0;
        double totalTime = 0;
        double totalio = 0;
        double readsCompleted = 0;
        double writesCompleted = 0;
        double totalnet = 0;
        double networkReceive = 0;
        double networkTransmit = 0;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        super.setupTest(context);
        this.ips = context.getParameter("ip").contains(",") ?
                context.getParameter("ip").split(",") :
                new String[]{context.getParameter("ip")};
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        for (BufferedWriter writer : writers.values()) {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {
                log.error("关闭文件写入器失败", e);
            }
        }
        writers.clear();
    }
}