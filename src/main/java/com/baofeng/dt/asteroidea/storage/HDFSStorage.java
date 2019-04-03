package com.baofeng.dt.asteroidea.storage;

import com.baofeng.dt.asteroidea.exception.BucketClosedException;
import com.baofeng.dt.asteroidea.metrics.LogMetricMXBean;
import com.baofeng.dt.asteroidea.model.TopicMeta;
import com.baofeng.dt.asteroidea.util.MBeanUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mignjia
 * @date 17/3/13
 */
public class HDFSStorage implements LogMetricMXBean {

    private AtomicLong counter = new AtomicLong();
    private AtomicLong totalSize = new AtomicLong();
    private long qps;
    private long flowSize;

    @Override
    public long getMessageCount() {
        return counter.get();
    }

    @Override
    public long getQPS() {
        return qps;
    }

    @Override
    public String getFlowSize() {
        return String.format("%.2f", (float) flowSize / 1048576) + "M";
    }

    @Override
    public String getTopicMeta() {
        return this.topicMeta.toString();
    }

    public interface WriterCallback {
        void run(String filePath);
    }

    private static final Logger LOG = LoggerFactory.getLogger(HDFSStorage.class);

    private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");

    private final HDFSWriterFactory writerFactory = new HDFSWriterFactory();
    private Configuration conf;
    private TopicMeta topicMeta;

    private static final long defaultRollInterval = 0;
    private static final long defaultRollSize = 0;
    private static final long defaultRollCount = 0;
    private static final String defaultFileName = "log";
    private static final String defaultSuffix = ".txt";
    private static final String defaultInUsePrefix = "";
    private static final String defaultFileNameFormat = "HH";
    private static final String defaultInUseSuffix = ".tmp";
    private static final long defaultBatchSize = 1;
    private static final String defaultFileType = HDFSWriterFactory.DataStreamType;
    private static final int defaultMaxOpenFiles = 5000;
    // Time between close retries, in seconds
    private static final long defaultRetryInterval = 180;
    // Retry forever.
    private static final int defaultTryCount = Integer.MAX_VALUE;

    /**
     * Default length of time we wait for blocking BucketWriter calls
     * before timing out the operation. Intended to prevent server hangs.
     */
    private static final long defaultCallTimeout = 15000;
    /**
     * Default number of threads available for tasks
     * such as append/open/close/flush with hdfs.
     * These tasks are done in a separate thread in
     * the case that they take too long. In which
     * case we create a new file and move on.
     */
    private static final int defaultThreadPoolSize = 20;
    private static final int defaultRollTimerPoolSize = 5;
    private static final int defaultFileClosePoolSize = 5;
    private static final int defaultFileCloseCheckPeriod = 60;

    private SimpleDateFormat sdfPath = null;
    private SimpleDateFormat sdfName = null;
    private SimpleDateFormat sdfHour = null;

    private String filePathFormat;
    private String fileNameFormat;

    private long rollInterval;
    private long rollSize;
    private long rollCount;
    private long batchSize;
    private int threadsPoolSize;
    private int rollTimerPoolSize;
    private int fileClosePoolSize;
    private CompressionCodec codeC;
    private SequenceFile.CompressionType compType;
    private String fileType;
    private String filePath;
    private String fileName;
    private volatile String oldFile = "";
    private String suffix;
    private String inUsePrefix;
    private String inUseSuffix;
    private TimeZone timeZone;
    private int maxOpenFiles;
    private ExecutorService callTimeoutPool;
    private ScheduledExecutorService timedRollerPool;
    private ScheduledExecutorService fileClosePool;

    private boolean needRounding = false;
    private int roundUnit = Calendar.HOUR;
    private int roundValue = 1;
    private boolean useLocalTime = true;

    private long callTimeout;

    private volatile int idleTimeout;
    private Clock clock;
    private FileSystem mockFs;
    private HDFSWriter mockWriter;
    private final Object sfWritersLock = new Object();
    private long retryInterval;
    private int tryCount;
    private volatile WriterLinkedHashMap sfWriters;


    private static class WriterLinkedHashMap
            extends LinkedHashMap<String, BucketWriter> {

        private final int maxOpenFiles;

        public WriterLinkedHashMap(int maxOpenFiles) {
            super(16, 0.75f, true); // stock initial capacity/load, access ordering
            this.maxOpenFiles = maxOpenFiles;
        }

        @Override
        protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
            if (size() > maxOpenFiles) {
                // If we have more that max open files, then close the last one and
                // return true
                try {
                    eldest.getValue().close();
                } catch (IOException e) {
                    LOG.warn(eldest.getKey().toString(), e);
                } catch (InterruptedException e) {
                    LOG.warn(eldest.getKey().toString(), e);
                    Thread.currentThread().interrupt();
                }
                return true;
            } else {
                return false;
            }
        }
    }


    public void init(Configuration conf, TopicMeta topicMeta) {

        this.conf = conf;
        this.topicMeta = topicMeta;
        filePathFormat = topicMeta.getFilePathFormat();
        sdfPath = new SimpleDateFormat(filePathFormat);
        fileNameFormat = defaultFileNameFormat;
        sdfName = new SimpleDateFormat(fileNameFormat);
        sdfHour = new SimpleDateFormat("HH");
        String defaultFS = conf.get("fs.default.name");

        filePath = Preconditions.checkNotNull(
                topicMeta.getFilePath(), "hdfs.path is required");

        filePath = defaultFS + filePath;


        String ip = "";
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOG.error("Fail to get local host ip,", e);
        }

        fileName = topicMeta.getTopics() + "_" + ip;

        this.suffix = topicMeta.getFileSuffix();
        inUsePrefix = defaultInUsePrefix;
        inUseSuffix = defaultInUseSuffix;
        timeZone = null;
        rollInterval = defaultRollInterval;
        rollSize = defaultRollSize;
        rollCount = defaultRollCount;
        batchSize = defaultBatchSize;
        idleTimeout = 0;

        String codecName = topicMeta.getCodeC();
        fileType = topicMeta.getFileType();

        maxOpenFiles = defaultMaxOpenFiles;
        callTimeout = defaultCallTimeout;

        threadsPoolSize = defaultThreadPoolSize;
        rollTimerPoolSize = defaultRollTimerPoolSize;
        fileClosePoolSize = defaultFileClosePoolSize;
        tryCount = defaultTryCount;

        if (tryCount <= 0) {
            LOG.warn("Retry count value : " + tryCount + " is not " +
                    "valid. The sink will try to close the file until the file " +
                    "is eventually closed.");
            tryCount = defaultTryCount;
        }

        retryInterval = defaultRetryInterval;

        if (retryInterval <= 0) {
            LOG.warn("Retry Interval value: " + retryInterval + " is not " +
                    "valid. If the first close of a file fails, " +
                    "it may remain open and will not be renamed.");
            tryCount = 1;
        }

        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        if (codecName == null) {
            codeC = null;
            compType = SequenceFile.CompressionType.NONE;
        } else {
            codeC = getCodec(codecName);
            // TODO : set proper compression type
            compType = SequenceFile.CompressionType.BLOCK;
        }

        // Do not allow user to set fileType DataStream with codeC together
        // To prevent output file with compress extension (like .snappy)
        if (fileType.equalsIgnoreCase(HDFSWriterFactory.DataStreamType) && codecName != null) {
            throw new IllegalArgumentException("fileType: " + fileType +
                    " which does NOT support compressed output. Please don't set codeC" +
                    " or change the fileType if compressed output is desired.");
        }

        if (fileType.equalsIgnoreCase(HDFSWriterFactory.CompStreamType)) {
            Preconditions.checkNotNull(codeC, "It's essential to set compress codec"
                    + " when fileType is: " + fileType);
        }

        needRounding = false;

        if (useLocalTime) {
            clock = new SystemClock();
        }

        QPSProcessor processor = new QPSProcessor();
        processor.setDaemon(true);
        processor.start();
        LOG.info("Start qps thread.");

        MBeanUtils.register("HDFSAdapter", "FlowStatistics_" + topicMeta.getId(), this);


    }

    private static boolean codecMatches(Class<? extends CompressionCodec> cls, String codecName) {
        String simpleName = cls.getSimpleName();
        if (cls.getName().equals(codecName) || simpleName.equalsIgnoreCase(codecName)) {
            return true;
        }
        if (simpleName.endsWith("Codec")) {
            String prefix = simpleName.substring(0, simpleName.length() - "Codec".length());
            if (prefix.equalsIgnoreCase(codecName)) {
                return true;
            }
        }
        return false;
    }

    static CompressionCodec getCodec(String codecName) {
        Configuration conf = new Configuration();
//        conf.set("io.compression.codecs","org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");

        List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
        // Wish we could base this on DefaultCodec but appears not all codec's
        // extend DefaultCodec(Lzo)
        CompressionCodec codec = null;
        ArrayList<String> codecStrs = new ArrayList<String>();
        codecStrs.add("None");
        for (Class<? extends CompressionCodec> cls : codecs) {
            codecStrs.add(cls.getSimpleName());
            if (codecMatches(cls, codecName)) {
                try {
                    codec = cls.newInstance();
                } catch (InstantiationException e) {
                    LOG.error("Unable to instantiate " + cls + " class");
                } catch (IllegalAccessException e) {
                    LOG.error("Unable to access " + cls + " class");
                }
            }
        }

        if (codec == null) {
            if (!codecName.equalsIgnoreCase("None")) {
                throw new IllegalArgumentException("Unsupported compression codec "
                        + codecName + ".  Please choose from: " + codecStrs);
            }
        } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
            // Must check instanceof codec as BZip2Codec doesn't inherit Configurable
            // Must set the configuration for Configurable objects that may or do use
            // native libs
            ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
        }
        return codec;
    }


    public synchronized String getTime(String type, long timestamp) {
        String time = "";
        if (type.equals("name"))
            time = sdfName.format(timestamp);
        else if (type.equals("path"))
            time = sdfPath.format(timestamp);
        else if (type.equals("hour")) {
            time = sdfHour.format(timestamp);
        }
        return time;
    }


    public void doStore(String data) throws IOException {
        try {
            // reconstruct the path name by substituting place holders
            String realPath = filePath;
            String realName = fileName;
            long time = clock.currentTimeMillis();
            String pathDate = getTime("path", time);
            realPath += DIRECTORY_DELIMITER + pathDate;
            if (topicMeta.getFilePathMode().equals("hour")) {
                realPath += DIRECTORY_DELIMITER + getTime("hour", time);
            }
            realName = pathDate + "_" + getTime("name", time) + "_" + realName;
            String lookupPath = realPath + DIRECTORY_DELIMITER + realName;

            synchronized (sfWritersLock) {
                if (!oldFile.equals(lookupPath)) {
                    if (sfWriters.containsKey(oldFile)) {
                        sfWriters.get(oldFile).close();
                        sfWriters.remove(oldFile);
                    }
                }
                oldFile = lookupPath;
            }

            BucketWriter bucketWriter;
            // Callback to remove the reference to the bucket writer from the
            // sfWriters map so that all buffers used by the HDFS file
            // handles are garbage collected.
            WriterCallback closeCallback = new WriterCallback() {
                @Override
                public void run(String bucketPath) {
                    LOG.info("Writer callback called.");
                    synchronized (sfWritersLock) {
                        if (sfWriters.containsKey(bucketPath)) {
                            sfWriters.remove(bucketPath);
                        }
                    }
                }
            };

            bucketWriter = getBucketWriter(lookupPath, closeCallback);

            // Write the data to HDFS
            try {
                bucketWriter.append(data.getBytes());
            } catch (BucketClosedException ex) {
                LOG.info("Bucket was closed while trying to append, " +
                        "reinitializing bucket and writing event.");
                bucketWriter = getBucketWriter(lookupPath, closeCallback);
                bucketWriter.append(data.getBytes());
            }



            counter.incrementAndGet();
            totalSize.addAndGet(data.length());

        } catch (IOException eIO) {
            LOG.warn("HDFS IO error", eIO);
            throw eIO;
        } catch (Throwable th) {
            LOG.error("process failed", th);
            if (th instanceof Error) {
                throw (Error) th;
            }
        }
    }


    private BucketWriter getBucketWriter(String lookupPath,WriterCallback closeCallback) throws IOException {
        BucketWriter bucketWriter;
        HDFSWriter hdfsWriter = null;
        synchronized (sfWritersLock) {
            bucketWriter = sfWriters.get(lookupPath);
            // we haven't seen this file yet, so open it and cache the handle
            if (bucketWriter == null) {
                //在多线程环境下，上个时间窗口的writer 有可能被关闭，所以重新计算一下lookupPath，避免重建上个窗口的writer
                String realPath = filePath;
                String realName = fileName;
                long time = clock.currentTimeMillis();
                String pathDate = getTime("path", time);
                realPath += DIRECTORY_DELIMITER + pathDate;
                if (topicMeta.getFilePathMode().equals("hour")) {
                    realPath += DIRECTORY_DELIMITER + getTime("hour", time);
                }
                realName = pathDate + "_" + getTime("name", time) + "_" + realName;
                lookupPath = realPath + DIRECTORY_DELIMITER + realName;
                if(sfWriters.get(lookupPath) == null ){
                    hdfsWriter = writerFactory.getWriter(fileType);
                    bucketWriter = initializeBucketWriter(realPath, realName,
                            lookupPath, hdfsWriter, closeCallback);
                    sfWriters.put(lookupPath, bucketWriter);
                    LOG.info("create new bucketWriter:"+lookupPath);
                }else {
                    bucketWriter = sfWriters.get(lookupPath);
                }
            }
        }
        return  bucketWriter;
    }




    private BucketWriter initializeBucketWriter(String realPath,
                                                String realName, String lookupPath, HDFSWriter hdfsWriter, WriterCallback closeCallback) {
        BucketWriter bucketWriter = new BucketWriter(rollInterval,
                rollSize, rollCount,
                batchSize, realPath, realName, inUsePrefix, inUseSuffix,
                suffix, codeC, compType, hdfsWriter, timedRollerPool,
                idleTimeout, closeCallback,
                lookupPath, callTimeout, callTimeoutPool, retryInterval,
                tryCount);

        return bucketWriter;
    }

    public void start() {
        String timeoutName = "hdfs-" + "asteroidea" + "-call-runner-%d";
        callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
                new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

        String rollerName = "hdfs-" + "asteroidea" + "-roll-timer-%d";
        timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize,
                new ThreadFactoryBuilder().setNameFormat(rollerName).build());


        String closerName = "hdfs-" + "asteroidea" + "-close-timer-%d";
        fileClosePool = Executors.newScheduledThreadPool(fileClosePoolSize,
                new ThreadFactoryBuilder().setNameFormat(closerName).build());

        this.sfWriters = new WriterLinkedHashMap(maxOpenFiles);

        fileClosePool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long time = clock.currentTimeMillis();
                String pathDate = getTime("path", time);
                String timeIndex = pathDate + "_" + getTime("name", time);
                synchronized (sfWritersLock) {
                    for (Entry<String, BucketWriter> entry : sfWriters.entrySet()) {
                        if (!entry.getKey().contains(timeIndex)) {
                            LOG.info("Closing {}", entry.getKey());
                            try {
                                entry.getValue().close();
                            } catch (Exception ex) {
                                LOG.warn("Exception while closing " + entry.getKey() + ". " +
                                        "Exception follows.", ex);
                                if (ex instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                            sfWriters.remove(entry.getKey());
                        }
                    }
                }

            }
        }, defaultFileCloseCheckPeriod, defaultFileCloseCheckPeriod, TimeUnit.SECONDS);

    }

    public void close() {
        this.stop();
    }

    public void stop() {
        // do not constrain close() calls with a timeout
        synchronized (sfWritersLock) {
            for (Entry<String, BucketWriter> entry : sfWriters.entrySet()) {
                LOG.info("Closing {}", entry.getKey());

                try {
                    entry.getValue().close();
                } catch (Exception ex) {
                    LOG.warn("Exception while closing " + entry.getKey() + ". " +
                            "Exception follows.", ex);
                    if (ex instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        // shut down all our thread pools
        ExecutorService[] toShutdown = {callTimeoutPool, timedRollerPool, fileClosePool};
        for (ExecutorService execService : toShutdown) {
            execService.shutdown();
            try {
                while (execService.isTerminated() == false) {
                    execService.awaitTermination(
                            Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                LOG.warn("shutdown interrupted on " + execService, ex);
            }
        }

        callTimeoutPool = null;
        timedRollerPool = null;
        fileClosePool = null;

        synchronized (sfWritersLock) {
            sfWriters.clear();
            sfWriters = null;
        }

        MBeanUtils.unregister("HDFSAdapter", "FlowStatistics_" + topicMeta.getId());

    }


    private class QPSProcessor extends Thread {
        long qpsTmp = 0l;
        long flowSizeTmp = 0l;

        public void run() {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.error("QPSProcessor fail to sleep");
                }
                qps = counter.get() - qpsTmp;
                qpsTmp = counter.get();

                flowSize = totalSize.get() - flowSizeTmp;
                flowSizeTmp = totalSize.get();
            }
        }
    }


}
