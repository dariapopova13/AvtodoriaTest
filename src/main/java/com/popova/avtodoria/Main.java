package com.popova.avtodoria;

import org.apache.log4j.Logger;

import java.util.concurrent.*;

public class Main {

    public static final int POOL_SIZE = 6;
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            createThreadPool();
        } catch (InterruptedException e) {
            logger.error("Error while creating a thread pool.", e);
        }

        DatabaseService.printStatistics();
        DatabaseService.clearTables();
    }

    private static void createThreadPool() throws InterruptedException {
        BlockingQueue<Long> idsToBeMoved = new ArrayBlockingQueue<>(100);

        ExecutorService service = Executors.newFixedThreadPool(POOL_SIZE);
        service.execute(new InsertThread(idsToBeMoved));
        for (int i = 0; i < POOL_SIZE - 1; i++) {
            service.execute(new MoveThread(idsToBeMoved));
        }

        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
}
