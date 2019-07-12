package com.popova.avtodoria;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class InsertThread implements Runnable {

    private final static Logger logger = Logger.getLogger(InsertThread.class);
    private BlockingQueue<Long> idsToBeMoved;

    public InsertThread(BlockingQueue<Long> idsToBeMoved) {
        this.idsToBeMoved = idsToBeMoved;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 10; i++) {
                Thread.sleep(50);
                insert();
            }
        } catch (SQLException | InterruptedException e) {
            logger.error("Exception in the thread.", e);
            Thread.currentThread().interrupt();
        }
        logger.info("Insert thread has ended its task.");
    }

    private void insert() throws SQLException {
        List<Long> ids = DatabaseService.insertIntoDatabase();
        idsToBeMoved.addAll(ids);
        logger.info("New data was insered.");
    }
}
