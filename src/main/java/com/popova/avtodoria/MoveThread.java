package com.popova.avtodoria;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class MoveThread implements Runnable {

    private final static Logger logger = Logger.getLogger(MoveThread.class);
    private BlockingQueue<Long> idsToBeMoved;

    public MoveThread(BlockingQueue<Long> idsToBeMoved) {
        this.idsToBeMoved = idsToBeMoved;
    }

    @Override
    public void run() {
        long currentThreadId = Thread.currentThread().getId();
        try {
            int count = 0;
            while (count < 7) {
                if (idsToBeMoved.isEmpty()) {
                    Thread.sleep(500);
                    count++;
                } else {
                    count = 0;
                    move(currentThreadId);
                }
            }
        } catch (InterruptedException | SQLException e) {
            logger.error("Exception in the thread.", e);
            Thread.currentThread().interrupt();
        }
        logger.info(String.format("Thread %s has ended its task.", currentThreadId));
    }

    private void move(long currentThreadId) throws SQLException {
        List<Long> ids = new ArrayList<>();
        idsToBeMoved.drainTo(ids);
        DatabaseService.moveDataInDatabase(ids, currentThreadId);
        logger.info(String.format("Thread %s. Data was moved.", currentThreadId));
    }
}
