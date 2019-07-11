package com.popova.avtodoria;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class MoveThread implements Runnable {

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
//                    System.out.printf("Thread %s. No data to move. Sleeping.\n", currentThreadId);
                } else {
                    count = 0;
                    move(currentThreadId);
                }
            }
        } catch (InterruptedException | SQLException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        System.out.printf("Thread %s has ended its task.\n", currentThreadId);
    }

    private void move(long currentThreadId) throws SQLException {
        List<Long> ids = new ArrayList<>();
        idsToBeMoved.drainTo(ids);
        DatabaseService.moveDataInDatabase(ids, currentThreadId);
        System.out.printf("Thread %s. Data was moved.\n", currentThreadId);
    }
}
