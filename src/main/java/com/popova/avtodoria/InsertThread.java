package com.popova.avtodoria;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class InsertThread implements Runnable {

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
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        System.out.println("Insert thread has ended its task.");
    }

    private void insert() throws SQLException {
        List<Long> ids = DatabaseService.insertIntoDatabase();
        idsToBeMoved.addAll(ids);
        System.out.println("New data was insered.");
    }
}
