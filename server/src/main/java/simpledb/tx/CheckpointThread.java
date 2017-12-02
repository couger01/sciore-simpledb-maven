/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author couger01
 */
public class CheckpointThread implements Runnable {
  private static final long MAXTIME = 10000;
  private static Object checkpointLock = new Object();
  private boolean inProgress = false;
  @Override
  public void run() {
    while (Transaction.getCurrentTransactionsList().size() != 0) {
      synchronized (Transaction.getLock()){
        try {
        checkpointLock.wait();
        bufferManager.flushAll();
        
      } catch (InterruptedException ex) {
          Logger.getLogger(CheckpointThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
  }
}
}
