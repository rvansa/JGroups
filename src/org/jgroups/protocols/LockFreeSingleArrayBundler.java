package org.jgroups.protocols;

import java.net.SocketException;
import java.util.concurrent.locks.LockSupport;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.util.AraQueue;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

public class LockFreeSingleArrayBundler implements Bundler, Runnable {
   protected static final int MAX_MESSAGES = 512;
   protected Log log;
   protected TP transport;
   protected ByteArrayDataOutputStream output;
   protected Thread thread;
   protected volatile boolean running;
   protected AraQueue<Message> queue = new AraQueue<>();
   protected final Message[] messages = new Message[MAX_MESSAGES];
   protected int curr, numMessages;

   @Override
   public void init(TP transport) {
      this.transport=transport;
      this.log = transport.getLog();
      output=new ByteArrayDataOutputStream(transport.getMaxBundleSize() + TP.MSG_OVERHEAD);
   }

   @Override
   public void start() {
      thread = new Thread(this, "LockfreeBunlder");
      running = true;
      thread.start();
   }

   @Override
   public void stop() {
      running = false;
      thread.interrupt();
   }

   @Override
   public void send(Message msg) throws Exception {
      while (!queue.offer(msg)) LockSupport.parkNanos(1); // or just drop it?
   }

   @Override
   public int size() {
      return queue.size();
   }

   @Override
   public void run() {
      while (running) {
         queue.drainAllTo(this::addMessage);
         sendBundledMessages();
      }
   }

   private void addMessage(Message msg) {
      int start = curr;
      boolean notOverflow;
      while (notOverflow = curr < MAX_MESSAGES && messages[curr] != null) ++curr;
      if (notOverflow) {
         messages[curr]=msg;
         ++curr;
         ++numMessages;
         return;
      }
      curr = 0;
      while (notOverflow = curr < start && messages[curr] != null) ++curr;
      if (notOverflow) {
         messages[curr]=msg;
         ++curr;
         ++numMessages;
         return;
      }
      sendBundledMessages();
      messages[0] = msg;
      curr = 1;
      numMessages = 1;
   }

   private void sendBundledMessages() {
      int start=0;
      for(;;) {
         while (start < MAX_MESSAGES && messages[start] == null) ++start;
         if(start >= MAX_MESSAGES) {
            curr = 0;
            numMessages = 0;
            return;
         }
         Address dest= messages[start].getDest();
         int numMsgs=1;
         for(int i = start + 1; i < MAX_MESSAGES; ++i) {
            Message msg= messages[i];
            if(msg != null && (dest == msg.getDest() || (dest != null && dest.equals(msg.getDest())))) {
               msg.setDest(dest); // avoid further equals() calls
               numMsgs++;
            }
         }
         try {
            output.position(0);
            if(numMsgs == 1) {
               sendSingleMessage(messages[start]);
               messages[start]=null;
            }
            else {
               Util.writeMessageListHeader(dest, messages[start].getSrc(), transport.cluster_name.chars(), numMsgs, output, dest == null);
               for(int i = start; i < MAX_MESSAGES; ++i) {
                  Message msg= messages[i];
                  // since we assigned the matching destination we can do plain ==
                  if(msg != null && msg.getDest() == dest) {
                     msg.writeToNoAddrs(msg.getSrc(), output, transport.getId());
                     messages[i]=null;
                  }
               }
               transport.doSend(output.buffer(), 0, output.position(), dest);
            }
            start++;
         }
         catch (Exception e) {
            log.error("Failed to send message", e);
         }
      }
   }

   private void sendSingleMessage(final Message msg) {
      Address dest=msg.getDest();
      try {
         Util.writeMessage(msg, output, dest == null);
         transport.doSend(output.buffer(), 0, output.position(), dest);
         if (transport.statsEnabled())
            transport.incrSingleMsgsInsteadOfBatches();
      }
      catch(SocketException sock_ex) {
         log.trace(Util.getMessage("SendFailure"),
               transport.localAddress(), (dest == null? "cluster" : dest), msg.size(), sock_ex.toString(), msg.printHeaders());
      }
      catch(Throwable e) {
         log.error(Util.getMessage("SendFailure"),
               transport.localAddress(), (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
      }
   }

}
