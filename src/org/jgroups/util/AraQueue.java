package org.jgroups.util;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

/**
 * Does not respect ordering guarantees.
 * @param <T>
 */
public class AraQueue<T> extends BundlerQueue<T> {
   private final int length;
   private final int shift;
   private final int maxCycles;
   private final AtomicReferenceArray<T> a;
   private int next;

   public AraQueue() {
      this(1024, 32, 32);
   }

   public AraQueue(int length, int cacheLineSize, int maxCycles) {
      if ((length & (length - 1)) != 0) throw new IllegalArgumentException("Length must be a power of two");
      if ((cacheLineSize & (cacheLineSize - 1)) != 0) throw new IllegalArgumentException("Length must be a power of two");

      this.length = length;
      this.shift = Integer.numberOfTrailingZeros(cacheLineSize);
      this.maxCycles = maxCycles;
      this.a = new AtomicReferenceArray<>(length);
   }

   @Override
   public boolean offer(T t) {
      ThreadLocalRandom rand = ThreadLocalRandom.current();
      for (int cycle = maxCycles; cycle > 0; --cycle) {
         int offset = rand.nextInt(length >> shift) << shift;
         int end = offset + (1 << shift);
         for ( ; offset < end; ++offset) {
            if (a.get(offset) == null) {
               if (a.weakCompareAndSet(offset, null, t)) {
                  return true;
               }
            }
         }
      }
      return false;
   }

   @Override
   public T poll() {
      int offset = next;
      int maxOffset = (offset + (maxCycles << shift) - 1) & (length - 1);
      do {
         T t = a.get(offset);
         if (t != null) {
            if (a.weakCompareAndSet(offset, t, null)) {
               next = (offset + 1) & (length - 1);
               return t;
            }
         }
         offset = (offset + 1) & (length - 1);
      } while (offset != maxOffset);
      next = (offset + 1) & (length - 1);
      return null;
   }

   public int drainTo(Collection<T> c) {
      return drainTo(c::add);
   }

   public int drainTo(Collection<T> c, int maxElements) {
      return drainTo(c::add, maxElements);
   }

   public int drainTo(Consumer<T> c) {
      int offset = next;
      int maxOffset = (offset + (maxCycles << shift)) & (length - 1);
      int added = 0;
      do {
         T t = a.get(offset);
         if (t != null) {
            if (a.weakCompareAndSet(offset, t, null)) {
               c.accept(t);
               ++added;
            }
         }
         offset = (offset + 1) & (length - 1);
      } while (offset != maxOffset);
      next = offset;
      return added;
   }

   /**
    * If maxCycles << shift == length, this is more effective
    */
   public void drainAllTo(Consumer<T> c) {
      for (int offset = 0; offset < length; ++offset) {
         T t = a.get(offset);
         if (t != null) {
            if (a.weakCompareAndSet(offset, t, null)) {
               c.accept(t);
            }
         }
      }
   }

   public int drainTo(Consumer<T> c, int maxElements) {
      int offset = next;
      int maxOffset = (offset + (maxCycles << shift) - 1) & (length - 1);
      int added = 0;
      do {
         T t = a.get(offset);
         if (t != null) {
            if (a.weakCompareAndSet(offset, t, null)) {
               c.accept(t);
               ++added;
               if (added >= maxElements) {
                  next = (offset + 1) & (length - 1);
                  return added;
               }
            }
         }
         offset = (offset + 1) & (length - 1);
      } while (offset != maxOffset);
      next = offset;
      return added;
   }

   /**
    * Provides reliable result only if the queue is not modified.
    */
   @Override
   public int size() {
      int size = 0;
      for (int i = 0; i < a.length(); ++i) {
         if (a.get(i) != null) ++size;
      }
      return size;
   }
}
