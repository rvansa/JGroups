package org.jgroups.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

public abstract class BundlerQueue<T> implements Queue<T> {
   @Override
   public boolean isEmpty() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean contains(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Iterator<T> iterator() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object[] toArray() {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T1> T1[] toArray(T1[] a) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean add(T t) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
   }

   @Override
   public T remove() {
      throw new UnsupportedOperationException();
   }

   @Override
   public T element() {
      throw new UnsupportedOperationException();
   }

   @Override
   public T peek() {
      throw new UnsupportedOperationException();
   }
}
