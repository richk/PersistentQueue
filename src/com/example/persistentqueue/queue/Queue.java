package com.example.persistentqueue.queue;

public interface Queue {
	public boolean enqueue(String newString);
	public String dequeue();
	public String peek();
	public QueueItem peekAt(int index);
	public boolean insertAt(int index, String newString);
	public String removeAt(int index);
	public int getSize();
}
