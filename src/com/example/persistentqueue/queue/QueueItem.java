package com.example.persistentqueue.queue;

public class QueueItem {
	public long id;
	public String data;
	public long next;
	
	public QueueItem(String str) {
		data = str;
	}
}
