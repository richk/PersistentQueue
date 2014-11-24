package com.example.persistentqueue.queue.db;

import java.util.concurrent.atomic.AtomicInteger;

import com.example.persistentqueue.queue.Queue;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDatabase.CursorFactory;
import android.database.sqlite.SQLiteOpenHelper;

public class QueueDatabaseHelper extends SQLiteOpenHelper {
	
	private AtomicInteger mRefCount = new AtomicInteger();

	public QueueDatabaseHelper(Context context) {
		super(context, QueueContracts.DATABASE_NAME, null, QueueContracts.DATABASE_VERSION);
	}

	@Override
	public void onCreate(SQLiteDatabase db) {
		db.execSQL(QueueContracts.QUEUE_ITEM_CREATE_TABLE);
		db.execSQL(QueueContracts.NEXT_CREATE_TABLE);
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		db.execSQL(QueueContracts.QUEUE_ITEM_DROP_TABLE);
		db.execSQL(QueueContracts.NEXT_DROP_TABLE);
		db.execSQL(QueueContracts.QUEUE_ITEM_CREATE_TABLE);
		db.execSQL(QueueContracts.NEXT_CREATE_TABLE);
	}

	@Override
	public SQLiteDatabase getWritableDatabase() {
		SQLiteDatabase db = super.getWritableDatabase();
		mRefCount.incrementAndGet();
		return db;
	}
	
	

	@Override
	public SQLiteDatabase getReadableDatabase() {
		SQLiteDatabase db = super.getReadableDatabase();
		mRefCount.incrementAndGet();
		return db;
	}

	@Override
	public synchronized void close() {
		if (mRefCount.get() > 0) {
			mRefCount.decrementAndGet();
		} else {
		    super.close();
		}
	}
}
