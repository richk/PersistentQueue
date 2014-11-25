package com.example.persistentqueue.queue.db;

import java.util.concurrent.atomic.AtomicInteger;

import com.example.persistentqueue.queue.Queue;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDatabase.CursorFactory;
import android.database.sqlite.SQLiteOpenHelper;

public class QueueDatabaseHelper extends SQLiteOpenHelper {
	
	private AtomicInteger mRefCount = new AtomicInteger();
	private SQLiteDatabase mDatabase;

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
	
	public SQLiteDatabase getRefCountedReadableDatabase() {
        return openDatabase();
    }

    public SQLiteDatabase getRefCountedWritableDatabase() {
        return openDatabase();
    }

    public synchronized SQLiteDatabase openDatabase() {
        if (mRefCount.get() == 0) {
            mDatabase = getWritableDatabase(); // open database
        }
        mRefCount.incrementAndGet();
        return mDatabase;
    }

    public synchronized boolean closeDatabase() {
        boolean result = true;
        if (mRefCount.get() > 0) {
            int openRef = mRefCount.decrementAndGet();
            if (openRef == 0) {
                try {
                    mDatabase.close(); // close database
                } catch (Throwable t) {
                    result = false;
                }
            }
        }
        return result;
    }
}
