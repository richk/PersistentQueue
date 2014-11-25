package com.example.persistentqueue.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

import com.example.persistentqueue.queue.db.QueueContracts;
import com.example.persistentqueue.queue.db.QueueDatabaseHelper;

/**
 * This is an implementation of a queue that provides all the APIs specified by Queue.
 * Works by persisting items in the queue in a database.
 * Each item has a pointer to the next item, its successor in the database.
 * @author kricha
 *
 */
public class PersistentQueue implements Queue {
	private static final String LOG_TAG = PersistentQueue.class.getSimpleName();
	private static final String PREFS_FILE_NAME = "MyQueue";
	private static final String QUEUE_PREFS_FRONT_ID_KEY = "front_id";
	private static final String QUEUE_PREFS_TAIL_ID_KEY = "tail_id";
	private static final String QUEUE_PREFS_SIZE_KEY = "size";
	
	private QueueItem mFront = null;
	private QueueItem mTail = null;
	private int mSize = 0;
	private QueueDatabaseHelper mDb;
	private SharedPreferences mPrefs;
	private List<Long> mNextNodeCache = new ArrayList<Long>();
	
	private static PersistentQueue sQueue;
	
	private static AtomicBoolean isInitialized = new AtomicBoolean(false);
	
	private PersistentQueue(Context context) {
		mDb = new QueueDatabaseHelper(context);
		mPrefs = context.getSharedPreferences(PREFS_FILE_NAME, Context.MODE_PRIVATE);
		initQueue();
		buildNextNodeCache();
	}
	
	public static PersistentQueue getInstance(Context context) {
		if (sQueue == null) {
			if (isInitialized.compareAndSet(false, true)) {
				sQueue = new PersistentQueue(context);
			}
		}
	    return sQueue;
	}
	
	private void initQueue() {
		SQLiteDatabase db = null;
		try {
			db = mDb.openDatabase();
			long frontId = mPrefs.getLong(QUEUE_PREFS_FRONT_ID_KEY, -1);
			if (frontId > 0) {
				mFront = getItem(frontId, db);
				Log.i(LOG_TAG, "Retrieved queue front, id=" + mFront.id);
			}
			long tailId = mPrefs.getLong(QUEUE_PREFS_TAIL_ID_KEY, -1);
			if (tailId > 0) {
				mTail = getItem(tailId, db);
				Log.i(LOG_TAG, "Retrieved queue tail, id=" + mTail.id);
			}
			mSize = mPrefs.getInt(QUEUE_PREFS_SIZE_KEY, 0);
			Log.i(LOG_TAG, "Retrieved queue size, size=" + mSize);
		} finally {
			if (db != null) {
				mDb.closeDatabase();
			}
		}
	}

	private void buildNextNodeCache() {
		if (mFront != null && mTail != null) {
            QueueItem current = mFront;
            SQLiteDatabase db = null;
            try {
            	db = mDb.openDatabase();
            	while(current.next > 0) {
            		mNextNodeCache.add(current.id);
            		current = getItem(current.next, db);
            	}
            	mNextNodeCache.add(current.id);
            } finally {
            	if (db != null) {
            		mDb.closeDatabase();
            	}
            }
            printCacheToLog();
            if (mSize != mNextNodeCache.size()) {
            	throw new IllegalStateException("Queue Corrupted");
            }
		}
	}

	/**
	 * This method accepts an item and inserts it as the last item to be fetched on dequeue
	 * Details : If the queue is empty, front and tail pointers should be updated to point to this object.
	 *           Else, tail should point to the new object.
	 * @return 
	 */
	@Override
	public synchronized boolean enqueue(String newString) {
		Log.i(LOG_TAG, "Enqueue:" + newString);
		if (newString == null || newString.isEmpty()) {
			return false;
		}
		SQLiteDatabase db = null;
		try {
			db = mDb.openDatabase();
			db.beginTransaction();
			if (mFront == null) {
				mFront = new QueueItem(newString);
				mFront.next = -1;
				mFront.id = insertItem(mFront, db);
				mTail = mFront;
				insertNext(mFront, db);
			} else {
				QueueItem newItem = new QueueItem(newString);
				newItem.id = insertItem(newItem, db);
				mTail.next = newItem.id;
				newItem.next = -1;
				insertNext(newItem, db);
				updateNext(mTail, db);
				mTail = newItem;
			}
			db.setTransactionSuccessful();
			++mSize;
			mNextNodeCache.add(mTail.id);
			Log.i(LOG_TAG, "New front=" + (mFront!=null?mFront.id:-1));
			Log.i(LOG_TAG, "New tail=" + (mTail!=null?mTail.id:-1));
			Log.i(LOG_TAG, "New size=" + mSize);
			updateSharedPrefs();
			printCacheToLog();
			return true;
		} finally {
			if (db != null) {
				if (db.inTransaction()) {
				    db.endTransaction();
				}
				mDb.closeDatabase();
			}
		}
	}
	
	@Override
	public synchronized String dequeue() {
		Log.i(LOG_TAG, "Dequeue:"+(mFront!=null?mFront.id:-1));
		if (mFront == null) {
			return null;
		}
		SQLiteDatabase db = null;
		try {
			db = mDb.openDatabase();
			db.beginTransaction();
			QueueItem newFront = getItem(mFront.next, db);
			Log.i(LOG_TAG, "Next front=" + (newFront!=null?newFront.data:-1));
			QueueItem front = mFront;
			deleteItem(front, db);
			mFront = newFront;
			if (mFront == null) {
				mTail = null;
			}
			mNextNodeCache.remove(0);
			db.setTransactionSuccessful();
			Log.i(LOG_TAG, "Updating queue size on dequeue");
			--mSize;
			Log.i(LOG_TAG, "New front=" + (mFront!=null?mFront.id:-1));
			Log.i(LOG_TAG, "New tail=" + (mTail!=null?mTail.id:-1));
			Log.i(LOG_TAG, "New size=" + mSize);
			printCacheToLog();
			updateSharedPrefs();
			return front.data;
		} finally {
			if (db != null) {
				if (db.inTransaction()) {
				    db.endTransaction();
				}
				mDb.closeDatabase();
			}
		}
	}
	
	@Override
	public synchronized String peek() {
		if (mFront != null) {
		    return mFront.data;
		} else {
			return null;
		}
	}
	
	@Override
	public synchronized QueueItem peekAt(int index) {
		if (mFront == null) {
		    return null;
		} 
		if (index > (mSize-1)) {
			return null;
		} else {
			long idAtIndex = mNextNodeCache.get(index);
			SQLiteDatabase db = null;
			try {
				db = mDb.openDatabase();
				QueueItem current = getItem(idAtIndex, db);
				return current;
			} finally {
				if (db != null) {
					mDb.closeDatabase();
				}
			}
		}    
	}
	
	@Override
	public synchronized boolean insertAt(int index, String newString) {
		if (index < 0) {
			return false;
		}
	    if (index > mSize-1) {
	        return false;
	    } else if (index == (mSize-1)) {
	    	return enqueue(newString);
	    } else {
	    	SQLiteDatabase db = null;
	    	try {
	    		db = mDb.openDatabase();	
	    		db.beginTransaction();
	    		QueueItem current = null;
	    		if (index > 0) {
	    		    current = getItem(mNextNodeCache.get(index-1), db);
	    		}
	    		if (current == null && index > 0) {
	    			return false;
	    		}
	    		QueueItem next = getItem(mNextNodeCache.get(index), db);
	    		QueueItem newItem = new QueueItem(newString);
	    		newItem.id = insertItem(newItem, db);
	    		if (current != null) {
	    		    current.next = newItem.id;
	    		    updateNext(current, db);
	    		}
	    		newItem.next = next.id;
	    		insertNext(newItem, db);
	    		mNextNodeCache.add(index, newItem.id);
	    		++mSize;
	    		db.setTransactionSuccessful();
	    		updateSharedPrefs();
	    		Log.i(LOG_TAG, "New front=" + (mFront!=null?mFront.id:-1));
	    		Log.i(LOG_TAG, "New tail=" + (mTail!=null?mTail.id:-1));
	    		Log.i(LOG_TAG, "New size=" + mSize);
	    		printCacheToLog();
	    		return true;
	    	} finally {
	    		if (db != null) {
	    			if (db.inTransaction()) {
					    db.endTransaction();
					}
	    			mDb.closeDatabase();
	    		}
	    	}
	    }
	}
	
	@Override
	public synchronized String removeAt(int index) {
		Log.i(LOG_TAG, "removeAt index:" + index);
	    if (index > mSize-1) {
	        return null;
	    } else if (index == 0) {
	    	return dequeue();
	    } else {
	    	SQLiteDatabase db = null;
	    	try {
	    		db = mDb.openDatabase();
	    		db.beginTransaction();
	    		QueueItem current = getItem(mNextNodeCache.get(index-1), db);
	    		if (current == null) {
	    			return null;
	    		}
	    		QueueItem next = getItem(mNextNodeCache.get(index), db);
	    		QueueItem newNext = getItem(next.next,db);
	    		if (newNext != null) {
	    			current.next = newNext.id;
	    		} else {
	    			current.next = -1;
	    		}
	    		deleteItem(next, db);
	    		updateNext(current, db);
	    		--mSize;
	    		mNextNodeCache.remove(index);
	    		db.setTransactionSuccessful();
	    		updateSharedPrefs();
	    		Log.i(LOG_TAG, "New front=" + (mFront!=null?mFront.id:-1));
	    		Log.i(LOG_TAG, "New tail=" + (mTail!=null?mTail.id:-1));
	    		Log.i(LOG_TAG, "New size=" + mSize);
	    		printCacheToLog();
	    		return next.data;
	    	} finally {
	    		if (db != null) {
	    			if (db.inTransaction()) {
					    db.endTransaction();
					}
	    			mDb.closeDatabase();
	    		}
	    	}
	    }
	}
	
	@Override
	public synchronized int getSize() {
	    return mSize;
	}

	private synchronized void deleteItem(QueueItem item, SQLiteDatabase db) {
		Log.i(LOG_TAG, "deleteItem with id=" + item.id);
		db.delete(QueueContracts.QUEUE_ITEM_TABLE_NAME, QueueContracts.QUEUE_ITEM_ID_COL_NAME + "=?", 
				new String[]{String.valueOf(item.id)});
		db.delete(QueueContracts.NEXT_TABLE_NAME, QueueContracts.NEXT_PARENT_COL_NAME + "=?", 
				new String[]{String.valueOf(item.id)});
	}
	
	private synchronized void updateNext(QueueItem item, SQLiteDatabase db) {
		Log.i(LOG_TAG, "updateNext. Parent=" + item.id + ", next=" + item.next);
		ContentValues cv = new ContentValues();
		cv.put(QueueContracts.NEXT_PARENT_COL_NAME, item.id);
		cv.put(QueueContracts.NEXT_NEXT_COL_NAME, item.next);
		long id = db.update(QueueContracts.NEXT_TABLE_NAME, cv, "next.parent=?", new String[]{String.valueOf(item.id)});
		Log.i(LOG_TAG, "Next node updated. Id = " + id);
	}
	
	private synchronized long insertItem(QueueItem newItem, SQLiteDatabase db) {
		ContentValues cv = new ContentValues();
		cv.put(QueueContracts.QUEUE_ITEM_DATA_COL_NAME, newItem.data);
		long itemId = db.insert(QueueContracts.QUEUE_ITEM_TABLE_NAME,null, cv);
		Log.i(LOG_TAG, "Inserted item with id=" + itemId);
		return itemId;
	}
	
	private synchronized void insertNext(QueueItem item, SQLiteDatabase db) {
		Log.i(LOG_TAG, "insertNext. Parent=" + item.id + ", next=" + item.next);
		ContentValues cv = new ContentValues();
		cv.put(QueueContracts.NEXT_PARENT_COL_NAME, item.id);
		cv.put(QueueContracts.NEXT_NEXT_COL_NAME, item.next);
		long id = db.insert(QueueContracts.NEXT_TABLE_NAME, null, cv);
		Log.i(LOG_TAG, "Inserted next node:" + id);
	}
	
	private synchronized QueueItem getItem(long itemIndex, SQLiteDatabase db) {
		Log.i(LOG_TAG, "getItem at index:" + itemIndex);
		Cursor cursor = null;
		try {
			cursor = db.rawQuery("select queue_item._id,queue_item.data,next.next from queue_item,next where queue_item._id="+ itemIndex +" and queue_item._id=next.parent;", null);
			if (cursor.moveToNext()) {
				int idIndex = cursor.getColumnIndex(QueueContracts.QUEUE_ITEM_ID_COL_NAME);
				int dataIndex = cursor.getColumnIndex(QueueContracts.QUEUE_ITEM_DATA_COL_NAME);
				int nextIndex = cursor.getColumnIndex(QueueContracts.NEXT_NEXT_COL_NAME);
				long id = cursor.getInt(idIndex);
				String nextData = cursor.getString(dataIndex);
				long nextItemId = cursor.getLong(nextIndex);
				QueueItem item = new QueueItem(nextData);
				item.id = id;
				item.data = nextData;
				item.next = nextItemId;
				Log.i(LOG_TAG, "item at index:" + item.id);
				return item;
			} else {
				return null;
			}
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}
	
	private synchronized void updateSharedPrefs() {
		SharedPreferences.Editor editor = mPrefs.edit();
		long frontid = -1;
		if (mFront != null) {
			frontid = mFront.id;
		}
		long tailid = -1;
		if (mTail != null) {
			tailid = mTail.id;
		}
		editor.putLong(QUEUE_PREFS_FRONT_ID_KEY, frontid);
		editor.putLong(QUEUE_PREFS_TAIL_ID_KEY, tailid);
		editor.putInt(QUEUE_PREFS_SIZE_KEY, mSize);
		editor.commit();
	}
	
	private void printCacheToLog() {
		Log.d(LOG_TAG, "Cache Items");
		for (long id:mNextNodeCache) {
		    Log.d(LOG_TAG, id + "->");
		}
	}
}
