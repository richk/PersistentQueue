package com.example.persistentqueue.queue;

import java.util.ArrayList;
import java.util.List;

import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

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
	
	public PersistentQueue(Context context) {
		mDb = new QueueDatabaseHelper(context);
		mPrefs = context.getSharedPreferences(PREFS_FILE_NAME, Context.MODE_PRIVATE);
		initQueue();
		buildNextNodeCache();
	}
	
	private void initQueue() {
        long frontId = mPrefs.getLong(QUEUE_PREFS_FRONT_ID_KEY, -1);
        if (frontId > 0) {
        	mFront = getItem(frontId);
        }
        long tailId = mPrefs.getLong(QUEUE_PREFS_TAIL_ID_KEY, -1);
        if (tailId > 0) {
        	mTail = getItem(tailId);
        }
    	if (mFront == null || mTail == null) {
    		throw new IllegalStateException("Could not initialize the queue. Check to make sure shared prefs is not corruped");
    	}
        mSize = mPrefs.getInt(QUEUE_PREFS_SIZE_KEY, 0);
	}

	private void buildNextNodeCache() {
		if (mFront != null && mTail != null) {
            QueueItem current = mFront;
            SQLiteDatabase db = null;
            try {
            	db = mDb.getReadableDatabase();
            	while(current.next > 0) {
            		mNextNodeCache.add(current.id);
            		current = getNext(current, db);
            	}
            } finally {
            	if (db != null) {
            		db.close();
            	}
            }
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
		if (newString == null || newString.isEmpty()) {
			return false;
		}
		if (mFront == null) {
	        mFront = new QueueItem(newString);
	        mFront.next = -1;
	        mFront.id = insertItem(mFront);
	        mTail = mFront;
	        insertNext(mFront);
	    } else {
	        QueueItem newItem = new QueueItem(newString);
	        newItem.id = insertItem(newItem);
	        mTail.next = newItem.id;
	        updateNext(mTail);
	        mTail = newItem;
	    }
	    mSize++;
	    mNextNodeCache.add(mTail.id);
	    updateSharedPrefs();
	    return true;
	}
	
	@Override
	public synchronized String dequeue() {
		QueueItem newFront = getNext(mFront, null);
		QueueItem front = mFront;
	    deleteItem(front);
	    mFront = newFront;
	    mNextNodeCache.remove(0);
	    mSize--;
	    updateSharedPrefs();
	    return front.data;
	}
	
	@Override
	public synchronized String peek() {
		return mFront.data;
	}
	
	@Override
	public synchronized QueueItem peekAt(int index) {
		int i = 0;
		if (index > (mSize-1)) {
			return null;
		} else {
			long idAtIndex = mNextNodeCache.get(index);
			QueueItem current = getItem(idAtIndex);
			return current;
		}    
	}
	
	@Override
	public synchronized boolean insertAt(int index, String newString) {
	    if (index > mSize-1) {
	        return false;
	    } else if (index == (mSize-1)) {
	    	return enqueue(newString);
	    } else {
	        QueueItem current = peekAt(index-1);
	        if (current == null) {
	        	return false;
	        }
	        QueueItem next = peekAt(index);
	        QueueItem newItem = new QueueItem(newString);
	        current.next = insertItem(newItem);
	        insertNext(newItem);
	        newItem.next = next.id;
	        updateNext(current);
	        mSize++;
	        updateSharedPrefs();
	        return true;
	    }
	}
	
	@Override
	public synchronized String removeAt(int index) {
	    if (index > mSize-1) {
	        return null;
	    } else if (index == 0) {
	    	return dequeue();
	    } else {
	        QueueItem current = peekAt(index-1);
	        if (current == null) {
	        	return null;
	        }
	        QueueItem next = peekAt(index);
	        QueueItem newNext = getNext(next,null);
	        current.next = newNext.id;
	        deleteItem(next);
	        updateNext(current);
	        mSize--;
	        updateSharedPrefs();
	        return next.data;
	    }
	}
	
	@Override
	public synchronized int getSize() {
	    return mSize;
	}

	private synchronized QueueItem getNext(QueueItem currentItem, SQLiteDatabase db) {
		boolean closeDb = (db==null);
		try {
			if (db != null) {
			    db = mDb.getReadableDatabase();
			}
			// First read the next table to find the id of the item next to the current one
			Cursor cursor = db.rawQuery("select queue_item._id,queue_item.data from queue_item, next where next.id="+currentItem.id +"and queue_item._id=next.next;", null);
			if (cursor.moveToNext()) {
				int idIndex = cursor.getColumnIndex(QueueContracts.QUEUE_ITEM_ID_COL_NAME);
				int dataIndex = cursor.getColumnIndex(QueueContracts.QUEUE_ITEM_DATA_COL_NAME);
				int id = cursor.getInt(idIndex);
				String nextData = cursor.getString(dataIndex);
				QueueItem nextItem = new QueueItem(nextData);
				nextItem.id = id;
				return nextItem;
			} else {
				return null;
			}
		} finally {
			if (db != null && closeDb) {
				db.close();
			}
		}
	}

	private synchronized void deleteItem(QueueItem item) {
		SQLiteDatabase db = null;
		try {
		    db = mDb.getWritableDatabase();
		    db.delete(QueueContracts.QUEUE_ITEM_TABLE_NAME, QueueContracts.QUEUE_ITEM_ID_COL_NAME + "=?", 
		    		new String[]{String.valueOf(item.id)});
		    db.delete(QueueContracts.NEXT_TABLE_NAME, QueueContracts.NEXT_ID_COL_NAME + "=?", 
		    		new String[]{String.valueOf(item.id)});
		} finally {
			if (db != null) {
				db.close();
			}
		}	
	}
	
	private synchronized void updateNext(QueueItem item) {
		SQLiteDatabase db = null;
		try {
		    db = mDb.getWritableDatabase();
		    ContentValues cv = new ContentValues();
		    cv.put(QueueContracts.NEXT_ID_COL_NAME, item.id);
		    cv.put(QueueContracts.NEXT_NEXT_COL_NAME, item.next);
		    db.update(QueueContracts.QUEUE_ITEM_TABLE_NAME, cv, "item._id=?", new String[]{String.valueOf(item.id)});
		} finally {
			if (db != null) {
				db.close();
			}
		}
	}
	
	private synchronized long insertItem(QueueItem newItem) {
		ContentValues cv = new ContentValues();
	    cv.put(QueueContracts.QUEUE_ITEM_DATA_COL_NAME, newItem.data);
	    SQLiteDatabase db = null;
	    try {
	    db = mDb.getWritableDatabase();
	    long itemId = db.insert(QueueContracts.QUEUE_ITEM_TABLE_NAME,null, cv);
	    return itemId;
	    } finally {
	    	if (db != null) {
	    		db.close();	    		
	    	}
	    }
	}
	
	private synchronized void insertNext(QueueItem item) {
		SQLiteDatabase db = null;
		try {
		    db = mDb.getWritableDatabase();
		    ContentValues cv = new ContentValues();
		    cv.put(QueueContracts.NEXT_ID_COL_NAME, item.id);
		    cv.put(QueueContracts.NEXT_NEXT_COL_NAME, item.next);
		    db.insert(QueueContracts.QUEUE_ITEM_TABLE_NAME, null, cv);
		} finally {
			if (db != null) {
				db.close();
			}
		}	
	}
	
	private synchronized QueueItem getItem(long itemIndex) {
		SQLiteDatabase db = null;
		try {
			db = mDb.getReadableDatabase();
			Cursor cursor = db.rawQuery("select queue_item._id,queue_item.data,next.next from queue_item,next where queue_item.id="+ itemIndex +"and queue_item._id=next.id;", null);
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
				return item;
			} else {
				return null;
			}
		} finally {
			if (db != null) {
				db.close();
			}
		}	
	}
	
	private synchronized void updateSharedPrefs() {
		SharedPreferences.Editor editor = mPrefs.edit();
		editor.putLong(QUEUE_PREFS_FRONT_ID_KEY, mFront.id);
		editor.putLong(QUEUE_PREFS_TAIL_ID_KEY, mTail.id);
		editor.putInt(QUEUE_PREFS_SIZE_KEY, mSize);
		editor.commit();
	}
}
