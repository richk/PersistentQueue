package com.example.persistentqueue.queue.db;

public class QueueContracts {
	
	public static final String DATABASE_NAME = "queue.db";
	public static final int DATABASE_VERSION = 1;
	
	public static final String QUEUE_TABLE_NAME = "queue_metadata";
	public static final String QUEUE_ID_COL_NAME = "_id";
	public static final String QUEUE_TAG_COL_NAME = "tag";
	public static final String QUEUE_FRONT_COL_NAME = "queue_front";
	public static final String QUEUE_TAIL_COL_NAME = "queue_tail";
	public static final String QUEUE_SIZE_COL_NAME = "size";
	
	public static final String QUEUE_ITEM_TABLE_NAME = "queue_item";
	public static final String QUEUE_ITEM_ID_COL_NAME = "_id";
	public static final String QUEUE_ITEM_DATA_COL_NAME = "data";
	public static final String QUEUE_ITEM_QUEUE_ID_COL_NAME = "queue_id";
	
	public static final String NEXT_TABLE_NAME = "next";
	public static final String NEXT_ID_COL_NAME = "_id";
	public static final String NEXT_PARENT_COL_NAME = "parent";
	public static final String NEXT_NEXT_COL_NAME = "next";
	public static final String NEXT_QUEUE_ID_COL_NAME = "queue_id";
	
	public static final String QUEUE_CREATE_TABLE = "create table "
			+ QUEUE_TABLE_NAME 
			+ "(" 
		    + QUEUE_ID_COL_NAME + " integer primary key autoincrement, " 
		    + QUEUE_TAG_COL_NAME + " text not null, " 
		    + QUEUE_FRONT_COL_NAME + " long not null," 
		    + QUEUE_TAIL_COL_NAME + " long not null,"
		    + QUEUE_SIZE_COL_NAME + " integer not null,"
		    + ");";
	
	public static final String QUEUE_ITEM_CREATE_TABLE = "create table "
			+ QUEUE_ITEM_TABLE_NAME 
			+ "(" 
		    + QUEUE_ITEM_ID_COL_NAME + " integer primary key autoincrement, " 
		    + QUEUE_ITEM_DATA_COL_NAME + " text not null, " 
		    + QUEUE_ITEM_QUEUE_ID_COL_NAME + " long not null,"
		    + ");";
	
	public static final String QUEUE_ITEM_DROP_TABLE = "drop table "
			+ QUEUE_ITEM_TABLE_NAME;
	
	public static final String NEXT_CREATE_TABLE = "create table "
			+ NEXT_TABLE_NAME 
			+ "(" 
		    + NEXT_ID_COL_NAME + " integer primary key autoincrement, " 
		    + NEXT_PARENT_COL_NAME + " long not null, " 
		    + NEXT_NEXT_COL_NAME + " long not null,"
		    + NEXT_QUEUE_ID_COL_NAME + " long not null,"
		    + "FOREIGN KEY("+NEXT_PARENT_COL_NAME+") REFERENCES "+QUEUE_ITEM_TABLE_NAME+"("+QUEUE_ITEM_ID_COL_NAME+"),"
		    + ");";
	
	public static final String NEXT_DROP_TABLE = "drop table "
			+ NEXT_TABLE_NAME;
}
