package com.example.persistentqueue;

import com.example.persistentqueue.queue.PersistentQueue;
import com.example.persistentqueue.queue.Queue;

import android.app.Activity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class MainActivity extends Activity {
	
	private EditText etEnqueue;
	private TextView tvDequeue;
	private Button btEnqueue;
	private Button btDequeue;
	private EditText etEnqueueIndex;
	private EditText etDequeueIndex;
	
	private Queue mQueue;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);
		mQueue = PersistentQueue.getInstance(this);
		etEnqueue = (EditText)findViewById(R.id.editText1);
		tvDequeue = (TextView)findViewById(R.id.textView1);
		btEnqueue = (Button)findViewById(R.id.button1);
		btDequeue = (Button)findViewById(R.id.button2);
		etEnqueueIndex = (EditText)findViewById(R.id.editText3);
		etDequeueIndex = (EditText)findViewById(R.id.editText2);
		
		btEnqueue.setOnClickListener(new OnClickListener() {
			
			@Override
			public void onClick(View v) {
				if (!etEnqueueIndex.getText().toString().isEmpty()) {
				    int i = Integer.valueOf(etEnqueueIndex.getText().toString());
				    mQueue.insertAt(i, etEnqueue.getText().toString());
				} else {
				    mQueue.enqueue(etEnqueue.getText().toString());
				}
			}
		});
		
		btDequeue.setOnClickListener(new OnClickListener() {
			
			@Override
			public void onClick(View v) {
				if (!etDequeueIndex.getText().toString().isEmpty()) {
				    int i = Integer.valueOf(etDequeueIndex.getText().toString());
				    String str = mQueue.removeAt(i);
				    tvDequeue.setText(str);
				} else {
					String str = mQueue.dequeue();
					tvDequeue.setText(str);
				}
			}
		});
	}
}
