package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.content.Context;

import android.support.annotation.Nullable;



public class DBUtils extends SQLiteOpenHelper {


    public static final String DATABASE_NAME = "database";
    public static final String TABLE_NAME = "databasetable";
    public static final String INTERMEDIARY = "intermediary";
    public static final String COL_1 = "key";
    public static final String COL_2 = "value";
    public static final String COL_3 = "belongs_to";
    public static final String COL_4 = "version_control";


    public DBUtils(@Nullable Context context){
        super(context,DATABASE_NAME,null,1);
    }

    @Override
    public void onCreate(SQLiteDatabase db){
        db.execSQL("CREATE TABLE "+TABLE_NAME+"(belongs_to TEXT, version_control TEXT,"+ COL_1 +" TEXT, value TEXT PRIMARY KEY, UNIQUE (key) ON CONFLICT REPLACE)");
        db.execSQL("CREATE TABLE "+ INTERMEDIARY +"( "+ COL_1 +" TEXT, value TEXT PRIMARY KEY, UNIQUE (key) ON CONFLICT REPLACE)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }


}