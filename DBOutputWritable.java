import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable{  //this is a interface
    //user movie0 movie1 movie2 movie3 movie4 movie5 movie6 movie7 movie8 movie9
    //Define the instance variable
    private String user;
    private String movie0;
    private String movie1;
    private String movie2;
    private String movie3;
    private String movie4;
    private String movie5;
    private String movie6;
    private String movie7;
    private String movie8;
    private String movie9;

    public DBOutputWritable(String user, String[] movies) { //Initiation
        this.user=user;
        this.movie0= movies[0];
        this.movie1= movies[1];
        this.movie2= movies[2];
        this.movie3= movies[3];
        this.movie4= movies[4];
        this.movie5= movies[5];
        this.movie6= movies[6];
        this.movie7= movies[7];
        this.movie8= movies[8];
        this.movie9= movies[9];
    }

    public void readFields(ResultSet arg0) throws SQLException { //ResultSet
        //user ResultSet Object
        this.user= arg0.getString(1);   //start from 1;
        this.movie0=arg0.getString(2);
        this.movie1=arg0.getString(3);
        this.movie2=arg0.getString(4);
        this.movie3=arg0.getString(5);
        this.movie4=arg0.getString(6);
        this.movie5=arg0.getString(7);
        this.movie6=arg0.getString(8);
        this.movie7=arg0.getString(9);
        this.movie8=arg0.getString(10);
        this.movie9=arg0.getString(11);


        //this.starting_phrase = arg0.getString(1);
        //this.following_word = arg0.getString(2);
        //this.count = arg0.getInt(3);

    }

    public void write(PreparedStatement arg0) throws SQLException {
        arg0.setString(1, user);
        arg0.setString(2, movie0);
        arg0.setString(3, movie1);
        arg0.setString(4, movie2);
        arg0.setString(5, movie3);
        arg0.setString(6, movie4);
        arg0.setString(7, movie5);
        arg0.setString(8, movie6);
        arg0.setString(9, movie7);
        arg0.setString(10, movie8);
        arg0.setString(11, movie9);


        //arg0.setString(1, starting_phrase);
        //arg0.setString(2, following_word);
        //arg0.setInt(3, count);

    }

}
