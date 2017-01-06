import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Position implements WritableComparable<Position> {

    public IntWritable i;
    public IntWritable j;
    //Default Constructor
    public Position()
    {
        this.i = new IntWritable(0);
        this.j = new IntWritable(0);
    }

    public Position(int _i, int _j)
    {
        this.i = new IntWritable(_i);
        this.j = new IntWritable(_j);
    }


    //Setter method to set the values of WebLogWritable object
    public void set(int _i, int _j)
    {
        this.i.set(_i);
        this.j.set(_j);
    }


    public IntWritable getI()
    {
        return i;
    }
    public IntWritable getJ()
    {
        return j;
    }

    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput in) throws IOException
    {
        i.readFields(in);
        j.readFields(in);
    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException
    {
        i.write(out);
        j.write(out);
    }

    @Override
    public int compareTo(Position o)
    {
        if (i.compareTo(o.i)==0)
        {
            return (j.compareTo(o.j));
        }
        else return (i.compareTo(o.i));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null) return false;
        if (o instanceof Position)
        {
            Position other = (Position) o;
            return i.equals(other.i) && j.equals(other.j);
        }
        return false;
    }
}
