import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
//import Java.lang.Character;

public class Key implements WritableComparable<Key>
{

    public IntWritable M;
//    public IntWritable i;
//    public IntWritable j;
    public Position pos;
    public IntWritable n;
    //Default Constructor
    public Key()
    {
        this.pos = new Position();
        this.n = new IntWritable(0);
        this.M = new IntWritable(0);
    }

    //Setter method to set the values of WebLogWritable object
    public void set(int _i, int _j, int _n, int _M)
    {
        this.pos.set(_i, _j);
        this.n.set(_n);
        this.M.set(_M);
    }


    public Position getPos()
    {
        return pos;
    }
    public IntWritable getN()
    {
        return n;
    }

    public IntWritable  getM()
    {
        return M;
    }


    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput in) throws IOException
    {
        pos.readFields(in);
        n.readFields(in);
        M.readFields(in);

    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException
    {
        pos.write(out);
        n.write(out);
        M.write(out);

    }

    @Override
    public int compareTo(Key o)
    {
        if (pos.compareTo(o.pos)==0)
        {
            if (n.compareTo(o.n)==0) {
                return (M.compareTo(o.M));
            }
            else return (n.compareTo(o.n));
        }
        else return (pos.compareTo(o.pos));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Key)
        {
            Key other = (Key) o;
            return pos.equals(other.pos) && n.equals(other.n)  && M.equals(other.M);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return pos.hashCode();
    }
}


