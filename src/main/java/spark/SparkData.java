package spark;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * SparkData
 * Created by breynard on 26/10/16.
 */
public class SparkData implements Writable, Serializable {
    private String id;
    private String name;

    public SparkData() {
        super();
    }

    public SparkData(String id, String name) {
        super();
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "SparkData{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SparkData sparkData = (SparkData) o;

        return id != null ? id.equals(sparkData.id) : sparkData.id == null;

    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Text tmp = new Text();
        tmp.readFields(in);
        id = tmp.toString();
        Text currentValue = new Text();
        currentValue.readFields(in);
        name = currentValue.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text tmp = new Text(id);
        tmp.write(out);
        Text currentValue = new Text(name);
        currentValue.write(out);
    }
}
