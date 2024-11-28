package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;

    private String bucketname;
    private String filename;
    private int row;
    private int col;

    // bucketname
    public String getBucketname(){
        return bucketname;
    }
    public void setBucketname(String bucketname){
        this.bucketname = bucketname;
    }
    // filename
    public String getFilename() {
        return filename;
    }
    public void setFilename(String filename) {
        this.filename = filename;
    } 
    // row
    public int getRow() {
        return row;
    }
    public void setRow(int row) {
        this.row = row;
    }
    // col
    public int getCol() {
        return col;
    }
    public void setCol(int col) {
        this.col = col;
    }
    // name
    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }
}