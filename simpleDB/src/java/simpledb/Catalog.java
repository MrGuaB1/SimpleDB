package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 目前，这是一个存根目录，在使用之前必须由用户程序填充表,最终，它应该转换为从磁盘读取目录表的目录。
 * 
 * @Threadsafe
 */
public class Catalog { //Catalog跟踪数据库中的所有可用表及其关联的模式

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
	
	ArrayList<Table> tables=new ArrayList<Table>();
	
    public Catalog() {} //创建一个新的，空的目录

    public class Table{ //自己添加的辅助类，简化下面有关表功能实现的代码
    	private DbFile file; //DbFile是一个数据库文件的接口,HeapFile实现了这个接口
    	private String name;
    	private String pkeyField;
    	public Table(DbFile file,String name,String key) {
    		this.file=file;
    		this.name=name;
    		this.pkeyField=key;
    	}
    }
    //这一段是描述Table中的三个属性
    /**
     * Add a new table to the catalog. 添加新的表到目录中
     * This table's contents are stored in the specified DbFile. 表的内容存放在DbFile类中
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * file是要添加的表的内容，file中的getID方法是getTupleDesc and getFile的参数
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * 如果名字冲突了，用最后的表(即删除之前的再添加新的)
     * @param pkeyField the name of the primary key field
     * 主键的名字
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        for(int i=0;i<tables.size();i++) {
        	if(file.getId()==tables.get(i).file.getId()||name.equals(tables.get(i).name)) 
        		tables.remove(i); //名字可能为空，这时候就要比file的ID
        }
        tables.add(new Table(file,name,pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) { //UUID是用于生成唯一标识符的类
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    //返回给定table的ID，即DbFile中的ID
    public int getTableId(String name) throws NoSuchElementException {
        if(name==null)
        	throw new NoSuchElementException();
        for(int i=0;i<tables.size();i++) 
        	if(name.equals(tables.get(i).name))
        		return tables.get(i).file.getId();
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	for(int i=0;i<tables.size();i++)
    		if(tableid==tables.get(i).file.getId())
    			return tables.get(i).file.getTupleDesc();
    	throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * 返回一个DbFile，能读特定table的内容
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	for(int i=0;i<tables.size();i++)
    		if(tableid==tables.get(i).file.getId())
    			return tables.get(i).file;
    	throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
    	for(int i=0;i<tables.size();i++)
    		if(tableid==tables.get(i).file.getId())
    			return tables.get(i).pkeyField;
        return null;
    }

    public String getTableName(int id) {
        for(int i=0;i<tables.size();i++)
        	if(id==tables.get(i).file.getId())
        		return tables.get(i).name;
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
       int n=tables.size();
       for(int i=0;i<n;i++)  //注意不能直接用size，否则每次i+1且size-1，相当于2步进
    	   tables.remove(i);
    }
    
    public Iterator<Integer> tableIdIterator() { 
        return null;
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    //从文件中读取schema并在数据库中创建适当的table
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

