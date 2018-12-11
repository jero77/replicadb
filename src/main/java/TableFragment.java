public class TableFragment {

    private String tableName;

    private String fragmentationAttribute;

    private int minvalue;
    private int maxvalue;

    private int fragmentNumber;

    // Server assignment?


    public TableFragment(String tableName, String fragmentationAttribute, int minvalue, int maxvalue,
                            int fragmentNumber) {
        this.tableName = tableName;
        this.fragmentationAttribute = fragmentationAttribute;
        this.minvalue = minvalue;
        this.maxvalue = maxvalue;
        this.fragmentNumber = fragmentNumber;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFragmentationAttribute() {
        return fragmentationAttribute;
    }

    public int getFragmentNumber() {
        return fragmentNumber;
    }

    public int getMinvalue() {
        return minvalue;
    }

    public int getMaxvalue() {
        return maxvalue;
    }


    // merge, divide, ...


    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableFragment))
            return super.equals(obj);
        TableFragment other = (TableFragment) obj;

        return this.tableName == other.tableName && this.fragmentationAttribute == other.fragmentationAttribute &&
                this.maxvalue == other.maxvalue && this.minvalue == other.minvalue;
    }
}
