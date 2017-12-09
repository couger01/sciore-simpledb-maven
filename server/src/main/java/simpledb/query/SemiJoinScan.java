/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

/**
 *
 * @author couger01
 */
public class SemiJoinScan implements Scan{
  private Predicate pred;
  private Scan prod;
  
  public SemiJoinScan(Scan s1, Scan s2, Predicate pred) {
    this.prod = new ProductScan(s1, s2);
    this.pred = pred;
  }
  
  public void beforeFirst() {
    prod.beforeFirst();
  }
  
  public boolean next() {
    while (prod.next())
      if (pred.isSatisfied(prod))
        return true;
    return false;
  }
  
  public void close() {
    prod.close();
  }
  
  public Constant getVal(String fldname) {
    return prod.getVal(fldname);
  }
  
  public int getInt(String fldname) {
    return prod.getInt(fldname);
  }
  
  public String getString(String fldname) {
    return prod.getString(fldname);
  }
  
  public boolean hasField(String fldname) {
    return prod.hasField(fldname);
  }
}
