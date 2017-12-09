/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import simpledb.record.Schema;

/**
 *
 * @author couger01
 */
public class SemiJoinPlan implements Plan{
  private Plan p1, p2;
  private Predicate pred;
  private Schema schema = new Schema();
  
  public SemiJoinPlan(Plan p1, Plan p2, Predicate pred) {
    this.p1 = p1;
    this.p2 = p2;
    this.pred = pred;
    schema.addAll(p1.schema());
    schema.addAll(p2.schema());
  }
  
  public Scan open() {
    Scan s1 = p1.open();
    Scan s2 = p2.open();
    return new SemiJoinScan(s1, s2, this.pred);
  }
  
  public int blocksAccessed() {
    return p1.blocksAccessed() + p1.recordsOutput();
  }
  
  public int recordsOutput() {
    return p1.recordsOutput();
  }
  
  public int distinctValues(String fldname) {
    if (p1.schema().hasField(fldname))
      return p1.distinctValues(fldname);
    else
      return p2.distinctValues(fldname);
  }
  
  public Schema schema() {
    return schema;
  }
}
