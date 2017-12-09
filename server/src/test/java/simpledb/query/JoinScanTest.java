/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.*;
import simpledb.metadata.StatInfo;
import simpledb.parse.Parser;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author couger01
 */
public class JoinScanTest {
  
  public JoinScanTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
    SimpleDB.init("studentdb");
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of beforeFirst method, of class JoinScan.
   */
  @Test
  @Ignore
  public void testBeforeFirst() {
    System.out.println("beforeFirst");
    JoinScan instance = null;
    instance.beforeFirst();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of next method, of class JoinScan.
   */
  @Test
  @Ignore
  public void testNext() {
    System.out.println("next");
    JoinScan instance = null;
    boolean expResult = false;
    boolean result = instance.next();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of close method, of class JoinScan.
   */
  @Test
  @Ignore
  public void testClose() {
    System.out.println("close");
    JoinScan instance = null;
    instance.close();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getVal method, of class JoinScan.
   */
  @Test
  @Ignore
  public void testGetVal() {
    System.out.println("getVal");
    String fldname = "";
    JoinScan instance = null;
    Constant expResult = null;
    Constant result = instance.getVal(fldname);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getInt method, of class JoinScan.
   */
  @Test
  @Ignore
  public void testGetInt() {
    System.out.println("getInt");
    String fldname = "";
    JoinScan instance = null;
    int expResult = 0;
    int result = instance.getInt(fldname);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getString method, of class JoinScan.
   */
  @Test
  @Ignore
  public void testGetString() {
    System.out.println("getString");
    String fldname = "";
    JoinScan instance = null;
    String expResult = "";
    String result = instance.getString(fldname);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of hasField method, of class JoinScan.
   */
  @Test
  @Ignore
  public void testHasField() {
    System.out.println("hasField");
    String fldname = "";
    JoinScan instance = null;
    boolean expResult = false;
    boolean result = instance.hasField(fldname);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }
  @Test
  public void testJoinScan() {
    System.out.println("JOIN");
//    String qry = "select sname from student join dept on majorid = did";
//    Parser p = new Parser(qry);
    Transaction tx = new Transaction();
    Plan studentTblPlan = new TablePlan("student", tx);
    Plan deptTblPlan = new TablePlan("dept", tx);
    tx.commit();
//    Plan p = SimpleDB.planner().createQueryPlan(qry, tx);
    Plan joinPlan = new JoinPlan(studentTblPlan, deptTblPlan,
            new Predicate(
                    new Term(
                      new FieldNameExpression("majorid"),
                      new FieldNameExpression("did"))));
    Scan joinScan = joinPlan.open();
    int records = 0;
    while (joinScan.next()) {
      for (String field: joinPlan.schema().fields()) {
        System.out.printf("%10s", joinScan.getVal(field).toString());
      }
      System.out.println();
      records++;
    }
    assertEquals(9, records);
  }
}
