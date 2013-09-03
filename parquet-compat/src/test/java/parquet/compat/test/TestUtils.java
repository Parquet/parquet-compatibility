package parquet.compat.test;

import junit.framework.Assert;

import org.junit.Test;

public class TestUtils {
  
  @Test
  public void testVersionComparator() {
    Utils.Version v1 = new Utils.Version("1.0.0");
    Utils.Version v2 = new Utils.Version("1.0.1");
    
    Assert.assertTrue(v1.compareMajorMinor(v2)==0);
    Assert.assertTrue(v1.compareTo(v2) < 0);
    
    v2 = new Utils.Version("1.1.0");
    
    Assert.assertTrue(v1.compareMajorMinor(v2) < 0);
    Assert.assertTrue(v1.compareTo(v2) < 0);
    
    v2 = new Utils.Version("1.0.0-SNAPSHOT");
    
    Assert.assertTrue(v1.compareTo(v2) > 0);
    
  }

}
