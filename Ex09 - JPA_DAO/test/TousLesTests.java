import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  A_FileDaoTest.class,
  B_JpaDaoTest.class,
  C_DbWorkerTest.class
})

public class TousLesTests {
}
