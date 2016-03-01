package se.kth.drools;


import org.drools.RuleBase;
import org.drools.RuleBaseFactory;
import org.drools.rule.Package;
import org.drools.WorkingMemory;
import org.drools.compiler.DroolsError;
import org.drools.compiler.DroolsParserException;
import org.drools.compiler.PackageBuilder;
import org.drools.compiler.PackageBuilderErrors;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MessageTest {

  @Test
  public void testMessage() throws IOException, DroolsParserException {
    RuleBase ruleBase = initialiseDrools();

    for (int i = 0; i < 10; ++i) {
      String type;

      if(i%3 == 0)
        type = "Hello";
      else
        type = "something";

      WorkingMemory workingMemory = initializeMessageObjects(ruleBase, type);
      int expectedNumberOfRulesFired = 1;
      int actualNumberOfRulesFired = workingMemory.fireAllRules();
      System.out.println("noOf rules Fired for " + type + " = " + actualNumberOfRulesFired);
    }

  }

  private RuleBase initialiseDrools() throws IOException, DroolsParserException {

    // read files
    PackageBuilder packageBuilder = new PackageBuilder();
    String ruleFile = "/helloWorld.drl";
    Reader reader = new InputStreamReader(getClass().getResourceAsStream(ruleFile));
    packageBuilder.addPackageFromDrl(reader);
    assertNoRuleErrors(packageBuilder);

    //add rules to memory
    RuleBase ruleBase = RuleBaseFactory.newRuleBase();
    Package rulesPackage = packageBuilder.getPackage();
    ruleBase.addPackage(rulesPackage);

    return ruleBase;
  }

  private WorkingMemory initializeMessageObjects(RuleBase ruleBase, String type) {
    WorkingMemory workingMemory = ruleBase.newStatefulSession();
    Message helloMessage = new Message();
    helloMessage.setType(type);
    workingMemory.insert(helloMessage);

    return workingMemory;
  }

  private void assertNoRuleErrors(PackageBuilder packageBuilder) {
    PackageBuilderErrors errors = packageBuilder.getErrors();

    if (errors.getErrors().length > 0) {
      StringBuilder errorMessages = new StringBuilder();
      errorMessages.append("Found errors in package builder\n");
      for (int i = 0; i < errors.getErrors().length; i++) {
        DroolsError errorMessage = errors.getErrors()[i];
        errorMessages.append(errorMessage);
        errorMessages.append("\n");
      }
      errorMessages.append("Could not parse knowledge");

      throw new IllegalArgumentException(errorMessages.toString());
    }
  }
}
