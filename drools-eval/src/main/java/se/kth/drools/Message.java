package se.kth.drools;

import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;

;
;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class Message {

  String type;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void trigger() throws ElasticScalarException {
    //AutoScalarAPI api = new AutoScalarAPI();
    //call action
    //api.deleteGroup("test");
  }

  public void m() {}

}
