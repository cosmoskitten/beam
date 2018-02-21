package org.apache.beam.sdk.io.xml;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Test JAXB annotated class.
 */
@SuppressWarnings("unused") @XmlRootElement(name = "bird") @XmlType(propOrder = { "name",
  "adjective" }) public final class Bird implements Serializable {
  private String name;
  private String adjective;

  @XmlElement(name = "species")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAdjective() {
    return adjective;
  }

  public void setAdjective(String adjective) {
    this.adjective = adjective;
  }

  public Bird() {}

  public Bird(String adjective, String name) {
    this.adjective = adjective;
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Bird bird = (Bird) o;

    if (!name.equals(bird.name)) {
      return false;
    }
    return adjective.equals(bird.adjective);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + adjective.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return String.format("Bird: %s, %s", name, adjective);
  }
}
