package com.danveloper.ratpack.graph;

/**
 * The metadata associated with a {@link Node}.
 * This class provides a composite relationship of a unique id and a {@link NodeClassifier}.
 */
public class NodeProperties {
  private final String id;
  private final NodeClassifier classifier;

  public NodeProperties(String id, NodeClassifier classifier) {
    this.id = id;
    this.classifier = classifier;
  }

  /**
   * @return unique id portion of the composite metadata
   */
  public String getId() {
    return id;
  }

  /**
   * @return the classifier within which the related node is categorized
   */
  public NodeClassifier getClassifier() {
    return classifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NodeProperties that = (NodeProperties) o;

    if (id != null ? !id.equals(that.id) : that.id != null) return false;
    return !(classifier != null ? !classifier.equals(that.classifier) : that.classifier != null);

  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (classifier != null ? classifier.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "NodeProperties{" +
        "id='" + id + '\'' +
        ", classifier=" + classifier +
        '}';
  }
}
