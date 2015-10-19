package com.danveloper.ratpack.graph;

/**
 * Type qualifier for {@link Node} types.
 * Allows you to categorically organize a typed node.
 */
public class NodeClassifier {
  private final String type;
  private final String category;

  public NodeClassifier(String type, String category) {
    this.type = type;
    this.category = category;
  }

  /**
   * @return the type qualifier for this classifier.
   */
  public String getType() {
    return type;
  }

  /**
   * @return the category within which this classifier exists
   */
  public String getCategory() {
    return category;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NodeClassifier that = (NodeClassifier) o;

    if (!type.equals(that.type)) return false;
    return category.equals(that.category);

  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + category.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "NodeClassifier{" +
        "type='" + type + '\'' +
        ", category='" + category + '\'' +
        '}';
  }
}
