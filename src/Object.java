import java.io.Serializable;

public class Object implements Serializable {
    String Object;
    String relationship;

    public String getObject() {
        return Object;
    }

    public void setObject(String object) {
        this.Object = object;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    String subject;
}
