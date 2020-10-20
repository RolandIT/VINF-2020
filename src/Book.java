import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Book implements Serializable
{
    public Book (String name){
        this.name = name;
        this.alias = new ArrayList<>();
        this.characters = new ArrayList<>();
        this.genre = new ArrayList<>();
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getAlias() {
        return alias;
    }

    public void setAlias(List<String> alias) {
        this.alias = alias;
    }

    public List<String> getCharacters() {
        return characters;
    }

    public void setCharacters(List<String> characters) {
        this.characters = characters;
    }

    public List<String> getGenre() {
        return genre;
    }

    public void setGenre(List<String> genre) {
        this.genre = genre;
    }

    private String name;
    private String id;
    private List<String> alias;
    private List<String> characters;
    private List<String> genre;



}
