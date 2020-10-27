import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Book implements Serializable
{
    public Book (){
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

    public void setAlias(String alias) {
        this.alias.add(alias);
    }

    public List<String> getCharacters() {
        return characters;
    }

    public void setCharacters(String characters) {
        this.characters.add(characters);
    }

    public List<String> getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre.add(genre);
    }

    private String name;
    private String id;
    private List<String> alias;
    private List<String> characters;
    private List<String> genre;
}
