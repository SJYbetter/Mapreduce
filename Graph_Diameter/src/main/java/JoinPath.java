import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class JoinPath {

    // public String[] adjacencyList;

    public int cost;

    public List<String> paths;

    public JoinPath push(String path) {
        if (this.paths == null)
            this.paths = new LinkedList<>();

        this.paths.add(path);
        this.cost++;

        return this;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer().append("p:");  // path tag
        sb.append(cost).append(" ");
        if (paths != null && paths.size() > 0) {
            for (String p : paths) {
                sb.append(p).append(',');
            }

            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }


    public static JoinPath parse(String text) throws IllegalArgumentException {

        String[] sections = (text.startsWith("p:") ? text.substring(2) : text)
                .split("\\s+");

        if (sections.length != 2) {
            throw new IllegalArgumentException("bad input data: " + text.toString());
        }

        JoinPath jp = new JoinPath();

//        if ("_".equalsIgnoreCase(sections[0])) {
//            jp.adjacencyList = new String[0];
//        } else {
//            jp.adjacencyList = sections[0].split(",");
//        }

        jp.cost = Integer.parseInt(sections[0]);

        if (sections[1].length() > 0) {
            jp.paths = new LinkedList<>();
            jp.paths.addAll(Arrays.asList(sections[1].split(",")));
        }

        return jp;
    }
}
