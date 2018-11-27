import java.util.Arrays;

public class Test {
    public static void main(String[] args) {

        outer:
        for (int i = 0; i <= 5; i++)
            for (int j : Arrays.asList(8, 9, 10)) {
                if (j == 8) {
                    System.out.println(i + " " + j);
                    break outer;
                }
            }
    }
}
