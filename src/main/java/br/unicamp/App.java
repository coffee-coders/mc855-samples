package br.unicamp;

public class App {

  public static void main(String[] args) throws Exception {
    String[] newArgs = new String[]{args[1], args[2]};
    switch (args[0]) {
        case "word-count": {
            Sample1.main(newArgs);
            break;
        }
        case "letter-count": {
            CountLetters.main(newArgs);
            break;
        }
        case "letter-frequency-count": {
            CountFrequencyLetters.main(newArgs);
            break;
        }
        default: throw new IllegalArgumentException("Unknown job: " + args[0]);
    }
  }
}

