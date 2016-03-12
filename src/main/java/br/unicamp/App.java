package br.unicamp;

import java.util.Arrays;
import java.util.LinkedList;

public class App {

    public static void main(String[] args) throws Exception {
        LinkedList<String> arguments = new LinkedList<>(Arrays.asList(args));
        String jobName = arguments.pollFirst();
        String[] jobArgs = arguments.toArray(new String[arguments.size()]);

        switch (jobName) {
            case "word-count": {
                WordCount.main(jobArgs);
                break;
            }
            case "letter-count": {
                CountLetters.main(jobArgs);
                break;
            }
            case "letter-frequency-count": {
                CountFrequencyLetters.main(jobArgs);
                break;
            }
            case "crimescount": {
                CrimesCount.main(jobArgs);
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown job: " + args[0]);
        }
    }
}

