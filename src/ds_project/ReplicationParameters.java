package ds_project;

/**
 *
 * @author recognition
 */
public class ReplicationParameters {
    //N - number of machines holding each key
    public static final int N = 2;
    //R - number of replies needed to achieve Read Quorum 
    public static final int R = 2;
    //W - number of replies needed to achieve Write Quorum
    public static final int W = 2;
    //T - milliseconds to wait before TIMEOUT
    public static final int T = 1000;
}
