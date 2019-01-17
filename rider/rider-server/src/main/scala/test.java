import java.util.*;
public class test {
    public static void main(String[] args) {
        test demos=new test();
        demos.findBads();
        //System.out.println(2);
    }
    private void findBads(){
        Scanner in = new Scanner(System.in);
        long W = in.nextLong();
        long N=in.nextLong();
        long count=0;
        if (N<=1) {
            System.out.println(1);
            return;
        }
        if (W<=1) {
            System.out.println(0);
            return;
        }
        for (long i = 0; i <W ; i++) {
            count+=findBadsCore(i,W-1,N);
            //count+=(N%100003);
//            count%=100003;
        }
        System.out.println(count-N);
    }

    private long findBadsCore(long prechoice,long  w,long  N){

        if (w==1) return 1;
        long count=0;
        for (long i = 0; i < N; i++) {
            if (i==prechoice) count+=((findBadsCore(i,w-1,N)+1)%100003);
            else count+=(findBadsCore(i,w-1,N)%100003);
        }
        return count%100003;
    }

}