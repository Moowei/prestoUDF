/**
 * @Description Test
 * @Date 2020/2/17
 * @Author wangwei
 */
public class Test {

    public static void main(String[] args) {

        Integer[] retainArrayResult = new Integer[0];

        long hour =  30L;
        if (hour > 0 && hour <= 24) {
            retainArrayResult[0] = 1;
        } else if (hour > 24 && hour <= 48){
            retainArrayResult[1] = 1;
        } else if (hour > 48 && hour <= 72){
            retainArrayResult[2] = 1;
        } else if (hour > 72 && hour <= 96){
            retainArrayResult[3] = 1;
        } else if (hour > 96 && hour <= 128){
            retainArrayResult[4] = 1;
        } else if (hour > 128 && hour <= 152){
            retainArrayResult[5] = 1;
        }

    }

}
