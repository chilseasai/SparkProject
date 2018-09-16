package java;

import java.util.Timer;
import java.util.TimerTask;

/**
 * java.Test
 *
 * @author cn-seo-dev@
 */
public class TimerExample {

    public static void main(String[] args) {
        final Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            int i = 0;

            @Override
            public void run() {

                System.out.println("MarketplaceId: '{}', DJS flow id: {}, DJS flow status: {}");

                if (i ++ == 6) {
                    timer.cancel();
                }
            }
        }, 0L, 1);

        System.out.println("hahahah");
    }
}
