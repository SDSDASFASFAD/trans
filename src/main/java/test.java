import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.ilotterytech.ocean.dp.D1D2.beans.LotteryConsume;
import com.ilotterytech.ocean.dp.D1D2.gtech.bj.GTechGameFactoryFactory;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameCategoryDecoder;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameCategoryFactory;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameFactory;
import com.ilotterytech.ocean.dp.D1D2.sile.SiLeGameFactoryFactory;
import com.ilotterytech.ocean.dp.D1D2.suicai.SuicaiGameFactoryFactory;
import com.ilotterytech.ocean.dp.D1D2.suicai.utils.ScTickerUtils;

import java.text.ParseException;

public class test {
    public static void main(String[] args) {
        GTechGameFactoryFactory gameFactoryFactory = GTechGameFactoryFactory.getInstance();
        GameFactory gameFactory = gameFactoryFactory.getFactory("kl8");
        GameCategoryFactory game = gameFactory.getCategoryFactory();
        String content = ScTickerUtils.appendBrd("bettyp:7;multiple:1;system:1;board:13 15 29 49 50 52 55");
        String[] contents = game.decodeContent(content);
        for (int i = 0 ; i < contents.length ; i++){
            String prefix = game.decodeCategoryPrefix(contents[i]);
            GameCategoryDecoder decoder = game.getCategoryDecoder(prefix);
            LotteryConsume decode = decoder.decode(contents[i], new LotteryConsume());
            System.out.println(decode.getTimes());
            System.out.println(decode.getTotalNum());
            System.out.println(decode.getTotalPrice());
        }



    }

}
